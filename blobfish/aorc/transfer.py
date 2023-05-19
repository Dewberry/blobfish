""" Script to handle transfer of data from FTP server to s3 with appropriate metadata """

import requests
import warnings
import datetime
import boto3
import os
import sys
import logging
import asyncio
import tempfile
from aiohttp import ClientSession, ServerDisconnectedError, StreamReader
from aiofile import async_open
from typing import List, Tuple, cast
from boto3.resources.factory import ServiceResource
from dateutil.relativedelta import relativedelta
from dataclasses import asdict

from .const import RFC_INFO_LIST, RFCInfo, FIRST_RECORD, FTP_HOST
from .classes import SourceURLObject, TransferContext, BaseTransferMetadata, TransferMetadata, FTPError


def get_sessioned_s3_resource() -> ServiceResource:
    """Utility function to get an s3 resource with its own session

    Returns:
        boto3.resources.factory.ServiceResource: s3 resource
    """
    # locally this will work. batch will not have these env variables
    try:
        session = boto3.Session(
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            region_name=os.environ["AWS_DEFAULT_REGION"],
        )
    # use the batch resources IAM role
    except:
        session = boto3.Session()
    s3 = session.resource("s3")
    return s3


class TransferHandler:
    def __init__(
        self,
        script_path: str,
        mirror_bucket_name: str,
        mirror_file_prefix: str,
        tagged_docker_image: str,
        rfc_list: List[RFCInfo] = RFC_INFO_LIST,
        start_date: datetime.datetime = datetime.datetime.strptime(FIRST_RECORD, "%Y-%m-%d"),
        end_date: datetime.datetime = datetime.datetime.today(),
        dev: bool = False,
        concurrency: int = 5,
        limit: int | None = None,
        max_retries: int = 10,
    ) -> None:
        # Settings for development vs production
        self.dev = dev
        self.__configure_warnings()
        # Make s3 session
        self.resource = get_sessioned_s3_resource()
        # Assign properties
        self.mirror_bucket_name = mirror_bucket_name
        self.mirror_file_prefix = mirror_file_prefix
        self.docker_image_url = tagged_docker_image
        self.rfc_list = rfc_list
        self.start_date = start_date
        self.end_date = end_date
        self.semaphore_size = concurrency
        self.script_path = script_path
        self.limit = limit
        self.max_retries = max_retries

    def __configure_warnings(self) -> None:
        if self.dev:
            warnings.filterwarnings(
                "ignore",
                message=".*Unverified HTTPS request is being made to host 'hydrology.nws.noaa.gov'.*",
            )

    def __verify(self) -> None:
        directory_template_url = "AORC_{0}RFC_4km/{0}RFC_precip_partition/"
        for rfc in self.rfc_list:
            verify = not self.dev
            directory_formatted_url = f"{FTP_HOST}/{directory_template_url.format(rfc.alias)}"
            with requests.head(directory_formatted_url, verify=verify) as resp:
                if resp.status_code != 200:
                    raise FTPError
        logging.info("expected file structure of FTP server verified")

    def __create_url_list(self) -> List[SourceURLObject]:
        urls = []
        rfc_catalog_url = "/AORC_{0}RFC_4km"
        precip_partition_url = "/{0}RFC_precip_partition"
        source_file_url = "/AORC_APCP_4KM_{0}RFC_{1}.zip"
        try:
            self.__verify()
            for rfc in self.rfc_list:
                current_datetime = self.start_date
                while current_datetime <= self.end_date:
                    urls.append(
                        SourceURLObject(
                            rfc_catalog_url.format(rfc.alias),
                            precip_partition_url.format(rfc.alias),
                            source_file_url.format(rfc.alias, current_datetime.strftime("%Y%m")),
                            current_datetime,
                            rfc,
                        )
                    )
                    current_datetime += relativedelta(months=1)
                    if self.limit and len(urls) >= self.limit:
                        break
                if self.limit and len(urls) >= self.limit:
                    break
        except FTPError:
            logging.error("expected file structure of FTP server was not verified")
            sys.exit(1)
        return urls

    async def __get_data(
        self, url: str, sem: asyncio.BoundedSemaphore, session: ClientSession, stream: bool
    ) -> Tuple[bytes | StreamReader | None, str | None, str | None]:
        retries = 0
        while retries < self.max_retries:
            try:
                async with sem:
                    verify = not self.dev
                    resp = await session.get(url, ssl=verify)
                    last_modified = resp.headers.get("Last-Modified")
                    content_length = resp.headers.get("Content-Length")
                    if stream:
                        return resp.content, last_modified, content_length
                    data = await resp.read()
                    return data, last_modified, content_length
            # If server disconnects, sleep then retry
            except ServerDisconnectedError:
                await asyncio.sleep(3)
                retries += 1
        return None, None, None

    async def __write_data(self, data: bytes, file):
        async with async_open(file, "wb") as outfile:
            await outfile.write(data)

    def __set_up_transfer(self, url_object: SourceURLObject) -> TransferContext:
        mirror_uri = f"{self.mirror_file_prefix}{url_object.rfc_catalog_relative_url}{url_object.precip_partition_relative_url}{url_object.source_relative_url}"
        full_mirror_uri = f"s3://{self.mirror_bucket_name}/{mirror_uri}"
        upload_meta = BaseTransferMetadata(
            url_object.rfc.name,
            url_object.rfc.alias,
            url_object.rfc_catalog_relative_url,
            url_object.precip_partition_relative_url,
            url_object.source_relative_url,
            full_mirror_uri,
            url_object.date.strftime("%Y-%m-%d"),
            self.docker_image_url,
            self.script_path,
        )
        context = TransferContext(mirror_uri, upload_meta)
        return context

    async def __write_out_data(
        self, url_object: SourceURLObject, sem: asyncio.BoundedSemaphore, session: ClientSession
    ):
        context = self.__set_up_transfer(url_object)
        mirror_bucket = self.resource.Bucket(self.mirror_bucket_name)
        with tempfile.TemporaryFile() as fp:
            full_url = f"{context.metadata.aorc_historic_uri}{url_object.rfc_catalog_relative_url}{url_object.precip_partition_relative_url}{url_object.source_relative_url}"
            data, last_modified, content_length = await self.__get_data(full_url, sem, session, stream=False)
            if data and last_modified and content_length:
                await self.__write_data(cast(bytes, data), fp)
                context_meta_dict = asdict(context.metadata)
                context_meta_dict["source_bytes"] = content_length
                context_meta_dict["source_last_modified"] = last_modified
                transfer_metadata = TransferMetadata(**context_meta_dict)
                mirror_bucket.upload_fileobj(
                    fp, context.relative_mirror_uri, ExtraArgs={"Metadata": asdict(transfer_metadata)}
                )
                logging.info(f"data from {full_url} successfully transferred to {transfer_metadata.mirror_uri}")
            elif last_modified and content_length:
                logging.error(f"tried to transfer data for {full_url}, received no data")
            else:
                logging.error(f"tried to transfer data for {full_url}, could not parse content headers")

    """
    Commenting out __stream_out_data() because it would require reworking script to use a version of boto which supports async syntax
    """
    # async def __stream_out_data(
    #     self, url_object: SourceURLObject, sem: asyncio.BoundedSemaphore, session: ClientSession
    # ):
    #     context = self.__set_up_transfer(url_object)
    #     mirror_bucket = self.resource.Bucket(self.mirror_bucket_name)
    #     data = await self.__get_data(url_object.url, sem, session, stream=True)
    #     if data:
    #         print("data received")
    #         mirror_bucket.upload_fileobj(
    #             data, context.relative_mirror_uri, ExtraArgs={"Metadata": asdict(context.metadata)}
    #         )
    #         logging.info(f"data from {url_object.url} successfully transferred to {context.metadata.mirror_uri}")
    #     else:
    #         logging.error(f"tried to transfer data for {url_object.url}, received no data")

    async def __gather_download_tasks(self, stream: bool) -> List[str]:
        url_objects = self.__create_url_list()
        tasks = []
        sem = asyncio.BoundedSemaphore(self.semaphore_size)
        async with ClientSession() as session:
            for url_object in url_objects:
                if stream:
                    print("Streaming from aiohttp content to boto3 file upload is not supported")
                    raise NotImplementedError
                    # task = asyncio.create_task(self.__stream_out_data(url_object, sem, session))
                else:
                    task = asyncio.create_task(self.__write_out_data(url_object, sem, session))
                tasks.append(task)
                logging.info(f"async task created to mirror data from {url_object.source_relative_url}")
            download_paths = await asyncio.gather(*tasks)
            return download_paths

    def transfer_files(self, stream: bool = False) -> List[str]:
        logging.info("starting async task gathering")
        download_paths = asyncio.run(self.__gather_download_tasks(stream))
        return download_paths


if __name__ == "__main__":
    from dotenv import load_dotenv
    from ..utils.dockerinfo import script
    from ..utils.logger import set_up_logger
    from ..utils.cloud_utils import view_downloads, clear_downloads

    load_dotenv()

    set_up_logger()

    # script_path = script(__file__, workdir="proj")
    # docker_url = f"https://hub.docker.com/layers/njroberts/blobfish-python/latest/images/4cb4d04766c8e0f6b8b0394c374e73cebf1ab82c38f378b00b3071f220348291?context=repo"
    # if script_path:
    #     transfer_handler = TransferHandler(script_path, "tempest", "test", docker_url, dev=True, limit=10)
    #     transfer_handler.transfer_files()

    view_downloads("tempest", "test/AORC")
    # clear_downloads("tempest", "test/AORC")
