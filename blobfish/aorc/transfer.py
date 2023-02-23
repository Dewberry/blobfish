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
from aiohttp import ClientSession, ServerDisconnectedError
from aiofile import async_open
from typing import List
from requests import Session
from boto3.resources.factory import ServiceResource
from dateutil.relativedelta import relativedelta
from dataclasses import dataclass, asdict

from .const import RFC_INFO_LIST, RFCInfo, FIRST_RECORD, FTP_HOST
from ..utils.gitinfo import GitInfo


@dataclass
class SourceURLObject:
    url: str
    date: datetime.datetime


@dataclass
class TransferMetadata:
    source_uri: str
    mirror_uri: str
    ref_date: str
    mirror_repository: str
    mirror_commit_hash: str
    mirror_script: str
    mirror_active_branch: str


class FTPError(Exception):
    "FTP site does not match expected structure"


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
        gitinfo: GitInfo,
        script_path: str,
        mirror_bucket_name: str,
        mirror_file_prefix: str,
        rfc_list: List[RFCInfo] = RFC_INFO_LIST,
        start_date: datetime.datetime = datetime.datetime.strptime(FIRST_RECORD, "%Y-%m-%d"),
        end_date: datetime.datetime = datetime.datetime.today(),
        dev: bool = False,
        concurrency: int = 5,
        limit: int | None = None,
    ) -> None:
        # Settings for development vs production
        self.dev = dev
        self.__configure_warnings()
        # Make a requests session for handling requests
        self.requests_session = self.__create_session()
        # Make s3 session
        self.resource = get_sessioned_s3_resource()
        # Assign properties
        self.mirror_bucket_name = mirror_bucket_name
        self.mirror_file_prefix = mirror_file_prefix
        self.rfc_list = rfc_list
        self.start_date = start_date
        self.end_date = end_date
        self.semaphore_size = concurrency
        self.gitinfo = gitinfo
        self.script_path = script_path
        self.limit = limit

    def __create_session(self) -> Session:
        session = requests.Session()
        if self.dev:
            session.verify = False
            return session
        return session

    def __configure_warnings(self) -> None:
        if self.dev:
            warnings.filterwarnings(
                "ignore",
                message=".*Unverified HTTPS request is being made to host 'hydrology.nws.noaa.gov'.*",
            )

    def __verify(self) -> None:
        directory_template_url = "AORC_{0}RFC_4km/{0}RFC_precip_partition/"
        for rfc in self.rfc_list:
            directory_formatted_url = f"{FTP_HOST}{directory_template_url.format(rfc.alias)}"
            with self.requests_session.head(directory_formatted_url) as resp:
                if resp.status_code != 200:
                    raise FTPError
        logging.info("expected file structure of FTP server verified")

    def __create_url_list(self) -> List[SourceURLObject]:
        urls = []
        file_template_url = "AORC_{0}RFC_4km/{0}RFC_precip_partition/AORC_APCP_4KM_{0}RFC_{1}.zip"
        try:
            self.__verify()
            for rfc in self.rfc_list:
                current_datetime = self.start_date
                while current_datetime <= self.end_date:
                    file_formatted_url = (
                        f"{FTP_HOST}{file_template_url.format(rfc.alias, current_datetime.strftime('%Y%m'))}"
                    )
                    urls.append(SourceURLObject(file_formatted_url, current_datetime))
                    current_datetime += relativedelta(months=1)
                    if self.limit and len(urls) >= self.limit:
                        break
                if self.limit and len(urls) >= self.limit:
                    break
        except FTPError:
            logging.error("expected file structure of FTP server was not verified")
            sys.exit(1)
        return urls

    async def __get_data(self, url: str, sem: asyncio.BoundedSemaphore, session: ClientSession) -> bytes | None:
        data = None
        while True:
            try:
                async with sem:
                    verify = not self.dev
                    async with session.get(url, ssl=verify) as resp:
                        data = await resp.read()
                        return data
            # If server disconnects, sleep then retry
            except ServerDisconnectedError:
                await asyncio.sleep(3)

    async def __write_data(self, data: bytes, file):
        async with async_open(file, "wb") as outfile:
            await outfile.write(data)

    async def __transfer_data(self, url_object: SourceURLObject, sem: asyncio.BoundedSemaphore, session: ClientSession):
        mirror_uri = f"s3://{self.mirror_bucket_name}/{self.mirror_file_prefix}/{url_object.url.replace(FTP_HOST, '')}"
        upload_meta = TransferMetadata(
            url_object.url,
            mirror_uri,
            url_object.date.strftime("%Y-%m-%d"),
            self.gitinfo.origin_url,
            self.gitinfo.commit_hash,
            self.script_path,
            self.gitinfo.active_branch,
        )
        mirror_bucket = self.resource.Bucket(self.mirror_bucket_name)
        with tempfile.TemporaryFile() as fp:
            data = await self.__get_data(url_object.url, sem, session)
            if data:
                await self.__write_data(data, fp)
                mirror_bucket.upload_fileobj(fp, mirror_uri, ExtraArgs={"Metadata": asdict(upload_meta)})
                logging.info(f"data from {url_object.url} successfully transferred to {mirror_uri}")
            else:
                logging.error(f"tried to transfer data for {url_object.url}, received no data")

    async def __gather_download_tasks(self) -> List[str]:
        url_objects = self.__create_url_list()
        tasks = []
        sem = asyncio.BoundedSemaphore(self.semaphore_size)
        async with ClientSession() as session:
            for url_object in url_objects:
                task = asyncio.create_task(self.__transfer_data(url_object, sem, session))
                tasks.append(task)
                logging.info(f"async task created to mirror data from {url_object.url}")
            download_paths = await asyncio.gather(*tasks)
            return download_paths

    def transfer_files(self) -> List[str]:
        logging.info("starting async task gathering")
        download_paths = asyncio.run(self.__gather_download_tasks())
        return download_paths


def clear_downloads(bucket: str, prefix: str):
    client = boto3.client(
        service_name="s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ["AWS_DEFAULT_REGION"],
    )
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        contents = page.get("Contents", [])
        for content in contents:
            client.delete_object(Bucket=bucket, Key=content.get("Key"))


def view_downloads(bucket: str, prefix: str):
    client = boto3.client(
        service_name="s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ["AWS_DEFAULT_REGION"],
    )
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        contents = page.get("Contents", [])
        for content in contents:
            print(client.get_object(Bucket=bucket, Key=content.get("Key")))


if __name__ == "__main__":
    from dotenv import load_dotenv
    from ..utils.gitinfo import version, script
    from ..utils.logger import set_up_logger

    load_dotenv()

    set_up_logger("logs/test.log")

    git_info = version()
    script_path = script(__file__)
    transfer_handler = TransferHandler(git_info, script_path, "tempest", "test", dev=True, limit=10)
    transfer_handler.transfer_files()

    # clear_downloads("tempest", "test/AORC")
    # view_downloads("tempest", "test/AORC")
