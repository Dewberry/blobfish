import requests
import warnings
import datetime
import boto3
import os
import sys
import logging
import asyncio
import tempfile
from dotenv import load_dotenv
from aiohttp import ClientSession, ServerDisconnectedError
from aiofile import async_open
from typing import List, IO
from requests import Session
from boto3.resources.factory import ServiceResource
from dateutil.relativedelta import relativedelta
from const import RFC_INFO_LIST, RFCInfo, FIRST_RECORD, FTP_HOST


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
        mirror_bucket_name: str,
        mirror_file_prefix: str,
        rfc_list: List[RFCInfo] = RFC_INFO_LIST,
        start_date: datetime.datetime = datetime.datetime.strptime(FIRST_RECORD, "%Y-%m-%d"),
        end_date: datetime.datetime = datetime.datetime.today(),
        dev: bool = False,
        concurrency: int = 5,
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

    def __create_url_list(self) -> List[str]:
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
                    urls.append(file_formatted_url)
                    current_datetime += relativedelta(months=1)
        except FTPError:
            logging.error("expected file structure is not verified")
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
        """Function to write data to file in order to verify full receipt of data before s3 upload begins

        Args:
            data (bytes): _description_
            file (_type_): _description_
        """
        async with async_open(file, "wb") as outfile:
            await outfile.write(data)

    async def __transfer_data(self, url: str, sem: asyncio.BoundedSemaphore, session: ClientSession):
        mirror_bucket = self.resource.Bucket(self.mirror_bucket_name)
        with tempfile.TemporaryFile() as fp:
            data = await self.__get_data(url, sem, session)
            if data:
                await self.__write_data(data, fp)
                mirror_bucket.upload_fileobj(fp, f"{self.mirror_file_prefix}/{url.replace(FTP_HOST, '')}")
            else:
                logging.error(f"tried to transfer data for {url}, received no data")

    async def __gather_download_tasks(self) -> List[str]:
        urls = self.__create_url_list()
        tasks = []
        sem = asyncio.BoundedSemaphore(self.semaphore_size)
        session = ClientSession()
        for url in urls:
            task = asyncio.create_task(self.__transfer_data(url, sem, session))
            tasks.append(task)
        download_paths = await asyncio.gather(*tasks)
        await session.close()
        return download_paths

    def transfer_files(self) -> List[str]:
        download_paths = asyncio.run(self.__gather_download_tasks())
        return download_paths


if __name__ == "__main__":
    load_dotenv()
    transfer_handler = TransferHandler("tempest", "test", dev=True)
    transfer_handler.transfer_files()
