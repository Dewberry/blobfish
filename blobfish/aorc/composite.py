""" Script to handle creation of CONUS composites from s3 mirrors of AORC precip files utilizing and adding to transfer job metadata """

import boto3
import pathlib
import os
import pathlib
import datetime
import xarray as xr
import logging
import zarr.storage as storage
from io import BytesIO
from collections.abc import Generator
from rdflib import XSD, DCAT, DCTERMS, PROV, Graph, Literal
from typing import cast
from zipfile import ZipFile
from tempfile import TemporaryDirectory
from dataclasses import dataclass, field

from .const import RFC_INFO_LIST
from ..pyrdf import AORC

# TODO - add metadata to .zarr upload for use in graph database parsing


@dataclass
class DatedPaths:
    start_date: datetime.datetime
    end_date: datetime.datetime
    paths: list[str]

    def __post_init__(self):
        # Alter end date from referring to first hour of end date (00) to last hour of end date (23)
        # in order to capture all data from covered time period for dataset
        self.end_date = self.end_date.replace(hour=23)


@dataclass
class CompositeMembershipMetadata:
    start_time: datetime.datetime
    end_time: datetime.datetime = field(init=False)
    _matches: set[str]
    members: set[str]


    def __post_init__(self) -> None:
        self.end_time = self.start_time + datetime.timedelta(hours=1)

    def serializable(self) -> dict:
        serializable_dictionary = {
            "start_time": self.start_time.strftime('%Y%m%d%H'),
            "end_time": self.start_time.strftime('%Y%m%d%H'),
            "members": ",".join(self.members)
        }
        return serializable_dictionary



class CloudHandler:
    def __init__(self) -> None:
        self.client = self.__create_client()

    def __create_client(self):
        client = boto3.client(
            service_name="s3",
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            region_name=os.environ["AWS_DEFAULT_REGION"],
        )
        return client

    def __partition_bucket_key_names(self, s3_path: str) -> tuple[str, str]:
        if not s3_path.startswith("s3://"):
            raise ValueError(f"s3path does not start with s3://: {s3_path}")
        bucket, _, key = s3_path[5:].partition("/")
        return bucket, key

    def get_object(self, s3_path: str) -> bytes:
        bucket, key = self.__partition_bucket_key_names(s3_path)
        data = self.client.get_object(Bucket=bucket, Key=key)
        data_bytes = data["Body"].read()
        return data_bytes

    def send_composite_zarr(
        self, merged_hourly_data: xr.Dataset, template_s3_path: str, timestamp: datetime.datetime, metadata: dict
    ) -> None:
        # Create s3 destination filepath using template s3 path bucket and assumed structure of s3://{bucket}/transforms/aorc/precipitation/{year}/{datetime_string}.zarr
        template_bucket, _ = self.__partition_bucket_key_names(template_s3_path)
        destination_fn = f"s3://{template_bucket}/test/transforms/aorc/precipitation/{timestamp.year}/{timestamp.strftime('%Y%m%d%H')}.zarr"
        store = storage.FSStore(destination_fn, s3_additional_kwargs={"Metadata": metadata})
        merged_hourly_data.to_zarr(store, mode="w")


def create_graph(ttl_directory: str) -> Graph:
    g = Graph()
    g.bind("dcat", DCAT)
    g.bind("dct", DCTERMS)
    g.bind("prov", PROV)
    g.bind("aorc", AORC)
    for filepath in pathlib.Path(ttl_directory).glob("*.ttl"):
        g.parse(filepath)
    return g


def format_xsd_date(xsd_date_object: Literal) -> datetime.datetime:
    xsd_string = str(xsd_date_object)
    return datetime.datetime.strptime(xsd_string, "%Y-%m-%d")


def query_metadata(g: Graph) -> Generator[DatedPaths, None, None]:
    # Get unique start date and end date pairs which denote distinct periods of temporal coverage for datasets
    time_coverage_query = """
    SELECT  DISTINCT ?sd ?ed
    WHERE   {
        ?s dcat:startDate ?sd .
        ?s dcat:endDate ?ed
    }
    """
    time_results = g.query(time_coverage_query, initNs={"dcat": DCAT})
    for result in time_results:
        start_date, end_date = cast(list, result)
        new_query = (
            """
        SELECT  ?mda
        WHERE   {\n"""
            + f"""\t\t"{start_date}"^^xsd:date ^dcat:startDate/^dct:temporal/^aorc:hasSourceDataset ?mda ."""
            + """\n\t}"""
        )
        source_results = g.query(new_query, initNs={"dcat": DCAT, "xsd": XSD, "dct": DCTERMS, "aorc": AORC})
        formatted_start_date = format_xsd_date(start_date)
        formatted_end_date = format_xsd_date(end_date)
        s3_paths = [str(cast(list, result)[0]) for result in source_results]
        # # Check to make sure the length of the s3 paths is the same as the length of the list of RFC offices
        # if len(RFC_INFO_LIST) == len(s3_paths):
        #     logging.error(f"Expected {len(RFC_INFO_LIST)} to match RFC office number, got {len(s3_paths)}")
        #     raise AttributeError
        yield DatedPaths(formatted_start_date, formatted_end_date, s3_paths)


def unzip_composite_files(dated_s3_paths: DatedPaths, directory: str, cloud_handler: CloudHandler) -> None:
    for s3_path in dated_s3_paths.paths:
        data_bytes = cloud_handler.get_object(s3_path)
        with ZipFile(BytesIO(data_bytes)) as zf:
            zf.extractall(directory)


def align_hourly_data(
    directory: str, start_date: datetime.datetime, end_date: datetime.datetime, source_paths: list[str], limit: int | None
) -> Generator[CompositeMembershipMetadata, None, None]:
    directory_path = pathlib.Path(directory)
    current_datetime = start_date
    i = 0
    if limit:
        stop_i = limit
    else:
        stop_i = 1
    while current_datetime <= end_date and i < stop_i:
        search_pattern = f"{current_datetime.strftime('*_%Y%m%d%H.nc4')}"
        match_set = {str(match) for match in directory_path.glob(search_pattern)}
        # if len(match_set) != len(RFC_INFO_LIST):
        #     logging.error(f"Expected {len(RFC_INFO_LIST)} to match RFC office number, got {len(match_set)}")
        #     raise AttributeError
        yield CompositeMembershipMetadata(current_datetime, match_set, set(source_paths))
        current_datetime += datetime.timedelta(hours=1)
        if limit:
            i += 1


def create_composite_datset(dataset_paths: set[str]) -> xr.Dataset:
    datasets = []
    for dataset_path in dataset_paths:
        ds = xr.open_dataset(dataset_path)
        ds.rio.write_crs(4326, inplace=True)
        datasets.append(ds)
    merged_hourly_data = xr.merge(datasets, compat="no_conflicts", combine_attrs="drop_conflicts")
    return merged_hourly_data


def main(ttl_directory: str, limit: int | None = None) -> None:
    g = create_graph(ttl_directory)
    cloud_handler = CloudHandler()
    for dated_s3_paths in query_metadata(g):
        with TemporaryDirectory() as tempdir:
            unzip_composite_files(dated_s3_paths, tempdir, cloud_handler)
            for dated_match_set in align_hourly_data(
                tempdir, dated_s3_paths.start_date, dated_s3_paths.end_date, dated_s3_paths.paths, limit
            ):
                logging.info(dated_match_set)
                merged_data = create_composite_datset(dated_match_set._matches)
                cloud_handler.send_composite_zarr(
                    merged_data,
                    dated_s3_paths.paths[0],
                    dated_match_set.start_time,
                    dated_match_set.serializable(),
                )


if __name__ == "__main__":
    from dotenv import load_dotenv
    from ..utils.cloud_utils import view_downloads, clear_downloads
    from ..utils.logger import set_up_logger

    load_dotenv()
    set_up_logger(level=logging.INFO)

    # main("mirrors", 10)

    view_downloads("tempest", "test/transforms")
    # clear_downloads("tempest", "test/transforms")
