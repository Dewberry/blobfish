from ..utils.cloud_utils import get_s3_content, update_metadata, check_exists, get_client
from .const import FTP_HOST, RFC_INFO_LIST
from .transfer import TransferMetadata
from .composite import CompositeMembershipMetadata
from dataclasses import asdict
from typing import cast, Any
import datetime
import os
import logging
import re


class TransferMetaBuilder:
    def __init__(self, s3_object: dict, client: Any | None):
        self.base = s3_object
        self.client = client
        self.__verify()
        self.bucket = cast(str, self.base.get("Bucket"))
        self.mirror_uri = cast(str, self.base.get("Key"))
        self.rfc_alias, self.rfc_name = self.__identify_rfc_info()
        self.precip_partition_uri = self.__construct_precip_partition()
        self.ref_date = self.__identify_ref_date()
        self.rfc_catalog_uri = self.__construct_catalog_url()
        self.source_uri = self.__construct_url()
        self.docker_image = f"https://hub.docker.com/layers/njroberts/blobfish-python/{os.environ['TAG']}/images/{os.environ['HASH']}?context=repo"
        self.mirror_script = "proj_blobfish_aorc_transfer.py"
        self.source_last_modified = "Mon, 01 Feb 2021 06:53:22 GMT"
        self.aorc_historic_uri = FTP_HOST
        self.source_bytes = self.__identify_bytes()

    def __verify(self):
        metadata = cast(dict, self.base.get("Metadata"))
        if len(metadata) > 0:
            try:
                TransferMetadata(**metadata)
            except TypeError:
                logging.info(f"Metadata for {self.base.get('Key')} is incomplete, continuing to creation")
            else:
                logging.info(f"This object has metadata which seems to have already been updated")
                raise ValueError
        else:
            logging.info(f"Metadata for {self.base.get('Key')} empty, continuing to creation")

    def __identify_rfc_info(self) -> tuple[str, str]:
        end_pos = self.mirror_uri.find("RFC")
        start_pos = end_pos - 2
        alias = self.mirror_uri[start_pos:end_pos]
        for rfc in RFC_INFO_LIST:
            if rfc.alias == alias:
                return rfc.alias, rfc.name
        logging.error(f"No matching rfc found for {self.mirror_uri}")
        raise AttributeError()

    def __construct_catalog_url(self) -> str:
        return f"/AORC_{self.rfc_alias}FC_4km"

    def __construct_precip_partition(self) -> str:
        return f"/{self.rfc_alias}RFC_precip_partition"

    def __identify_ref_date(self):
        fn = self.mirror_uri.split("/")[-1]
        date_string = fn.split("_")[-1].replace(".zip", "")
        date_dt = datetime.datetime.strptime(date_string, "%Y%m")
        return date_dt.strftime("%Y-%m-%d")

    def __construct_url(self) -> str:
        fn = self.mirror_uri.split("/")[-1]
        formatted = f"{FTP_HOST}/AORC_{self.rfc_alias}RFC_4km/{self.rfc_alias}RFC_precip_partition/{fn}"
        return formatted

    def __identify_bytes(self) -> str:
        object_bytes = cast(int, self.base.get("ContentLength"))
        return str(object_bytes)

    def as_transfer_metadata(self) -> TransferMetadata:
        return TransferMetadata(
            self.rfc_name,
            self.rfc_alias,
            self.rfc_catalog_uri,
            self.precip_partition_uri,
            self.source_uri,
            self.mirror_uri,
            self.ref_date,
            self.docker_image,
            self.mirror_script,
            self.aorc_historic_uri,
            self.source_last_modified,
            self.source_bytes,
        )


class CompositeMetaBuilder:
    def __init__(self, s3_object: dict, client: Any | None):
        self.base = s3_object
        self.client = client
        self.__verify()
        self.bucket = cast(str, self.base.get("Bucket"))
        self.key = cast(str, self.base.get("Key"))
        self.zarr_key = self.__identify_zarr()
        self.start_time_dt = self.__identify_temporal_coverage()
        self.end_time_dt = self.start_time_dt + datetime.timedelta(hours=1)
        self.members = self.__identify_members()
        self.docker_image_url = f"https://hub.docker.com/layers/njroberts/blobfish-python/{os.environ['TAG']}/images/{os.environ['HASH']}?context=repo"
        self.composite_script = "proj_blobfish_aorc_composite.py"

    def __verify(self):
        metadata = cast(dict, self.base.get("Metadata"))
        if len(metadata) > 0:
            try:
                self.check_keys(metadata)
            except KeyError:
                logging.info(f"Metadata for {self.base.get('Key')} is incomplete, continuing to creation")
            else:
                logging.info(f"This object has metadata which seems to have already been updated")
                raise ValueError
        else:
            logging.info(f"Metadata for {self.base.get('Key')} empty, continuing to creation")

    def __identify_zarr(self) -> str:
        zarr_end_pos = self.key.find(".zarr") + len(".zarr")
        return self.key[:zarr_end_pos]

    def __identify_temporal_coverage(self) -> datetime.datetime:
        fn = self.zarr_key.split("/")[-1].replace(".zarr", "")
        dt = datetime.datetime.strptime(fn, "%Y%m%d%H")
        return dt

    def __identify_members(self) -> set[str]:
        member_set = set()
        for rfc_info in RFC_INFO_LIST:
            key = f"mirrors/aorc/precip/AORC_{rfc_info.alias}RFC_4km/{rfc_info.alias}RFC_precip_partition/AORC_APCP_4KM_{rfc_info.alias}RFC_{self.start_time_dt.strftime('%Y%m')}.zip"
            full_path = f"s3://{self.bucket}/{key}"
            if check_exists(self.bucket, key, self.client):
                member_set.add(full_path)
            else:
                logging.error(f"Supposed member of {self.zarr_key}, {full_path}, does not exist")
                raise AttributeError
        return member_set

    def serialize(self) -> dict:
        meta = CompositeMembershipMetadata(
            self.start_time_dt, self.docker_image_url, self.composite_script, self.members
        )
        serialized = meta.serialize()
        return serialized

    @staticmethod
    def check_keys(metadata_dictionary: dict) -> None:
        if {"start_time", "end_time", "docker_image_url", "composite_script", "members"} != {
            key for key in metadata_dictionary.keys()
        }:
            raise KeyError


def update_mirrors(bucket: str, prefix: str, client: Any | None = None):
    for obj in get_s3_content(bucket, prefix, True, client):
        try:
            update_mirror(obj, bucket)
        except ValueError:
            logging.info(f"Object {obj.get('Key')} metadata has already been updated, skipping")


def update_mirror(mirror_object: dict, bucket: str, client: Any | None = None) -> None:
    new_meta_obj = TransferMetaBuilder(mirror_object, client)
    transfer_metadata = asdict(new_meta_obj.as_transfer_metadata())
    update_metadata(bucket, new_meta_obj.mirror_uri, transfer_metadata, client)


def update_composites(bucket: str, prefix: str, pattern: re.Pattern, client: Any | None = None) -> None:
    for obj in get_s3_content(bucket, prefix, True, client):
        key = cast(str, obj.get("Key"))
        if re.match(pattern, key):
            try:
                update_composite(obj, bucket)
            except ValueError:
                logging.info(f"Object {key} metadata has already been updated, skipping")


def update_composite(mirror_object: dict, bucket: str, client: Any | None = None):
    new_meta_obj = CompositeMetaBuilder(mirror_object, client)
    composite_metadata = new_meta_obj.serialize()
    update_metadata(bucket, new_meta_obj.key, composite_metadata, client)


if __name__ == "__main__":
    from dotenv import load_dotenv
    from multiprocessing import Pool
    from ..utils.logger import set_up_logger

    set_up_logger(level=logging.INFO)
    load_dotenv()

    bucket = "tempest"
    metadata_pattern = re.compile(r".*\.zmetadata$")

    client = get_client()

    # update_mirrors(bucket, "mirrors/aorc/precip", client)

    def mappable_update(year: int):
        return update_composites(bucket, f"transforms/aorc/precipitation/{year}", metadata_pattern)

    with Pool(processes=44) as pool:
        for i in pool.imap_unordered(mappable_update, range(1979, 2023)):
            continue
