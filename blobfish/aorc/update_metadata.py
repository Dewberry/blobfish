from ..utils.cloud_utils import get_s3_content, update_metadata
from .const import FTP_HOST, RFC_INFO_LIST, RFCInfo
from .transfer import TransferMetadata
from dataclasses import asdict
from typing import cast
import datetime
import os


class MetaBuilder:
    def __init__(self, s3_object: dict):
        self.base = s3_object
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
        metadata = cast(str, self.base.get("Metadata"))
        if len(metadata) > 0:
            raise ValueError(f"This object has unexpected existing metadata: {metadata}")

    def __identify_rfc_info(self) -> tuple[str, str]:
        end_pos = self.mirror_uri.find("RFC")
        start_pos = end_pos - 2
        alias = self.mirror_uri[start_pos:end_pos]
        for rfc in RFC_INFO_LIST:
            if rfc.alias == alias:
                return rfc.alias, rfc.name
        raise AttributeError(f"No matching rfc found for {self.mirror_uri}")

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
        # https://hydrology.nws.noaa.gov/pub/aorc-historic/AORC_ABRFC_4km/ABRFC_precip_partition/AORC_APCP_4KM_ABRFC_197902.zip
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


def retrieve_mirrors(bucket: str, prefix: str):
    for obj in get_s3_content(bucket, prefix, True):
        try:
            update_mirror(obj, bucket)
        except ValueError:
            print(f"Object {obj.get('Key')} metadata has already been updated, skipping")


def update_mirror(mirror_object: dict, bucket: str) -> None:
    new_meta_obj = MetaBuilder(mirror_object)
    transfer_metadata = asdict(new_meta_obj.as_transfer_metadata())
    update_metadata(bucket=bucket, key=new_meta_obj.mirror_uri, new_meta=transfer_metadata)


# def retrieve_composites():
#     pass


# def update_composite():
#     pass


# def main():
#     for mirror_object in retrieve_mirrors():
#         update_mirror()
#     for composite_object in retrieve_composites():
#         update_composite

if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    retrieve_mirrors("tempest", "aorc/precip/source")
