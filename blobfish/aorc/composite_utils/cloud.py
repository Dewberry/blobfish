"""Cloud utilities used during composite dataset creation"""
import datetime
import os
from io import BytesIO
from zipfile import ZipFile

from general_utils.cloud import extract_bucketname_and_keyname


def stream_s3_zipped(s3_resource, s3_uri: str, extract_directory: str) -> list[str]:
    bucket, key = extract_bucketname_and_keyname(s3_uri)
    response = s3_resource.meta.client.get_object(Bucket=bucket, Key=key)
    zip_data = response["Body"].read()
    with ZipFile(BytesIO(zip_data), "r") as zf:
        outpaths = [os.path.join(extract_directory, info.filename) for info in zf.filelist]
        zf.extractall(extract_directory)
    return outpaths


def check_zarr_modification(s3_resource, zarr_path: str) -> datetime.datetime:
    bucket, key = extract_bucketname_and_keyname(zarr_path)
    key += "/.zmetadata"
    obj = s3_resource.Object(bucket, key)
    obj.load()
    return obj.last_modified
