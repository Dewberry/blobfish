"""Cloud utilities used during composite dataset creation"""
import datetime
import os
from io import BytesIO
from zipfile import ZipFile

from general_utils.cloud import extract_bucketname_and_keyname


def stream_s3_zipped(s3_resource, s3_uri: str, extract_directory: str) -> list[str]:
    """Streams zipped s3 resource to provided directory

    Args:
        s3_resource: s3 service resource to use in streaming
        s3_uri (str): s3 uri of zipped resource (ie s3://bucket/key.zip)
        extract_directory (str): Directory to which zipped resources will be extracted

    Returns:
        list[str]: Paths of unzipped data
    """
    bucket, key = extract_bucketname_and_keyname(s3_uri)
    response = s3_resource.meta.client.get_object(Bucket=bucket, Key=key)
    zip_data = response["Body"].read()
    with ZipFile(BytesIO(zip_data), "r") as zf:
        outpaths = [os.path.join(extract_directory, info.filename) for info in zf.filelist]
        zf.extractall(extract_directory)
    return outpaths


def check_zarr_modification(s3_resource, zarr_path: str) -> datetime.datetime:
    """Checks modification date of a .zmetadata resource in a zarr dataset

    Args:
        s3_resource: s3 service resource to use in streaming
        zarr_path (str): s3 uri of zarr dataset (ie s3://bucket/zarr_dataset)

    Returns:
        datetime.datetime: _description_
    """
    bucket, key = extract_bucketname_and_keyname(zarr_path)
    key += "/.zmetadata"
    obj = s3_resource.Object(bucket, key)
    obj.load()
    return obj.last_modified
