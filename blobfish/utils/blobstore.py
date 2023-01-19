import boto3
from typing import Iterator


def list_s3_keys(s3_client: boto3.client, bucket_name: str, prefix="/", delimiter="/", start_after="") -> Iterator:
    """
    Generic method to search all keys given a delimiter
    or optional start_after parameter
    """
    s3_paginator = s3_client.get_paginator("list_objects_v2")
    prefix = prefix.lstrip(delimiter)
    start_after = (start_after or prefix) if prefix.endswith(delimiter) else start_after
    for page in s3_paginator.paginate(Bucket=bucket_name, Prefix=prefix, StartAfter=start_after):
        for content in page.get("Contents", ()):
            yield content["Key"]


def s3_key_exists(prefix: str) -> bool:
    """
    Utility to check if file exists on S3
    """
    if prefix in list_s3_keys(prefix=prefix):
        return True
    return False
