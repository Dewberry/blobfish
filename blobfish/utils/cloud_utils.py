import os
import boto3
import logging
import enum
import datetime
from typing import Generator, Any
from botocore.response import StreamingBody


class ObjectProperties(enum.Enum):
    BODY = enum.auto()
    LAST_MODIFIED = enum.auto()
    CONTENT_LENGTH = enum.auto()
    ETAG = enum.auto()
    METADATA = enum.auto()


def get_client():
    client = boto3.client(
        service_name="s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ["AWS_DEFAULT_REGION"],
    )
    return client


def clear_downloads(bucket: str, prefix: str, client: None | Any = None) -> None:
    if not client:
        client = get_client()
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        contents = page.get("Contents", [])
        for content in contents:
            client.delete_object(Bucket=bucket, Key=content.get("Key"))


def view_downloads(bucket: str, prefix: str, client: None | Any = None) -> None:
    if not client:
        client = get_client()
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        contents = page.get("Contents", [])
        for content in contents:
            logging.info(client.get_object(Bucket=bucket, Key=content.get("Key")))


def get_s3_content(
    bucket: str, prefix: str, with_key: bool = False, client: None | Any = None
) -> Generator[dict, None, None]:
    if not client:
        client = get_client()
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        contents = page.get("Contents", [])
        for content in contents:
            key = content.get("Key")
            object = client.head_object(Bucket=bucket, Key=key)
            object["Bucket"] = bucket
            if with_key:
                object["Key"] = key
            yield object


def update_metadata(bucket: str, key: str, new_meta: dict, client: None | Any = None) -> None:
    if not client:
        client = get_client()
    client.copy_object(
        Bucket=bucket,
        Key=key,
        CopySource={"Bucket": bucket, "Key": key},
        Metadata=new_meta,
        MetadataDirective="REPLACE",
    )


def check_exists(bucket: str, key: str, client: None | Any = None) -> bool:
    if not client:
        client = get_client()
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception as exc:
        logging.error(f"Exception received when retrieving metadata for s3://{bucket}/{key}: {repr(exc)}")
    return False


def upload_graph_ttl(bucket: str, key: str, ttl_body: str, client: None | Any = None) -> None:
    if not client:
        client = get_client()
    client.put_object(Bucket=bucket, Key=key, Body=ttl_body)


def get_object_property(bucket: str, key: str, property_name: ObjectProperties, client: None | Any = None):
    if not client:
        client = get_client()
    obj = client.get_object(Bucket=bucket, Key=key)
    if property_name.value == ObjectProperties.BODY.value:
        name = "Body"
    elif property_name.value == ObjectProperties.CONTENT_LENGTH.value:
        name = "ContentLength"
    elif property_name.value == ObjectProperties.ETAG.value:
        name = "ETag"
    elif property_name.value == ObjectProperties.LAST_MODIFIED.value:
        name = "LastModified"
    elif property_name.value == ObjectProperties.METADATA.value:
        name = "Metadata"
    else:
        raise NotImplementedError(f"{property_name} missing from ObjectProperties class")
    return obj.get(name)


def extract_bucketname_and_keyname(s3path: str) -> tuple[str, str]:
    if not s3path.startswith("s3://"):
        raise ValueError(f"s3path does not start with s3://: {s3path}")
    bucket, _, key = s3path[5:].partition("/")
    return bucket, key
