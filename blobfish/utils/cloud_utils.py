import os
import boto3
import logging
import enum
from typing import Generator, Any


class ObjectProperties(enum.Enum):
    BODY = enum.auto()
    LAST_MODIFIED = enum.auto()
    CONTENT_LENGTH = enum.auto()
    ETAG = enum.auto()
    METADATA = enum.auto()


def get_client():
    """Gets client for use in cloud operations"""
    client = boto3.client(
        service_name="s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ["AWS_DEFAULT_REGION"],
    )
    return client


def clear_downloads(bucket: str, prefix: str, client: None | Any = None) -> None:
    """Clears downloads from a bucket that share a prefix provided

    Args:
        bucket (str): Target bucket
        prefix (str): Target prefix
        client (None | Any, optional): s3 client. Defaults to None.
    """
    if not client:
        client = get_client()
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        contents = page.get("Contents", [])
        for content in contents:
            client.delete_object(Bucket=bucket, Key=content.get("Key"))


def view_downloads(bucket: str, prefix: str, client: None | Any = None) -> None:
    """Views downloads in a bucket that share a prefix provided

    Args:
        bucket (str): Target bucket
        prefix (str): Target prefix
        client (None | Any, optional): s3 client. Defaults to None.
    """
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
    """Yields s3 content from a bucket with a shared prefix provided

    Args:
        bucket (str): Target bucket
        prefix (str): Target prefix
        with_key (bool, optional): If true, return object with key value included. Defaults to False.
        client (None | Any, optional): s3 client. Defaults to None.

    Yields:
        Generator[dict, None, None]: _description_
    """
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
    """Updates metadata for specific s3 object

    Args:
        bucket (str): Target bucket
        key (str): Target key
        new_meta (dict): New metadata to assign to object
        client (None | Any, optional): s3 client. Defaults to None.
    """
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
    """Checks if s3 object exists

    Args:
        bucket (str): Target bucket
        key (str): Target key
        client (None | Any, optional): s3 client. Defaults to None.

    Returns:
        bool: _description_
    """
    if not client:
        client = get_client()
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception as exc:
        logging.error(f"Exception received when retrieving metadata for s3://{bucket}/{key}: {repr(exc)}")
    return False


def upload_body(bucket: str, key: str, body: str, client: None | Any = None) -> None:
    """Uploads a string to an s3 object

    Args:
        bucket (str): Target bucket
        key (str): Target key
        body (str): String to upload as object
        client (None | Any, optional): s3 client. Defaults to None.
    """
    if not client:
        client = get_client()
    client.put_object(Bucket=bucket, Key=key, Body=body)


def get_object_property(bucket: str, key: str, property_name: ObjectProperties, client: None | Any = None) -> Any:
    """Gets one of the following properties from an s3 object: Body, ContentLength, Etag, LastModified, or Metadata

    Args:
        bucket (str): Target bucket
        key (str): Target key
        property_name (ObjectProperties): Property to get
        client (None | Any, optional): s3 client. Defaults to None.

    Returns:
        Any: Retrieved object
    """
    if not client:
        client = get_client()
    obj = client.get_object(Bucket=bucket, Key=key)
    if property_name == ObjectProperties.BODY:
        name = "Body"
    elif property_name == ObjectProperties.CONTENT_LENGTH:
        name = "ContentLength"
    elif property_name == ObjectProperties.ETAG:
        name = "ETag"
    elif property_name == ObjectProperties.LAST_MODIFIED:
        name = "LastModified"
    elif property_name == ObjectProperties.METADATA:
        name = "Metadata"
    return obj.get(name)


def extract_bucketname_and_keyname(s3path: str) -> tuple[str, str]:
    """Extracts bucket and key from s3 path

    Args:
        s3path (str): s3 URI

    Raises:
        ValueError: If poorly formatted s3 value is provided, will raise ValueError exception

    Returns:
        tuple[str, str]: Bucket and string, in that order, in a tuple
    """
    if not s3path.startswith("s3://"):
        raise ValueError(f"s3path does not start with s3://: {s3path}")
    bucket, _, key = s3path[5:].partition("/")
    return bucket, key


def download_object(bucket: str, key: str, filepath: str, client: None | Any = None) -> str:
    """Downloads s3 object

    Args:
        bucket (str): Target bucket
        key (str): Target key
        filepath (str): Local filepath to which object will be downloaded
        client (None | Any, optional): s3 client. Defaults to None.

    Returns:
        str: Filepath of downloaded resource
    """
    if not client:
        client = get_client()
    client.head_object(Bucket=bucket, Key=key)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    client.download_file(Bucket=bucket, Key=key, Filename=filepath)
    return filepath
