"""General functions to interface with AWS"""
import boto3
import logging


def create_s3_resource(access_key_id: str, secret_access_key: str, region_name: str):
    """Creates s3 resource

    Args:
        access_key_id (str): AWS access key ID
        secret_access_key (str): AWS secret access key
        region_name (str): Default region for AWS resource to use

    Returns:
        Any: s3 resource
    """
    logging.info("Creating s3 resource")
    session = boto3.Session(access_key_id, secret_access_key, region_name=region_name)
    resource = session.resource("s3")
    return resource


def extract_bucketname_and_keyname(s3path: str) -> tuple[str, str]:
    """Extracts bucket and key name from s3 URI

    Args:
        s3path (str): s3 URI (ex: s3://bucket/key)

    Raises:
        ValueError: Error if path doesn't have correct format

    Returns:
        tuple[str, str]: Tuple of bucket and key
    """
    if not s3path.startswith("s3://"):
        raise ValueError(f"s3path does not start with s3://: {s3path}")
    bucket, _, key = s3path[5:].partition("/")
    return bucket, key
