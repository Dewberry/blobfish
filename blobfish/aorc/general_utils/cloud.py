"""General functions to interface with AWS"""
import boto3
import logging


def create_s3_resource(access_key_id: str, secret_access_key: str, region_name: str):
    logging.info("Creating s3 resource")
    session = boto3.Session(access_key_id, secret_access_key, region_name=region_name)
    resource = session.resource("s3")
    return resource


def extract_bucketname_and_keyname(s3path: str) -> tuple[str, str]:
    if not s3path.startswith("s3://"):
        raise ValueError(f"s3path does not start with s3://: {s3path}")
    bucket, _, key = s3path[5:].partition("/")
    return bucket, key
