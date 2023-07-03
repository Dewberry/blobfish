import boto3


def create_s3_resource(access_key_id: str, secret_access_key: str, region_name: str):
    session = boto3.Session(access_key_id, secret_access_key, region_name=region_name)
    resource = session.resource("s3")
    return resource
