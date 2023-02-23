""" Script to parse metadata from uploaded mirror files and create a rdf graph network using the ontology defined in ./pyrdf/_AORC.py """
import os
import boto3
import rdflib
import datetime
from dataclasses import dataclass
from typing import Generator

from .transfer import TransferMetadata
from ..pyrdf import AORC


@dataclass
class CompletedTransferMetadata(TransferMetadata):
    last_modified: datetime.datetime
    etag: str


def get_mirrored_content(bucket: str, prefix: str) -> Generator[TransferMetadata, None, None]:
    client = boto3.client(
        service_name="s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ["AWS_DEFAULT_REGION"],
    )
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        contents = page.get("Contents", [])
        for content in contents:
            object = client.head_object(Bucket=bucket, Key=content.get("Key"))
            partial_metadata = object.get("Metadata")
            complete_metadata = CompletedTransferMetadata(
                partial_metadata.get("source_uri"),
                partial_metadata.get("mirror_uri"),
                partial_metadata.get("ref_date"),
                partial_metadata.get("mirror_repository"),
                partial_metadata.get("mirror_commit_hash"),
                partial_metadata.get("mirror_script"),
                partial_metadata.get("mirror_active_branch"),
                object.get("LastModified"),
                object.get("ETag"),
            )
            yield complete_metadata


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    g = rdflib.Graph()

    for content in get_mirrored_content("tempest", "test/AORC"):
        print(content)
