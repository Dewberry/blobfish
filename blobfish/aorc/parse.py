""" Script to parse metadata from uploaded mirror files and create a rdf graph network using the ontology defined in ./pyrdf/_AORC.py """
import os
import boto3
import rdflib
import datetime
from dataclasses import dataclass
from typing import Generator, cast
from rdflib import RDF, OWL, XSD, Literal, URIRef

from .transfer import TransferMetadata
from .const import SOURCE_CATALOG, MIRROR_CATALOG
from ..pyrdf import AORC


@dataclass
class CompletedTransferMetadata(TransferMetadata):
    last_modified: datetime.datetime
    etag: str


def get_mirrored_content(bucket: str, prefix: str) -> Generator[dict, None, None]:
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
            yield object


def construct_aorc_mirror_graph(bucket: str, prefix: str):
    def create_dataset_uri(metadata: CompletedTransferMetadata, prefix: str = "p"):
        ref_datetime = datetime.datetime.strptime(metadata.ref_date, "%Y-%m-%d")
        ref_year_month_date = ref_datetime.strftime("%Y%m")
        return f"{prefix}{ref_year_month_date}{metadata.rfc_alias}"

    g = rdflib.Graph()
    g.bind("aorc", AORC)
    g.bind("aorcftpcat", SOURCE_CATALOG)
    g.bind("aorcs3cat", MIRROR_CATALOG)
    for object in get_mirrored_content(bucket, prefix):
        # Make sure metadata conforms to expected structure of metadata
        partial_metadata = cast(dict, object.get("Metadata"))
        partial_metadata["last_modified"] = object.get("LastModified")
        partial_metadata["etag"] = object.get("ETag")
        complete_metadata = CompletedTransferMetadata(**partial_metadata)

        # Create URI references for applicable metadata properties
        source_uri_ref = URIRef(complete_metadata.source_uri)
        mirror_uri_ref = URIRef(complete_metadata.mirror_uri)
        mirror_repository_uri_ref = URIRef(complete_metadata.mirror_repository)
        mirror_commit_hash_uri_ref = URIRef(complete_metadata.mirror_commit_hash)
        mirror_script_uri_ref = AORC._NS[complete_metadata.mirror_script]
        rfc_uri_ref = AORC._NS[f"{complete_metadata.rfc_alias}{AORC.RFC.fragment}"]

        # Convert literals
        ref_date_xsd_literal = Literal(complete_metadata.ref_date, datatype=XSD.date)
        last_modified_xsd_literal = Literal(complete_metadata.last_modified.strftime("%Y-%m-%d"), datatype=XSD.date)
        etag_literal = Literal(complete_metadata.etag)
        rfc_alias_literal = Literal(complete_metadata.rfc_alias)
        rfc_name_literal = Literal(complete_metadata.rfc_name)

        # Create annotation format for active branch
        active_branch_annotation = Literal(f"current branch {complete_metadata.mirror_active_branch}")

        # Named individuals for source and mirror datasets
        # Format source dataset uri; ex: pFTP198001AB - p = precipitation, FTP = source file from FTP server, 1980 = year, 01 = month, AB = RFC alias
        source_dataset_uri = SOURCE_CATALOG[create_dataset_uri(complete_metadata, prefix="pFTP")]
        g.add((source_dataset_uri, RDF.type, OWL.NamedIndividual))
        g.add((source_dataset_uri, RDF.type, AORC.SourceDataset))

        mirror_dataset_uri = MIRROR_CATALOG[create_dataset_uri(complete_metadata, prefix="ps3")]
        g.add((mirror_dataset_uri, RDF.type, OWL.NamedIndividual))
        g.add((mirror_dataset_uri, RDF.type, AORC.MirrorDataset))

        # Add properties
        g.add((source_uri_ref, RDF.type, AORC.SourceURI))
        g.add((mirror_uri_ref, RDF.type, AORC.MirrorURI))
        g.add((rfc_uri_ref, RDF.type, AORC.RFC))
        g.add((mirror_script_uri_ref, RDF.type, AORC.Script))
        g.add((mirror_repository_uri_ref, RDF.type, AORC.CodeRepository))
        g.add((mirror_commit_hash_uri_ref, RDF.type, AORC.CommitHash))

        # Assign properties to datasets
        g.add((source_dataset_uri, AORC.hasRefDate, ref_date_xsd_literal))
        g.add((source_dataset_uri, AORC.hasSourceURI, source_uri_ref))
        g.add((source_dataset_uri, AORC.hasMirrorURI, mirror_uri_ref))
        g.add((source_dataset_uri, AORC.hasRFC, rfc_uri_ref))

        g.add((mirror_dataset_uri, AORC.hasDateCreated, last_modified_xsd_literal))
        g.add((mirror_dataset_uri, AORC.hasSourceURI, source_uri_ref))
        g.add((mirror_dataset_uri, AORC.hasMirrorURI, mirror_uri_ref))
        g.add((mirror_dataset_uri, AORC.hasCreationScript, mirror_script_uri_ref))
        g.add((mirror_dataset_uri, AORC.hasETag, etag_literal))

        # Assign properties and annotation to script
        g.add((mirror_script_uri_ref, AORC.hasCodeRepository, mirror_repository_uri_ref))
        g.add((mirror_script_uri_ref, AORC.hasCommitHash, mirror_commit_hash_uri_ref))
        g.add((mirror_script_uri_ref, OWL.Annotation, active_branch_annotation))

        # Assign properties to RFC
        g.add((rfc_uri_ref, AORC.hasRFCAlias, rfc_alias_literal))
        g.add((rfc_uri_ref, AORC.hasRFCName, rfc_name_literal))

        # # Assign properties to properties
        # g.add((AORC.hasCodeRepository, OWL.inverseOf, AORC.isCodeRepositoryOf))
        # g.add((AORC.hasCommitHash, OWL.inverseOf, AORC.isCommitHashOf))
        # g.add((AORC.hasCreationScript, OWL.inverseOf, AORC.isCreationScriptOf))
        # g.add((AORC.hasRFC, OWL.inverseOf, AORC.isRFCOf))
        # g.add((AORC.hasSourceURI, OWL.inverseOf, AORC.isSourceURIOf))
        # g.add((AORC.hasMirrorURI, OWL.inverseOf, AORC.isMirrorURIOf))

    g.serialize("logs/big_graph.ttl", format="ttl")


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    construct_aorc_mirror_graph("tempest", "test/AORC")
