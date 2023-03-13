""" Script to parse metadata from uploaded mirror files and create a rdf graph network using the ontology defined in ./pyrdf/_AORC.py """
import os
import boto3
import rdflib
import datetime
import requests
import logging
from dateutil import relativedelta
from dataclasses import dataclass, field
from typing import Generator, cast
from rdflib import RDF, OWL, XSD, DCAT, DCTERMS, PROV, Literal, URIRef, BNode

from .transfer import TransferMetadata

# from .const import SOURCE_CATALOG, MIRROR_CATALOG
from ..pyrdf import AORC
from ..utils.logger import set_up_logger


@dataclass
class CompletedTransferMetadata(TransferMetadata):
    mirror_last_modified: str
    mirror_public_uri: str = field(init=False)
    ref_end_date: str = field(init=False)
    rfc_office_uri: str = field(init=False)

    def __post_init__(self):
        # Create public s3 address
        bucket, *filename_parts = self.mirror_uri.replace("s3://", "").split("/")
        filename = "/".join(filename_parts)
        public_uri = f"https://{bucket}.s3.amazonaws.com/{filename}"
        self.mirror_public_uri = public_uri

        # Calculate and format end duration for dataset
        ref_end_datetime = (
            datetime.datetime.strptime(self.ref_date, "%Y-%m-%d")
            + relativedelta.relativedelta(months=1, day=1)
            - datetime.timedelta(days=1)
        )
        self.ref_end_date = ref_end_datetime.strftime("%Y-%m-%d")

        # Format source last modified property
        if self.source_last_modified:
            self.source_last_modified = datetime.datetime.strptime(
                self.source_last_modified, "%a, %d %b %Y %H:%M:%S %Z"
            ).isoformat()

        # Get validated page for RFC office
        self.rfc_office_uri = self.__validate_rfc_office_page()

    def __validate_rfc_office_page(self) -> str:
        url = f"https://www.weather.gov/{self.rfc_alias.lower()}rfc"
        resp = requests.get(url, allow_redirects=True)
        if resp.ok:
            return url
        else:
            logging.error(f"rfc homepage url {url} not valid")
            raise requests.exceptions.RequestException


class NodeNamer:
    def __init__(self) -> None:
        self.name_set = set()


    def __verify_name(self, new_name: str) -> None:
        if new_name in self.name_set:
            logging.error("Duplicate name already exists: {0}".format(new_name))
            raise ValueError
        self.name_set.add(new_name)

    def name_source_ds(self, meta: CompletedTransferMetadata) -> str:
        fn_index = meta.source_uri.rfind("/") + 1
        fn = meta.source_uri[fn_index:].replace(".zip", "")
        self.__verify_name(fn)
        return fn

    def name_ds_period(self, meta: CompletedTransferMetadata) -> str:
        name = f"{meta.ref_date}_{meta.ref_end_date}"
        return name

    def name_transfer(self, meta: CompletedTransferMetadata):
        fn_index = meta.source_uri.rfind("/") + 1
        fn = meta.source_uri[fn_index:].replace(".zip", "")
        name = f"{meta.mirror_script}_{fn}"
        return name


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


def complete_metadata(mirror_object: dict) -> CompletedTransferMetadata | None:
    partial_metadata = cast(dict, mirror_object.get("Metadata"))
    partial_metadata["mirror_last_modified"] = cast(datetime.datetime, mirror_object.get("LastModified")).isoformat()
    try:
        return CompletedTransferMetadata(**partial_metadata)
    except TypeError:
        logging.error(
            f"Incomplete transfer metadata received from s3 object {mirror_object.get('Key')}, could not complete object"
        )
        return None


def construct_mirror_graph(bucket: str, prefix: str, outfile: str) -> None:
    g = rdflib.Graph()
    g.bind("dcat", DCAT)
    g.bind("prov", PROV)
    g.bind("dct", DCTERMS)
    g.bind("aorc", AORC)

    namer = NodeNamer()

    for object in get_mirrored_content(bucket, prefix):
        meta = complete_metadata(object)
        if meta:
            create_graph_triples(meta, g, namer)
    g.serialize(outfile, format="ttl")


def create_graph_triples(meta: CompletedTransferMetadata, g: rdflib.Graph, node_namer: NodeNamer) -> rdflib.Graph:
    # Create source dataset instance, properties
    source_dataset_node = BNode(node_namer.name_source_ds(meta))
    g.add((source_dataset_node, RDF.type, AORC.SourceDataset))
    source_dataset_period_of_time_node = BNode(node_namer.name_ds_period(meta))
    g.add((source_dataset_node, DCTERMS.temporal, source_dataset_period_of_time_node))
    source_dataset_period_start = Literal(meta.ref_date, datatype=XSD.date)
    g.add((source_dataset_period_of_time_node, DCAT.startDate, source_dataset_period_start))
    source_dataset_period_end = Literal(meta.ref_end_date, datatype=XSD.date)
    g.add((source_dataset_period_of_time_node, DCAT.endDate, source_dataset_period_end))

    # Create source dataset distribution instance, properties
    source_distribution_uri = URIRef(
        "".join([meta.aorc_historic_uri, meta.rfc_catalog_uri, meta.precip_partition_uri, meta.source_uri])
    )
    g.add((source_distribution_uri, RDF.type, AORC.SourceDistribution))
    source_distribution_byte_size = Literal(meta.source_bytes, datatype=XSD.positiveInteger)
    g.add((source_distribution_uri, DCAT.byteSize, source_distribution_byte_size))
    source_last_modified = Literal(meta.source_last_modified, datatype=XSD.dateTime)
    g.add((source_distribution_uri, DCTERMS.modified, source_last_modified))
    zip_compression = URIRef("https://www.iana.org/assignments/media-types/application/zip")
    g.add((source_distribution_uri, DCAT.compressFormat, zip_compression))
    netcdf_format = URIRef("https://publications.europa.eu/resource/authority/file-type/NETCDF")
    g.add((source_distribution_uri, DCAT.packageFormat, netcdf_format))
    monthly_frequency = URIRef("http://purl.org/cld/freq/monthly")
    g.add((source_dataset_node, DCTERMS.accrualPeriodicity, monthly_frequency))

    # Associate distribution with dataset
    g.add((source_dataset_node, DCAT.distribution, source_distribution_uri))

    # Create mirror dataset instance, properties
    mirror_dataset_uri = URIRef(meta.mirror_uri)
    g.add((mirror_dataset_uri, RDF.type, AORC.MirrorDataset))
    mirror_last_modified = Literal(meta.mirror_last_modified, datatype=XSD.dateTime)
    g.add((mirror_dataset_uri, DCTERMS.created, mirror_last_modified))
    access_description = Literal(
        "Access is restricted based on users credentials for AWS bucket holding data", datatype=XSD.string
    )
    g.add((mirror_dataset_uri, OWL.Annotation, access_description))

    # Associate mirror dataset with source dataset
    g.add((mirror_dataset_uri, AORC.hasSourceDataset, source_dataset_node))

    # Create mirror distribution instance, properties
    mirror_distribution_uri = URIRef(meta.mirror_public_uri)
    # g.add((mirror_distribution_uri, RDF.type, OWL.NamedIndividual))
    g.add((mirror_distribution_uri, RDF.type, AORC.MirrorDistribution))

    # Associate mirror distribution with mirror dataset
    g.add((mirror_dataset_uri, DCAT.distribution, mirror_distribution_uri))

    # Create transfer script instance
    script_uri = URIRef(meta.mirror_script)
    g.add((script_uri, AORC.hasTransferScript, AORC.TransferScript))

    # Create docker image instance, properties
    docker_image_uri = URIRef(meta.docker_image_url)
    g.add((docker_image_uri, RDF.type, AORC.DockerImage))
    g.add((docker_image_uri, AORC.hasTransferScript, script_uri))

    # Create transfer job activity instance, properties
    transfer_job_node = BNode(node_namer.name_transfer(meta))
    g.add((transfer_job_node, RDF.type, AORC.TransferJob))
    g.add((transfer_job_node, AORC.transferred, mirror_dataset_uri))
    g.add((transfer_job_node, PROV.used, source_dataset_node))
    g.add((transfer_job_node, PROV.wasStartedBy, script_uri))

    # Create RFC office instance
    rfc_office_uri = URIRef(meta.rfc_office_uri)
    g.add((rfc_office_uri, RDF.type, AORC.RFC))
    rfc_office_title = Literal(meta.rfc_name, datatype=XSD.string)
    g.add((rfc_office_uri, AORC.hasRFCName, rfc_office_title))
    rfc_office_alias = Literal(meta.rfc_alias, datatype=XSD.string)
    g.add((rfc_office_uri, AORC.hasRFCAlias, rfc_office_alias))

    # Create precip partition catalog instance, properties
    precip_partition_uri = URIRef("".join([meta.aorc_historic_uri, meta.rfc_catalog_uri, meta.precip_partition_uri]))
    precip_keyword_uri = Literal("precipitation", datatype=XSD.string)
    g.add((precip_partition_uri, RDF.type, AORC.PrecipPartition))
    g.add((precip_partition_uri, DCAT.keyword, precip_keyword_uri))
    g.add((precip_partition_uri, AORC.hasRFC, rfc_office_uri))

    # Associate precip partition catalog with source dataset it holds
    g.add((precip_partition_uri, DCAT.dataset, source_dataset_node))

    return g


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    set_up_logger()

    construct_mirror_graph("tempest", "test/AORC", "mirrors/test.ttl")
