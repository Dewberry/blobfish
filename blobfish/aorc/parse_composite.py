import pathlib
import datetime
import logging
import enum
from dataclasses import dataclass, field
from rdflib import DCAT, DCTERMS, OWL, PROV, RDF, XSD, Graph, URIRef, BNode, Literal
from typing import cast, Generator, Any

from ..pyrdf import AORC
from ..utils.cloud_utils import get_s3_content, upload_graph_ttl, get_object_body_string


class AORCFilter(enum.Enum):
    YEAR = enum.auto()
    RFC = enum.auto()


@dataclass
class CompletedCompositeMetadata:
    start_time: str
    end_time: str
    docker_image_url: str
    members: str
    composite_last_modified: str
    composite_s3_directory: str
    composite_script: str
    public_uri: str = field(init=False)

    def __post_init__(self):
        bucket, *filename_parts = self.composite_s3_directory.replace("s3://", "").split("/")
        filename = "/".join(filename_parts)
        self.public_uri = f"https://{bucket}.s3.amazonaws.com/{filename}"
        self.composite_script = self.composite_script.replace("/", "_")

    def get_member_datasets(self) -> list[str]:
        return self.members.split(",")


class NodeNamer:
    def __init__(self):
        self.name_set = set()

    def name_ds_period(self, meta: CompletedCompositeMetadata) -> str:
        name = f"{meta.start_time}_{meta.end_time}"
        return name

    def name_composite_job(self, meta: CompletedCompositeMetadata) -> str:
        zarr_fn_index = meta.composite_s3_directory.rfind("/") + 1
        zarr_fn = meta.composite_s3_directory[zarr_fn_index:]
        name = f"{meta.composite_script}_{zarr_fn.replace('.zarr', '')}"
        return name


def complete_metadata(composite_object: dict, bucket: str) -> CompletedCompositeMetadata | None:
    partial_metadata = cast(dict, composite_object.get("Metadata"))
    partial_metadata["composite_last_modified"] = cast(
        datetime.datetime, composite_object.get("LastModified")
    ).isoformat()
    # Just get up to .zarr extension, not whole key
    full_key = composite_object.get("Key")
    zarr_path = format_zarr_s3_path(bucket, cast(str, full_key))
    partial_metadata["composite_s3_directory"] = zarr_path
    try:
        meta = CompletedCompositeMetadata(**partial_metadata)
        if meta:
            return meta
    except TypeError:
        logging.error(
            f"Incomplete composite metadata received from s3 object {composite_object.get('Key')}, could not complete object"
        )
        return None


def group_meta(
    bucket: str, prefix: str, with_key: bool = True, client: Any | None = None
) -> Generator[CompletedCompositeMetadata, None, None]:
    content_generator = get_s3_content(bucket, prefix, with_key, client)
    prev = None
    current = complete_metadata(next(content_generator), bucket)
    if current:
        current.composite_s3_directory
        datetime_set = set()
        for next_item in content_generator:
            next_item = complete_metadata(next_item, bucket)
            if next_item:
                if prev:
                    if prev.composite_s3_directory != current.composite_s3_directory:
                        prev.composite_last_modified = max(datetime_set)
                        yield prev
                        datetime_set.clear()
                    else:
                        datetime_set.add(datetime.datetime.fromisoformat(current.composite_last_modified))
                prev, current = current, next_item
        current.composite_last_modified = max(datetime_set)
        yield current


def format_zarr_s3_path(bucket: str, key: str) -> str:
    if ".zarr" not in key:
        logging.error(f"Trying to format key for resource which is not a .zarr file: {key}")
        raise ValueError
    zarr_path = key[: key.find(".zarr") + len(".zarr")]
    return f"s3://{bucket}/{zarr_path}"


def create_graph_local(ttl_directory: str, pattern: str) -> Graph:
    g = Graph()
    g.bind("dcat", DCAT)
    g.bind("dct", DCTERMS)
    g.bind("prov", PROV)
    g.bind("aorc", AORC)
    for filepath in pathlib.Path(ttl_directory).glob(pattern):
        g.parse(filepath)
    return g


def create_graph_s3(bucket: str, prefix: str, client: Any | None = None):
    g = Graph()
    g.bind("dcat", DCAT)
    g.bind("dct", DCTERMS)
    g.bind("prov", PROV)
    g.bind("aorc", AORC)
    for obj in get_s3_content(bucket, prefix, True, client):
        obj = get_object_body_string(bucket, cast(str, obj.get("Key")), client)
        g.parse(data=obj.read())
    return g


def create_graph_triples(meta: CompletedCompositeMetadata, merged_graph: Graph, node_namer: NodeNamer):
    # Create composite dataset
    composite_dataset_uri = URIRef(meta.composite_s3_directory)
    merged_graph.add((composite_dataset_uri, RDF.type, AORC.CompositeDataset))

    # Add composite dataset properties
    composite_dataset_period_of_time_node = BNode(node_namer.name_ds_period(meta))
    merged_graph.add((composite_dataset_period_of_time_node, RDF.type, DCTERMS.PeriodOfTime))
    merged_graph.add((composite_dataset_uri, DCTERMS.temporal, composite_dataset_period_of_time_node))
    start_time = Literal(meta.start_time, datatype=XSD.dateTime)
    end_time = Literal(meta.end_time, datatype=XSD.dateTime)
    merged_graph.add((composite_dataset_period_of_time_node, DCAT.startDate, start_time))
    merged_graph.add((composite_dataset_period_of_time_node, DCAT.endDate, end_time))

    # Create distribution
    composite_distribution_uri = URIRef(meta.public_uri)
    merged_graph.add((composite_distribution_uri, RDF.type, AORC.CompositeDistribution))
    netcdf_format = URIRef("https://publications.europa.eu/resource/authority/file-type/NETCDF")
    merged_graph.add((composite_distribution_uri, DCAT.packageFormat, netcdf_format))
    last_modified = Literal(meta.composite_last_modified, datatype=XSD.dateTime)
    merged_graph.add((composite_dataset_uri, DCTERMS.created, last_modified))
    access_description = Literal(
        "Access is restricted based on users credentials for AWS bucket holding data", datatype=XSD.string
    )
    merged_graph.add((composite_distribution_uri, OWL.Annotation, access_description))

    # Create docker image
    docker_image_uri = URIRef(meta.docker_image_url)
    merged_graph.add((docker_image_uri, RDF.type, AORC.DockerImage))

    # Create composite job
    composite_job_node = BNode(node_namer.name_composite_job(meta))
    merged_graph.add((composite_job_node, RDF.type, AORC.CompositeJob))

    # Create script
    composite_script_node = BNode(meta.composite_script)
    merged_graph.add((composite_script_node, RDF.type, AORC.CompositeScript))
    merged_graph.add((composite_script_node, DCTERMS.identifier, Literal(meta.composite_script)))

    # Associate docker image, script, job, and dataset generated
    merged_graph.add((composite_dataset_uri, AORC.wasCompositedBy, composite_job_node))
    merged_graph.add((composite_job_node, PROV.wasStartedBy, composite_script_node))
    merged_graph.add((composite_script_node, AORC.hasDockerImage, docker_image_uri))

    # Associate members of composite with composite dataset and composite job
    for member_dataset in meta.get_member_datasets():
        member_dataset_uri = URIRef(member_dataset)
        merged_graph.add((composite_dataset_uri, AORC.isCompositeOf, member_dataset_uri))
        merged_graph.add((composite_job_node, PROV.used, member_dataset_uri))


def main(
    ttl_directory: str,
    ttl_pattern: str,
    composites_bucket: str,
    composites_prefix: str,
    from_s3: bool = False,
    to_s3: bool = False,
    target_bucket: str | None = None,
    target_key: str | None = None,
    client: Any | None = None,
) -> None:
    node_namer = NodeNamer()
    if from_s3:
        g = create_graph_s3(ttl_directory, ttl_pattern, client)
    else:
        g = create_graph_local(ttl_directory, ttl_pattern)
    for grouped_metadata in group_meta(composites_bucket, composites_prefix):
        create_graph_triples(grouped_metadata, g, node_namer)
    if to_s3 and target_bucket and target_key:
        ttl_body = g.serialize(format="ttl")
        upload_graph_ttl(target_bucket, target_key, ttl_body, client)
    else:
        g.serialize("logs/composite.ttl", format="ttl")


if __name__ == "__main__":
    from ..utils.cloud_utils import view_downloads, clear_downloads, get_client
    from dotenv import load_dotenv

    load_dotenv()
    client = get_client()
    bucket = "tempest"
    main(bucket, "graphs/aorc/precip/1979", bucket, "transforms", True, True, bucket, "graphs/transforms.ttl", client)
    # view_downloads("tempest", "test/transforms")
    # clear_downloads("tempest", "test/transforms")
