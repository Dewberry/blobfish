import pathlib
import datetime
import logging
import re
import os
from rdflib import DCAT, DCTERMS, OWL, PROV, RDF, XSD, Graph, URIRef, BNode, Literal
from typing import cast, Generator, Any

from .classes import CompletedCompositeMetadata, CompositeConfig, DataFormat
from ..pyrdf import AORC
from ..utils.cloud_utils import get_s3_content, upload_graph_ttl, get_object_property, ObjectProperties
from ..utils.graph_utils import GraphCreator


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


def get_meta(
    bucket: str, prefix: str, metadata_pattern: re.Pattern, with_key: bool = True, client: Any | None = None
) -> Generator[CompletedCompositeMetadata, None, None]:
    for obj in get_s3_content(bucket, prefix, with_key, client):
        key = cast(str, obj.get("Key"))
        if re.match(metadata_pattern, key):
            meta = complete_metadata(obj, bucket)
            if meta:
                yield meta
            else:
                logging.info(f"Skipping {key} due to returning None from complete_metadata()")


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
        obj = get_object_property(bucket, cast(str, obj.get("Key")), ObjectProperties.BODY, client)
        g.parse(data=obj.read())

    return g


def create_graph_triples(
    meta: CompletedCompositeMetadata,
    node_namer: NodeNamer,
    graph: Graph | None = None,
    graph_creator: GraphCreator | None = None,
    filter_by_year: bool = True,
):
    if graph:
        pass
    elif graph_creator:
        # Get graph if none provided
        filter_value = None
        if filter_by_year:
            filter_value = meta.start_time[:4]
        graph = graph_creator.get_graph(filter_value)
    else:
        logging.error("Neither graph nor graph creator supplied, raising error")
        raise ValueError
    # Create composite dataset
    composite_dataset_uri = URIRef(meta.composite_s3_directory)
    graph.add((composite_dataset_uri, RDF.type, AORC.CompositeDataset))

    # Add composite dataset properties
    composite_dataset_period_of_time_node = BNode(node_namer.name_ds_period(meta))
    graph.add((composite_dataset_period_of_time_node, RDF.type, DCTERMS.PeriodOfTime))
    graph.add((composite_dataset_uri, DCTERMS.temporal, composite_dataset_period_of_time_node))
    start_time = Literal(meta.start_time, datatype=XSD.dateTime)
    end_time = Literal(meta.end_time, datatype=XSD.dateTime)
    graph.add((composite_dataset_period_of_time_node, DCAT.startDate, start_time))
    graph.add((composite_dataset_period_of_time_node, DCAT.endDate, end_time))

    # Create distribution
    composite_distribution_uri = URIRef(meta.public_uri)
    graph.add((composite_distribution_uri, RDF.type, AORC.CompositeDistribution))
    netcdf_format = URIRef("https://publications.europa.eu/resource/authority/file-type/NETCDF")
    graph.add((composite_distribution_uri, DCAT.packageFormat, netcdf_format))
    last_modified = Literal(meta.composite_last_modified, datatype=XSD.dateTime)
    graph.add((composite_dataset_uri, DCTERMS.created, last_modified))
    access_description = Literal(
        "Access is restricted based on users credentials for AWS bucket holding data", datatype=XSD.string
    )
    graph.add((composite_distribution_uri, OWL.Annotation, access_description))

    # Create docker image
    docker_image_uri = URIRef(meta.docker_image_url)
    graph.add((docker_image_uri, RDF.type, AORC.DockerImage))

    # Create composite job
    composite_job_node = BNode(node_namer.name_composite_job(meta))
    graph.add((composite_job_node, RDF.type, AORC.CompositeJob))

    # Create script
    composite_script_node = BNode(meta.composite_script)
    graph.add((composite_script_node, RDF.type, AORC.CompositeScript))
    graph.add((composite_script_node, DCTERMS.identifier, Literal(meta.composite_script)))

    # Associate docker image, script, job, and dataset generated
    graph.add((composite_dataset_uri, AORC.wasCompositedBy, composite_job_node))
    graph.add((composite_job_node, PROV.wasStartedBy, composite_script_node))
    graph.add((composite_script_node, AORC.hasDockerImage, docker_image_uri))

    # Associate members of composite with composite dataset and composite job
    for member_dataset in meta.get_member_datasets():
        member_dataset_uri = URIRef(member_dataset)
        graph.add((member_dataset_uri, RDF.type, AORC.MirrorDataset))
        graph.add((composite_dataset_uri, AORC.isCompositeOf, member_dataset_uri))
        graph.add((composite_job_node, PROV.used, member_dataset_uri))


def main(
    composites_bucket: str,
    composites_prefix: str,
    composites_metadata_pattern: re.Pattern,
    config: CompositeConfig,
    client: Any | None = None,
    limit: int | None = None,
) -> None:
    i = 0
    node_namer = NodeNamer()
    g = None
    graph_creator = None

    def triples_wrapper(i: int, g: Graph | None = None, graph_creator: GraphCreator | None = None):
        for meta in get_meta(composites_bucket, composites_prefix, composites_metadata_pattern):
            if g:
                create_graph_triples(meta, node_namer, graph=g)
            else:
                create_graph_triples(meta, node_namer, graph_creator=graph_creator)
                if limit:
                    if i >= limit:
                        break
                    else:
                        i += 1

    if config.extended and config.in_dir and config.in_pattern and config.input_format:
        if config.input_format.name == "S3":
            g = create_graph_s3(config.in_dir, config.in_pattern, client)
        else:
            g = create_graph_local(config.in_dir, config.in_pattern)
        g.bind("dcat", DCAT)
        g.bind("dct", DCTERMS)
        g.bind("prov", PROV)
        g.bind("aorc", AORC)
        triples_wrapper(i, g)
    else:
        graph_creator = GraphCreator({"dcat": DCAT, "dct": DCTERMS, "prov": PROV, "aorc": AORC})
        triples_wrapper(i, graph_creator=graph_creator)
    if config.output_format.name == "S3" and g:
        ttl_body = g.serialize(format="ttl")
        upload_graph_ttl(config.out_dir, config.out_path, ttl_body, client)
    elif config.output_format.name == "LOCAL" and g:
        g.serialize(os.path.join(config.out_dir, config.out_path), format="ttl")
    elif config.output_format.name == "S3" and graph_creator:
        graph_creator.serialize_graphs(config.out_path, True, client, config.out_dir)
    elif config.output_format.name == "LOCAL" and graph_creator:
        graph_creator.serialize_graphs(os.path.join(config.out_dir, config.out_path))


if __name__ == "__main__":
    from ..utils.cloud_utils import view_downloads, clear_downloads, get_client
    from ..utils.logger import set_up_logger
    from dotenv import load_dotenv

    load_dotenv()

    set_up_logger(level=logging.INFO)

    client = get_client()
    bucket = "tempest"
    config = CompositeConfig(DataFormat.S3, bucket, "graphs/transforms/{0}.ttl", False)
    metadata_pattern = re.compile(r".*\.zmetadata$")
    main(bucket, "transforms", metadata_pattern, config, client, limit=35040)
    # view_downloads("tempest", "test/transforms")
    # clear_downloads("tempest", "test/transforms")
