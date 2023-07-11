""" Utilities used in creating RDF metadata for s3 mirror creation """
import datetime
import logging

from classes.namespaces import AORC, EU, IANA_APP, LOCN
from const import NOAA_URL
from rdflib import DCAT, DCTERMS, ORG, RDF, RDFS, SKOS, XSD, BNode, Graph, IdentifiedNode, Literal, URIRef
from rdflib.namespace._GEO import GEO
from shapely.geometry import MultiPolygon, Polygon


def _bind_namespaces(target_graph: Graph) -> None:
    """Binds additional namespaces used in mirror dataset creation

    Args:
        target_graph (Graph): Graph which needs namespaces bound
    """
    target_graph.bind("aorc", AORC)
    target_graph.bind("locn", LOCN)


def _add_common_dataset_attributes(
    target_graph: Graph,
    dataset_node: IdentifiedNode,
    distribution_node: IdentifiedNode,
    download_url: str,
    last_modification: datetime.datetime,
    rfc_name: str,
    rfc_alias: str,
    rfc_geom: Polygon | MultiPolygon,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    temporal_resolution: datetime.timedelta,
    spatial_resolution: float,
) -> None:
    """Adds attributes common to both aorc:SourceDataset and aorc:MirrorDataset instances"""
    target_graph.add((distribution_node, DCAT.compressFormat, IANA_APP.zip))
    target_graph.add((distribution_node, DCTERMS.format, EU.NETCDF))
    target_graph.add((dataset_node, DCAT.distribution, distribution_node))

    url_literal = Literal(download_url, datatype=XSD.anyURI)
    target_graph.add((distribution_node, DCAT.downloadURL, url_literal))

    last_modification_literal = Literal(last_modification.isoformat(), datatype=XSD.dateTime)
    target_graph.add((dataset_node, DCTERMS.modified, last_modification_literal))

    rfc_b_node = BNode()
    rfc_alias_literal = Literal(rfc_alias, datatype=XSD.string)
    rfc_name_literal = Literal(rfc_name, datatype=XSD.string)
    target_graph.add((rfc_b_node, SKOS.altLabel, rfc_alias_literal))
    target_graph.add((rfc_b_node, SKOS.prefLabel, rfc_name_literal))
    target_graph.add((rfc_b_node, RDF.type, AORC.RFC))
    target_graph.add((dataset_node, AORC.hasRFC, rfc_b_node))

    rfc_geom_b_node = BNode()
    rfc_geom_wkt_literal = Literal(rfc_geom.wkt, datatype=GEO.wktLiteral)
    geom_label_literal = Literal(
        "This geometry is a convex hull of the RFC region, not an exact geometry, and therefore may produce false positives when querying for points covered by the region."
    )
    target_graph.add((rfc_geom_b_node, RDF.type, LOCN.Geometry))
    target_graph.add((rfc_b_node, LOCN.geometry, rfc_geom_b_node))
    target_graph.add((rfc_geom_b_node, GEO.asWKT, rfc_geom_wkt_literal))
    target_graph.add((rfc_geom_b_node, RDFS.label, geom_label_literal))

    rfc_org_uri = URIRef(NOAA_URL)
    target_graph.add((rfc_b_node, ORG.unitOf, rfc_org_uri))

    period_of_time_b_node = BNode()
    start_time_literal = Literal(start_time.isoformat(), datatype=XSD.dateTime)
    end_time_literal = Literal(end_time.isoformat(), datatype=XSD.dateTime)
    target_graph.add((period_of_time_b_node, RDF.type, DCTERMS.PeriodOfTime))
    target_graph.add((period_of_time_b_node, DCAT.startDate, start_time_literal))
    target_graph.add((period_of_time_b_node, DCAT.endDate, end_time_literal))
    target_graph.add((dataset_node, DCTERMS.temporal, period_of_time_b_node))

    spatial_resolution_literal = Literal(spatial_resolution, datatype=XSD.float)
    target_graph.add((dataset_node, DCAT.spatialResolutionInMeters, spatial_resolution_literal))

    temporal_resolution_literal = timedelta_to_xsd_duration(temporal_resolution)
    target_graph.add((dataset_node, DCAT.temporalResolution, temporal_resolution_literal))

    return


def create_source_dataset(
    download_url: str,
    last_modification: datetime.datetime,
    rfc_name: str,
    rfc_alias: str,
    rfc_geom: Polygon | MultiPolygon,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    temporal_resolution: datetime.timedelta,
    spatial_resolution: float,
) -> Graph:
    """Creates source dataset graph

    Args:
        download_url (str): Zip file download URL
        last_modification (datetime.datetime): Date of last modification for source dataset
        rfc_name (str): Full name of RFC associated with source dataset
        rfc_alias (str): Alias of RFC associated with source dataset
        rfc_geom (Polygon | MultiPolygon): Geometry of RFC associated with source dataset
        start_time (datetime.datetime): Start of temporal coverage for source dataset
        end_time (datetime.datetime): End of temporal coverage for source dataset
        temporal_resolution (datetime.timedelta): Temporal resolution (time between records) for source dataset
        spatial_resolution (float): Spatial resolution (in meters) for source dataset

    Returns:
        Graph: Graph with an aorc:SourceDataset instance
    """
    logging.info(f"Creating source dataset representing AORC data from {download_url}")
    g = Graph()

    _bind_namespaces(g)

    source_dataset_b_node = BNode()
    g.add((source_dataset_b_node, RDF.type, AORC.SourceDataset))

    source_distribution_b_node = BNode()
    g.add((source_distribution_b_node, RDF.type, DCAT.Distribution))

    _add_common_dataset_attributes(
        g,
        source_dataset_b_node,
        source_distribution_b_node,
        download_url,
        last_modification,
        rfc_name,
        rfc_alias,
        rfc_geom,
        start_time,
        end_time,
        temporal_resolution,
        spatial_resolution,
    )

    return g


# TODO: Implement creation of RDF graph of datasets to use in pySHACL validation
def create_mirror_dataset():
    pass


def timedelta_to_xsd_duration(timedelta_obj: datetime.timedelta) -> Literal:
    """Converts timedelta to an XSD duration literal

    Args:
        timedelta_obj (datetime.timedelta): timedelta object of interest

    Returns:
        Literal: Duration literal
    """
    days = timedelta_obj.days
    hours, remainder = divmod(timedelta_obj.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return Literal(f"P{days}DT{hours}H{minutes}M{seconds}S", datatype=XSD.duration)
