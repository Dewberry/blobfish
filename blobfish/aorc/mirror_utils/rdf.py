""" Utilities used in creating RDF metadata for s3 mirror creation """
# Make sure script can access common classes
import sys

sys.path.append("../classes")

import datetime
import json

from classes.namespaces import AORC, EU, IANA_APP, LOCN
from rdflib import DCAT, DCTERMS, ORG, RDF, SKOS, XSD, BNode, Graph, Literal, URIRef
from rdflib.namespace._GEO import GEO
from shapely.geometry import MultiPolygon, Polygon


def create_source_dataset_jsonld(
    url: str,
    last_modification: datetime.datetime,
    rfc_name: str,
    rfc_alias: str,
    rfc_geom: Polygon | MultiPolygon,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    temporal_resolution: datetime.timedelta,
    spatial_resolution: float,
) -> dict | list:
    g = Graph()

    # # skolemize source dataset blank node URI
    # source_dataset_b_node = BNode(skolemize_source_dataset_uri(rfc_alias, start_time))
    source_dataset_b_node = BNode()
    g.add((source_dataset_b_node, RDF.type, AORC.SourceDataset))

    source_distribution_b_node = BNode()
    g.add((source_distribution_b_node, RDF.type, DCAT.Distribution))
    g.add((source_distribution_b_node, DCAT.compressFormat, IANA_APP.zip))
    g.add((source_distribution_b_node, DCTERMS.format, EU.NETCDF))
    g.add((source_dataset_b_node, DCAT.distribution, source_distribution_b_node))

    url_literal = Literal(url, datatype=XSD.anyURI)
    g.add((source_distribution_b_node, DCAT.downloadURL, url_literal))

    last_modification_literal = Literal(last_modification.isoformat(), datatype=XSD.dateTime)
    g.add((source_dataset_b_node, DCTERMS.modified, last_modification_literal))

    rfc_b_node = BNode()
    rfc_alias_literal = Literal(rfc_alias, datatype=XSD.string)
    rfc_name_literal = Literal(rfc_name, datatype=XSD.string)
    g.add((rfc_b_node, SKOS.altLabel, rfc_alias_literal))
    g.add((rfc_b_node, SKOS.prefLabel, rfc_name_literal))
    g.add((rfc_b_node, RDF.type, AORC.RFC))
    g.add((source_dataset_b_node, AORC.hasRFC, rfc_b_node))

    rfc_geom_b_node = BNode()
    rfc_geom_wkt_literal = Literal(rfc_geom.wkt, datatype=GEO.wktLiteral)
    g.add((rfc_geom_b_node, RDF.type, LOCN.Geometry))
    g.add((rfc_b_node, LOCN.geometry, rfc_geom_b_node))
    g.add((rfc_geom_b_node, GEO.asWKT, rfc_geom_wkt_literal))

    rfc_org_uri = URIRef("https://noaa.gov/")
    g.add((rfc_b_node, ORG.unitOf, rfc_org_uri))

    period_of_time_b_node = BNode()
    start_time_literal = Literal(start_time.isoformat(), datatype=XSD.dateTime)
    end_time_literal = Literal(end_time.isoformat(), datatype=XSD.dateTime)
    g.add((period_of_time_b_node, RDF.type, DCTERMS.PeriodOfTime))
    g.add((period_of_time_b_node, DCAT.startDate, start_time_literal))
    g.add((period_of_time_b_node, DCAT.endDate, end_time_literal))
    g.add((source_dataset_b_node, DCTERMS.temporal, period_of_time_b_node))

    spatial_resolution_literal = Literal(spatial_resolution, datatype=XSD.float)
    g.add((source_dataset_b_node, DCAT.spatialResolutionInMeters, spatial_resolution_literal))

    temporal_resolution_literal = timedelta_to_xsd_duration(temporal_resolution)
    g.add((source_dataset_b_node, DCAT.temporalResolution, temporal_resolution_literal))

    json_ld = g.serialize(format="json-ld")
    json_ld_verified = json.loads(json_ld)
    return json_ld_verified


def timedelta_to_xsd_duration(timedelta_obj: datetime.timedelta) -> Literal:
    days = timedelta_obj.days
    hours, remainder = divmod(timedelta_obj.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return Literal(f"P{days}DT{hours}H{minutes}M{seconds}S", datatype=XSD.duration)


# def skolemize_source_dataset_uri(rfc_alias: str, start_time: datetime.datetime) -> str:
#     return f"sourceDataset{rfc_alias.upper()}_{start_time.strftime('%Y%m')}"
