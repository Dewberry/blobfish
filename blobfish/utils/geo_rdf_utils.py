""" Script to create geospatial properties """

import json
import logging
from shapely.geometry import shape, box, Polygon, MultiPolygon, Point
from rdflib import Graph, Namespace, URIRef, BNode, IdentifiedNode, Literal, RDF, XSD, DCTERMS, DCAT
from rdflib.namespace._GEO import GEO
from dataclasses import dataclass


@dataclass
class NamedSpatialProperty:
    name: str
    geom: Point | Polygon | MultiPolygon


@dataclass
class ParentNode:
    parent_node: IdentifiedNode
    attached_graph: Graph


def validate_geom_type(geom) -> None:
    expected_geometry_types = [
        "Point",
        "Polygon",
        "MultiPolygon",
    ]
    if geom.geom_type not in expected_geometry_types:
        raise ValueError(f"Expected on of {', '.join(expected_geometry_types)}, got {geom.geom_type}")


def format_wkt(
    geom: Polygon | MultiPolygon | Point, bbox: bool, crs_uri: str = "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
) -> str:
    if bbox:
        if geom.geom_type == "Point":
            logging.warning(
                "Point provided with bbox parameter set to true. Ignoring and proceeding with wkt formatting using point geometry"
            )
        else:
            geom = box(*geom.bounds)
    return f"<{crs_uri}> {geom.wkt}"


def load_feature(features: list[dict], index: int | None) -> dict:
    n = len(features)
    if n > 1:
        if index == None:
            raise ValueError("Expecting geojson with single feature to use as spatial extent")
        if n > index:
            raise ValueError(f"Invalid index provided. Only {n} features with provided index {index}")
        else:
            feature = features[index]
    else:
        if index != None:
            logging.warning("Index provided for geojson with single feature, defaulting to only feature in geojson")
        feature = features[0]
    return feature


def parse_geojson_str(geojson_string: str, index: int | None = None) -> NamedSpatialProperty:
    geojson = json.loads(geojson_string)
    features = geojson["features"]
    name = geojson["name"]
    feature = load_feature(features, index)
    geom = shape(feature["geometry"])
    validate_geom_type(geom)
    return NamedSpatialProperty(name, geom)


def parse_geojson_file(fn: str, index: int | None = None) -> NamedSpatialProperty:
    with open(fn, "r") as f:
        geojson = json.load(f)
        features = geojson["features"]
        name = geojson["name"]
        feature = load_feature(features, index)
        geom = shape(feature["geometry"])
    validate_geom_type(geom)
    return NamedSpatialProperty(name, geom)


def create_rdf_location(
    geom: Polygon | MultiPolygon | Point,
    name: str | None = None,
    bbox: bool = False,
) -> ParentNode:
    """Adds valid dct:Location for use as spatial attribute within range of dct:spatial property to graph

    Args:
        geom (Polygon | MultiPolygon | Point): Geometry to convert to Location instance
        name (str | None, optional): Name to provide location using property locn:geographicName. Defaults to None.
        bbox (bool, optional): If true, simplify geometry to bounding box. Defaults to False.

    Returns:
        ParentNode: Location node with attached attributes to create valid spatial property

    Follows general template:
    ```
    @prefix locn: <http://w3.org/ns/locn#> .
    @prefix geo: <http://www.opengis.net/ont/geosparql#> .
    @prefix sf: <http://www.opengis.net/ont/sf#> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    @prefix dct: <http://purl.org/dc/terms/> .

    [] a dct:Location ;
      locn:geographicName "The house of Anne Frank"^^xsd:string ;
      locn:geometry [ a sf:Point ;
        geo:asWKT "<http://www.opengis.net/def/crs/OGC/1.3/CRS84> POINT(4.88412 52.37509)"^^geo:wktLiteral ] ;
      ] ;
    ```
    """
    # Create graph
    graph = Graph()

    # Create namespaces for locn and sf used in geospatial features
    locn_uri = "http://w3.org/ns/locn#"
    sf_uri = "http://www.opengis.net/ont/sf#"
    locn_ns = Namespace(locn_uri)
    sf_ns = Namespace(sf_uri)

    # Bind namespaces used
    graph.bind("locn", locn_ns, override=False)
    graph.bind("geo", GEO, override=False)
    graph.bind("sf", sf_ns, override=False)
    graph.bind("dct", DCTERMS, override=False)
    graph.bind("dcat", DCAT, override=False)

    # Create nodes
    if name:
        location_bnode = BNode(f"{name}_node")
    else:
        location_bnode = BNode()
    graph.add((location_bnode, RDF.type, DCTERMS.Location))
    geometry_bnode = BNode()
    geometry_wkt_literal = Literal(format_wkt(geom, bbox), datatype=GEO.wktLiteral)
    if bbox:
        graph.add((location_bnode, DCAT.bbox, geometry_bnode))
        geom_type_uri = sf_ns.Polygon
    else:
        geom_type_uri = URIRef(f"{sf_uri}{geom.geom_type}")

        graph.add((location_bnode, locn_ns.geometry, geometry_bnode))
    graph.add((geometry_bnode, RDF.type, geom_type_uri))
    graph.add((geometry_bnode, GEO.asWKT, geometry_wkt_literal))
    if name:
        geographic_name_literal = Literal(name, datatype=XSD.string)
        graph.add((location_bnode, locn_ns.geographicName, geographic_name_literal))

    return ParentNode(location_bnode, graph)
