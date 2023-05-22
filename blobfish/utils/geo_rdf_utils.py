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
    """Ensures that geometry type of input geometry is either a Point, a Polygon, or a MultiPolygon, the expected options for a spatial coverage field in RDF

    Args:
        geom (shapely.Geometry): geometry to validate

    Raises:
        ValueError: Error raised if geometry type is not of expected type (Point for point location, Polygon or MultiPolygon for spatial coverage, Polygon for bounding box)
    """
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
    """Formats WKT string as is expected for RDF formatting

    Returns:
        str: Formatted WKT
    """
    if bbox:
        if geom.geom_type == "Point":
            logging.warning(
                "Point provided with bbox parameter set to true. Ignoring and proceeding with wkt formatting using point geometry"
            )
        else:
            geom = box(*geom.bounds)
    return f"<{crs_uri}> {geom.wkt}"


def load_feature(features: list[dict], index: int | None) -> dict:
    """Loads a single feature from geojson features

    Args:
        features (list[dict]): Geojson features
        index (int | None): Index to use when selecting feature

    Raises:
        ValueError: Error to indicate that geojson provided has more than one feature and no index was provided
        ValueError: Error to indicate that geojson provided does not have enough features to select the provided index

    Returns:
        dict: Geojson feature
    """
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
    """Parse geojson string to a NamedSpatialProperty

    Args:
        geojson_string (str): GeoJSON string
        index (int | None, optional): Index of feature of interest in geojson. Defaults to None.

    Returns:
        NamedSpatialProperty: Named geometry parsed from geojson
    """
    geojson = json.loads(geojson_string)
    features = geojson["features"]
    name = geojson["name"]
    feature = load_feature(features, index)
    geom = shape(feature["geometry"])
    validate_geom_type(geom)
    return NamedSpatialProperty(name, geom)


def parse_geojson_file(fn: str, index: int | None = None) -> NamedSpatialProperty:
    """Parse geojson file to a NamedSpatialProperty

    Args:
        fn (str): Filename
        index (int | None, optional): Index of feature of interest in geojson. Defaults to None.

    Returns:
        NamedSpatialProperty: Named geometry parsed from geojson
    """
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
    -- Point
    ```
    @prefix locn: <http://w3.org/ns/locn#> .
    @prefix geo: <http://www.opengis.net/ont/geosparql#> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    @prefix dct: <http://purl.org/dc/terms/> .
    @prefix sf: <http://www.opengis.net/ont/sf#> .

    [] a dct:Location ;
      locn:geographicName "The house of Anne Frank"^^xsd:string ;
      locn:geometry [
        a sf:Point ;
        geo:asWKT "<http://www.opengis.net/def/crs/OGC/1.3/CRS84> POINT(4.88412 52.37509)"^^geo:wktLiteral
      ] ;
    ```
    -- Polygon
    ```
    @prefix locn: <http://w3.org/ns/locn#> .
    @prefix geo: <http://www.opengis.net/ont/geosparql#> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    @prefix dct: <http://purl.org/dc/terms/> .
    @prefix sf: <http://www.opengis.net/ont/sf#> .

    [] a dct:Location ;
      locn:geographicName "The house of Anne Frank"^^xsd:string ;
      locn:geometry [
        a sf:Polygon ;
        geo:asWKT "<http://www.opengis.net/def/crs/OGC/1.3/CRS84> POLYGON ((
        4.8842353 52.375108 , 4.884276 52.375153 ,
        4.8842567 52.375159 , 4.883981 52.375254 ,
        4.8838502 52.375109 , 4.883819 52.375075 ,
        4.8841037 52.374979 , 4.884143 52.374965 ,
        4.8842069 52.375035 , 4.884263 52.375016 ,
        4.8843200 52.374996 , 4.884255 52.374926 ,
        4.8843289 52.374901 , 4.884451 52.375034 ,
        4.8842353 52.375108
        ))"^^geo:wktLiteral ;
      ] ;
    ```
    -- Bounding box (has both dcat:bbox data property and locn:geometry object property to both conform to DCAT and provide a more unified and therefore queryable approach to spatial attribution)
    ```
    @prefix locn: <http://w3.org/ns/locn#> .
    @prefix geo: <http://www.opengis.net/ont/geosparql#> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    @prefix dct: <http://purl.org/dc/terms/> .
    @prefix sf: <http://www.opengis.net/ont/sf#> .

    [] a dct:Location ;
      locn:geographicName "The Netherlands"^^xsd:string ;
      locn:geometry [
        a sf:Polygon ;
        geo:asWKT "<http://www.opengis.net/def/crs/OGC/1.3/CRS84> POLYGON((
        3.053 47.975 , 7.24  47.975 ,
        7.24  53.504 , 3.053 53.504 ,
        3.053 47.975
        ))"^^geo:wktLiteral ;
      ]
      dcat:bbox "<http://www.opengis.net/def/crs/OGC/1.3/CRS84> POLYGON((
      3.053 47.975 , 7.24  47.975 ,
      7.24  53.504 , 3.053 53.504 ,
      3.053 47.975
    ))"^^geo:wktLiteral ;
    ```

    Location syntax recommendations provided:
    https://semiceu.github.io/Core-Location-Vocabulary/releases/2.0.2/#examples
    https://www.w3.org/TR/vocab-dcat-2/#Class:Location
    https://www.w3.org/TR/vocab-dcat-2/#ex-spatial-coverage-bbox
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
        graph.add((location_bnode, DCAT.bbox, geometry_wkt_literal))
    geom_type_uri = URIRef(f"{sf_uri}{geom.geom_type}")
    graph.add((location_bnode, locn_ns.geometry, geometry_bnode))
    graph.add((geometry_bnode, RDF.type, geom_type_uri))
    graph.add((geometry_bnode, GEO.asWKT, geometry_wkt_literal))
    if name:
        geographic_name_literal = Literal(name, datatype=XSD.string)
        graph.add((location_bnode, locn_ns.geographicName, geographic_name_literal))

    return ParentNode(location_bnode, graph)
