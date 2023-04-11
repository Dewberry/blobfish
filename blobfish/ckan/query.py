import requests
import fiona
import pathlib
import enum
from io import BytesIO
from shapely.geometry import shape
from shapely import Point, Polygon, MultiPolygon
from zipfile import ZipFile
from tempfile import TemporaryDirectory
from dataclasses import dataclass
from collections.abc import Generator
from rdflib import URIRef, Literal, BNode, RDF, DCTERMS
from rdflib.namespace._GEO import GEO
from rdflib.query import Result
from typing import cast

from .const import RFC_SHP_URL, GEOF, SF, CKAN_URL
from .load import RDFHandler
from ..pyrdf import AORC
from ..aorc.const import RFC_INFO_LIST

class CoverageGeometryType(enum.Enum):
    POLYGON = enum.auto()
    MULTIPOLYGON = enum.auto()

@dataclass
class RFCCoverage:
    rfc: str
    wkt: str
    geom_type: CoverageGeometryType

@dataclass
class RFCGeometry:
    rfc: str
    geom: Polygon | MultiPolygon


def extract_shapes(zip_url: str, extract_dir: str) -> Generator[RFCGeometry, None, None]:
    aliases = [rfc.alias for rfc in RFC_INFO_LIST]
    with requests.get(zip_url, stream=True) as resp:
        with ZipFile(BytesIO(resp.content)) as z:
            z.extractall(extract_dir)
            shp_path = pathlib.Path(extract_dir).glob("*.shp").__next__()
            with fiona.open(shp_path, "r") as shp:
                for f in shp:
                    rfc = f["properties"]["BASIN_ID"][:2]
                    if rfc in aliases:
                        coverage_shape = shape(f["geometry"])
                        yield RFCGeometry(rfc, coverage_shape)

def identify_rfc_alias(x: float, y: float, zip_url: str = RFC_SHP_URL) -> str:
    point = Point(x, y)
    with TemporaryDirectory() as tmpdir:
        for coverage_shape in extract_shapes(zip_url, tmpdir):
            if coverage_shape.geom.contains(point):
                return coverage_shape.rfc
    raise ValueError(f"Point ({x, y}) is not within RFC regions found in RFC_INFO_LIST. Check that input point and shapefile zip_url are in same CRS.")


class SpatialRDFHandler(RDFHandler):
    def __init__(self, ckan_url: str = CKAN_URL, init_ttl: str | None = None) -> None:
        if ckan_url:
            super().__init__(ckan_url, init_ttl)
        else:
            super().__init__(init_ttl = init_ttl)

    @staticmethod
    def get_rfc_coverages(shp_url = RFC_SHP_URL) -> list[RFCCoverage]:
        coverages = []
        with TemporaryDirectory() as tmpdir:
            for coverage_shape in extract_shapes(shp_url, tmpdir):
                wkt = coverage_shape.geom.wkt
                geom_type = CoverageGeometryType[coverage_shape.geom.geom_type.upper()]
                coverages.append(RFCCoverage(coverage_shape.rfc, wkt, geom_type))
        return coverages

    @staticmethod
    def prepend_crs(input_wkt: str) -> Literal:
        wkt_with_crs = f"<http://www.opengis.net/def/crs/OGC/1.3/CRS84> {input_wkt}"
        literal = Literal(wkt_with_crs, datatype=GEO.wktLiteral)
        return literal

    def add_spatial_coverages(self) -> None:
        self.graph.bind("geo", GEO)
        self.graph.bind("sf", SF)
        for coverage in self.get_rfc_coverages():
            spatial_node = BNode()
            self.graph.add((spatial_node, RDF.type, GEO.Feature))
            geom_node = BNode()
            if coverage.geom_type.name == "POLYGON":
                geom_type = SF.Polygon
            else:
                geom_type = SF.MultiPolygon
            self.graph.add((geom_node, RDF.type, geom_type))
            wkt_literal = self.prepend_crs(coverage.wkt)
            self.graph.add((geom_node, GEO.asWKT, wkt_literal))
            self.graph.add((spatial_node, GEO.hasGeometry, geom_node))
            rfc_uri = URIRef(f"https://www.weather.gov/{coverage.rfc.lower()}rfc")
            self.graph.add((rfc_uri, DCTERMS.spatial, spatial_node))

    def identify_rfc_datasets(self, x: float, y: float) -> Result:
        raise NotImplementedError("This function relies on geosparql functions which are not implemented in the SPARQL processor for RDFLib")
        point = Point(x, y).wkt
        point_literal = self.prepend_crs(point)
        query = """
        SELECT  ?s3
        WHERE   {
            ?r rdf:type aorc:RFC .
            ?r dct:spatial/geosparql:hasGeometry ?g .
            ?g geosparql:asWKT ?w .
            ?r ^aorc:hasRFC ?p .
            ?p dcat:dataset ?sda .
            ?sda ^aorc:hasSourceDataset ?s3 .
            FILTER (geof:sfWithin(?p, ?w))
        }
        """
        results = self.graph.query(query, initNs={"rdf": RDF, "aorc": AORC, "dct": DCTERMS, "geosparql": GEO, "geof": GEOF}, initBindings={"p": point_literal})
        return results

if __name__ == "__main__":
    # Expected output: MB (Missouri Basin)
    rfc = identify_rfc_alias(-99, 40)
    print(rfc)
