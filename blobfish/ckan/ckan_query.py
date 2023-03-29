import requests
import fiona
import pathlib
from io import BytesIO
from shapely.geometry import shape
import shapely
from zipfile import ZipFile
from tempfile import TemporaryDirectory
from dataclasses import dataclass
from collections.abc import Generator
from rdflib import Graph, URIRef, Literal, BNode, RDF, DCTERMS
from rdflib.namespace._GEO import GEO
from rdflib.query import Result

from .const import CKAN_URL, RFC_SHP_URL
from ..pyrdf import AORC
from ..aorc.const import RFC_INFO_LIST


@dataclass
class RFCCoverage:
    rfc: str
    wkt: str


def get_rfc_coverages(shp_url = RFC_SHP_URL) -> list[RFCCoverage]:
    aliases = [rfc.alias for rfc in RFC_INFO_LIST]
    print(aliases)
    coverages = []
    with TemporaryDirectory() as tmpdir:
        shp_path = extract_shapefile(shp_url, tmpdir)
        with fiona.open(shp_path, "r") as shp:
            for f in shp:
                rfc = f["properties"]["BASIN_ID"][:2]
                if rfc in aliases:
                    wkt = shape(f["geometry"]).wkt
                    coverages.append(RFCCoverage(rfc, wkt))
    return coverages


def extract_shapefile(zip_url: str, extract_dir: str) -> str:
    with requests.get(zip_url, stream=True) as resp:
        with ZipFile(BytesIO(resp.content)) as z:
            z.extractall(extract_dir)
            shp_path = pathlib.Path(extract_dir).glob("*.shp").__next__()
            return str(shp_path)


class RDFHandler:
    def __init__(self, ckan_url: str = CKAN_URL) -> None:
        self.graph = Graph()
        self.ckan_url = ckan_url

    def get_ttl_urls(self, include_ontology: bool) -> Generator[str, None, None]:
        with requests.get(f"{self.ckan_url}/api/3/action/package_search") as resp:
            data = resp.json()
            datasets = data.get("result").get("results")
            for dataset in datasets:
                dataset_resources = dataset.get("resources")
                for resource in dataset_resources:
                    if resource.get("mimetype") == "text/turtle":
                        if not include_ontology:
                            tag_names = [tag.get("name") for tag in dataset.get("tags")]
                            if "ontology" in tag_names:
                                continue
                        yield resource.get("url")

    def load_graph(self, include_ontology: bool = False) -> None:
        for url in self.get_ttl_urls(include_ontology):
            with requests.get(url) as resp:
                ttl = resp.content
                self.graph.parse(data=ttl)

    def add_spatial_coverages(self):
        self.graph.bind("locn", "http://www.w3.org/ns/locn#")
        for coverage in get_rfc_coverages():
            location_bnode = BNode()
            wkt_literal = Literal(coverage.wkt, datatype=GEO.asWKT)
            self.graph.add((location_bnode, RDF.type, DCTERMS.Location))
            self.graph.add((location_bnode, URIRef("locn:geometry"), wkt_literal))
            rfc_uri = URIRef(f"https://www.weather.gov/{coverage.rfc.lower()}rfc")
            self.graph.add((rfc_uri, DCTERMS.spatial, location_bnode))

    def query_rfc_offices(self) -> Result:
        res = self.graph.query(
            """
        SELECT ?r
        WHERE {
            ?r rdf:type aorc:RFC
        }
        """,
            initNs={"rdf": RDF, "aorc": AORC},
        )
        return res





if __name__ == "__main__":
    rdf_handler = RDFHandler()
    rdf_handler.load_graph()
    for res in rdf_handler.query_rfc_offices():
        print(res)
