""" Unit testing for geospatial RDF creation and querying capabilities"""
import logging
import os
import unittest
import sys
from tempfile import TemporaryDirectory
from typing import cast

from rdflib import XSD, Literal
from rdflib.query import ResultRow

from ..utils.cloud_utils import download_object
from ..utils.geo_rdf_utils import create_rdf_location, parse_geojson_file
from ..utils.graph_utils import create_repo, delete_repo, enable_geosparql, load_to_graphdb, query_repo


class GeospatialQueryCapability(unittest.TestCase):
    def setUp(self):
        """Set up test environment"""
        self.logger = logging.getLogger()
        self.logger.level = logging.INFO
        stream_handler = logging.StreamHandler(sys.stdout)
        self.logger.addHandler(stream_handler)
        self.geojson_directory = TemporaryDirectory()
        self.geojson_file = os.path.join(self.geojson_directory.name, "test.geojson")
        self.repository = "test"
        self.region_name = "Test Region"
        self.base_url = "http://db:7200"
        download_object("tempest", "watersheds/kanawha/kanawha-transpo-area-v01.geojson", self.geojson_file)
        create_repo(self.repository, self.base_url)

    def test_a_create_node(self):
        """Test creating a valid dct:Location instance"""
        spatial_object = parse_geojson_file(self.geojson_file)
        spatial_node = create_rdf_location(spatial_object.geom, self.region_name)
        rdflib_query_results = spatial_node.attached_graph.query(
            """
        PREFIX dct: <http://purl.org/dc/terms/>
        PREFIX locn: <http://w3.org/ns/locn#>

        SELECT  ?n
        WHERE   {
            ?l a dct:Location .
            ?l locn:geographicName ?n
        }
        """
        )
        for row in rdflib_query_results:
            row = cast(ResultRow, row)
            retrieved_name = row.get("n")
            expected_name = Literal(self.region_name, datatype=XSD.string)
            self.assertEqual(
                retrieved_name,
                expected_name,
                f"Name retrieved from query: {retrieved_name}. Expected name: {expected_name}",
            )
            break

    def test_b_upload_node(self):
        """Test upload of created spatial node to repository"""
        spatial_object = parse_geojson_file(self.geojson_file)
        spatial_node = create_rdf_location(spatial_object.geom, self.region_name)
        response = load_to_graphdb(spatial_node.attached_graph, self.repository, self.base_url)
        self.assertTrue(
            response.ok, f"Test node upload query returned with failure status code: {response.status_code}"
        )

    def test_c_geosparql_query(self):
        """Test geosparql query"""
        enable_geosparql(self.repository, self.base_url)
        spatial_object = parse_geojson_file(self.geojson_file)
        spatial_node = create_rdf_location(spatial_object.geom, self.region_name)
        load_to_graphdb(spatial_node.attached_graph, self.repository, self.base_url)
        query = """
        PREFIX sf: <http://www.opengis.net/ont/sf#>
        PREFIX geo: <http://www.opengis.net/ont/geosparql#>
        PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
		PREFIX locn: <http://w3.org/ns/locn#>

        SELECT ?n
        WHERE {
            ?l locn:geometry ?g .
            ?g geo:asWKT ?gWKT .
            ?l locn:geographicName ?n .
            FILTER(geof:sfWithin('''
            <http://www.opengis.net/def/crs/OGC/1.3/CRS84> POINT(-81.8745 37.1171)
            '''^^geo:wktLiteral, ?gWKT
            ))
        }
        """
        response = query_repo(self.repository, query, self.base_url)
        names = response.text.splitlines()
        self.assertTrue(self.region_name in names, "Expected test region name not found in geosparql query results")

    def tearDown(self) -> None:
        """Deletes geojson and gets rid of created repository"""
        delete_repo(self.repository, self.base_url)
        self.geojson_directory.cleanup()


if __name__ == "__main__":
    unittest.main()
