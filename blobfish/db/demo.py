import logging

from ..utils.geo_rdf_utils import parse_geojson_file, create_rdf_location
from ..utils.graph_utils import load_to_graphdb
from ..utils.logger import set_up_logger


if __name__ == "__main__":
    set_up_logger(level=logging.INFO)
    transposition = parse_geojson_file("kanawha-transpo-area-v01.geojson")
    transposition_node = create_rdf_location(transposition.geom, "kanawha_transposition", bbox=True)
    logging.info(transposition_node.attached_graph.serialize())
    response = load_to_graphdb(transposition_node.attached_graph, "test", "http://db:7200")
    logging.info(response.status_code)
