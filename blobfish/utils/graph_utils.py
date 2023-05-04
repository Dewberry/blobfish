import rdflib
import logging
import requests
from typing import Any
from tempfile import TemporaryFile
from .cloud_utils import upload_body


class GraphCreator:
    def __init__(self, bindings: dict) -> None:
        self.bindings = bindings
        self.filter_graphs = dict()
        self.default_graph = None

    def __create_graph(self) -> rdflib.Graph:
        logging.info("rdflib.Graph object created by graph creator")
        g = rdflib.Graph()
        for prefix, ns in self.bindings.items():
            g.bind(prefix, ns)
        return g

    def get_graph(self, filter_key: str | None = None) -> rdflib.Graph:
        if filter_key:
            filter_graph = self.filter_graphs.get(filter_key)
            if filter_graph:
                return filter_graph
            logging.info(f"No graph found for filter key {filter_key}")
            filter_graph = self.__create_graph()
            self.filter_graphs[filter_key] = filter_graph
            return filter_graph
        if self.default_graph:
            return self.default_graph
        self.default_graph = self.__create_graph()
        return self.default_graph

    def serialize_graphs(
        self, filepath_pattern: str, to_s3: bool = False, client: Any | None = None, bucket: str | None = None
    ) -> None:
        if len(self.filter_graphs.items()) > 0:
            for filter_key, filter_graph in self.filter_graphs.items():
                fn = filepath_pattern.format(filter_key)
                if to_s3 and bucket:
                    ttl_body = filter_graph.serialize(format="turtle")
                    upload_body(bucket, fn, ttl_body, client)
                else:
                    filter_graph.serialize(fn, format="turtle")
                    logging.info(f"Graph serialized to {fn}")
        elif self.default_graph:
            fn = filepath_pattern.format("")
            self.default_graph.serialize(fn, format="turtle")
            logging.info(f"Graph serialized to {fn}")
        else:
            logging.error(f"No graph object was created, serialization failed")
            raise ValueError


def load_to_graphdb(graph: rdflib.Graph, repository: str, base_url: str = "http://localhost:7200") -> None:
    # Not working correctly for some reason
    with TemporaryFile() as tempf:
        graph.serialize(tempf, format="turtle")
        endpoint = f"{base_url}/repositories/{repository}/statements"
        headers = {"Content-Type": "text/turtle"}
        resp = requests.post(endpoint, headers=headers, data=tempf.read())
        logging.info(f"{resp.url}: {resp.status_code}")
