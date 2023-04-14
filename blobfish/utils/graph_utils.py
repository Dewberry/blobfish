import rdflib
import logging
from typing import Any
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
                    ttl_body = filter_graph.serialize(format="ttl")
                    upload_body(bucket, fn, ttl_body, client)
                else:
                    filter_graph.serialize(fn, format="ttl")
                    logging.info(f"Graph serialized to {fn}")
        elif self.default_graph:
            fn = filepath_pattern.format("")
            self.default_graph.serialize(fn, format="ttl")
            logging.info(f"Graph serialized to {fn}")
        else:
            logging.error(f"No graph object was created, serialization failed")
            raise ValueError

