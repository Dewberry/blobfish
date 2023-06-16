from dataclasses import dataclass
import datetime
import rdflib
from pyshacl import validate

@dataclass
class RFCInfo:
    """
    Data Property: Regional Forecast Center (RFC) names and aliases
    """

    alias: str
    name: str

@dataclass
class AORCDataURL:
    url: str
    data_start_dt: datetime.datetime
    rfc_alias: str
    last_modified: datetime.datetime = None

    def s3_key(self, prefix: str = "test/mirrors/aorc/precip/") -> str:
        if prefix[-1] != "/":
            prefix += "/"
        url_parts = self.url.split("/")
        return f"{prefix}{'/'.join(url_parts[-3:])}"

class GraphCreator:
    def __init__(self, shacl_fn: str | None, ont_fn: str | None) -> None:
        self.tracked_graphs = {}
        self.shacl_graph = self.__load_shacl(shacl_fn)
        self.ont_graph = self.__load_ont(ont_fn)

    @staticmethod
    def __load_shacl(shacl_fn: str | None) -> rdflib.Graph | None:
        if shacl_fn:
            graph = rdflib.Graph()
            graph.parse(shacl_fn)
            return graph
        return shacl_fn

    @staticmethod
    def __load_ont(ont_fn: str | None) -> rdflib.Graph | None:
        if ont_fn:
            graph = rdflib.Graph()
            graph.parse(ont_fn)
            return graph
        return ont_fn

    def __create_graph(self, id: str | None = None) -> rdflib.Graph:
        graph = rdflib.Graph()
        if id:
            self.tracked_graphs[id] = graph
        return graph

    def get_graph(self, id: str | None = None):
        if id:
            for graph in self.tracked_graphs:
                if graph.id == id:
                    return graph
            else:
                return self.__create_graph(id)
        else:
            return self.__create_graph()

    def validate_graphs(self, **kwargs):
        if self.shacl_graph:
            for graph in self.tracked_graphs.values():
                validate_args = {"data_graph":graph, "shacl_graph": self.shacl_graph}
                if self.ont_graph:
                    validate_args["ont_graph"] = self.ont_graph
                if kwargs:
                    validate_args.update(kwargs)
                validate(**validate_args)
        else:
            raise ValueError("No shacl graph associated with graph creator, validation failed")

    def upload_graphs_to_ckan(self):
        for id, graph in self.tracked_graphs.items():
            pass
