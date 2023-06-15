""" Utilities used in creating RDF metadata for s3 mirror creation """
import rdflib
import requests
import pyshacl


def skolemize_source_dataset(source_dataset: dict) -> str:
    return ""


def create_source_dataset(source_dataset: dict) -> rdflib.IdentifiedNode:
    return rdflib.BNode()


def create_mirror_dataset(mirror_dataset: dict) -> rdflib.IdentifiedNode:
    return rdflib.URIRef()


class GraphCreator:
    def __init__(self, shacl_fn: str) -> None:
        self.tracked_graphs = {}
        self.shacl_graph = self.__load_shacl(shacl_fn)

    @staticmethod
    def __load_shacl(shacl_fn: str) -> rdflib.Graph:
        graph = rdflib.Graph()
        graph = graph.parse(shacl_fn)
        return graph

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

    def validate_graphs(self):
        for graph in self.tracked_graphs.values():
            pass

    def upload_graphs_to_ckan(self):
        for id, graph in self.tracked_graphs.items():
            pass
