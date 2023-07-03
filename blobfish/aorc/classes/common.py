from typing import Any
import rdflib
from dataclasses import dataclass
from pyshacl import validate


@dataclass
class BasicDescriptors:
    title: str
    dataset_id: str
    name: str
    url: str
    notes: str


@dataclass
class ProvenanceMetadata:
    docker_image: str
    remote_compose_file: str
    remote_docker_file: str
    git_repo: str
    commit_hash: str
    docker_repo: str
    digest_hash: str


# class GraphCreator:
#     def __init__(self, shacl_fn: str | None, ont_fn: str | None) -> None:
#         self.tracked_graphs: dict[Any, rdflib.Graph] = {}
#         self.shacl_graph = self.__load_shacl(shacl_fn)
#         self.ont_graph = self.__load_ont(ont_fn)

#     @staticmethod
#     def __load_shacl(shacl_fn: str | None) -> rdflib.Graph | None:
#         if shacl_fn:
#             graph = rdflib.Graph()
#             graph.parse(shacl_fn)
#             return graph
#         return shacl_fn

#     @staticmethod
#     def __load_ont(ont_fn: str | None) -> rdflib.Graph | None:
#         if ont_fn:
#             graph = rdflib.Graph()
#             graph.parse(ont_fn)
#             return graph
#         return ont_fn

#     def __create_graph(self, id: str | None = None) -> rdflib.Graph:
#         graph = rdflib.Graph()
#         if id:
#             self.tracked_graphs[id] = graph
#         return graph

#     def get_graph(self, id: str | None = None):
#         if id:
#             for graph in self.tracked_graphs:
#                 if graph.id == id:
#                     return graph
#             else:
#                 return self.__create_graph(id)
#         else:
#             return self.__create_graph()

#     def validate_graphs(self, **kwargs):
#         if self.shacl_graph:
#             for graph in self.tracked_graphs.values():
#                 validate_args = {"data_graph": graph, "shacl_graph": self.shacl_graph}
#                 if self.ont_graph:
#                     validate_args["ont_graph"] = self.ont_graph
#                 if kwargs:
#                     validate_args.update(kwargs)
#                 validate(**validate_args)
#         else:
#             raise ValueError("No shacl graph associated with graph creator, validation failed")

#     def upload_graphs_to_ckan(self):
#         for id, graph in self.tracked_graphs.items():
#             pass
