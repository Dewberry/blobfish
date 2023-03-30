import requests
from collections.abc import Generator
from rdflib import Graph

from .const import CKAN_URL

class RDFHandler:
    def __init__(self, ckan_url: str = CKAN_URL, init_ttl: str | None = None) -> None:
        self.graph = self.__create_graph(init_ttl)
        self.ckan_url = ckan_url

    def __create_graph(self, ttl: str | None) -> Graph:
        graph = Graph()
        if ttl:
            graph.parse(ttl)
        return graph

    def __get_ttl_urls(self, include_ontology: bool) -> Generator[str, None, None]:
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
        for url in self.__get_ttl_urls(include_ontology):
            with requests.get(url) as resp:
                ttl = resp.content
                self.graph.parse(data=ttl)

    def serialize(self, outfile: str) -> None:
        self.graph.serialize(destination=outfile)

    def query(self, query_string: str, namespace_bindings: dict, variable_bindings: dict):
        return self.graph.query(query_string, initNs=namespace_bindings, initBindings=variable_bindings)
