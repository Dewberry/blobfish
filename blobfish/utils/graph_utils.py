import json
import logging
import os
from tempfile import TemporaryDirectory
from typing import Any
from urllib.parse import urlencode

import rdflib
import requests

from .cloud_utils import upload_body
from .constants import DEFAULT_REPOSITORY_CONFIG


class GraphCreator:
    def __init__(self, bindings: dict) -> None:
        """Initializes graph creator which separates out graph objects based on a provided filter

        Args:
            bindings (dict): Bindings to supply to the default created graph (eg: {"dct": rdflib.DCTERMS})
        """
        self.bindings = bindings
        self.filter_graphs = dict()
        self.default_graph = None

    def __create_graph(self) -> rdflib.Graph:
        """Creates graph

        Returns:
            rdflib.Graph: New graph object with class bindings
        """
        logging.info("rdflib.Graph object created by graph creator")
        g = rdflib.Graph()
        for prefix, ns in self.bindings.items():
            g.bind(prefix, ns)
        return g

    def get_graph(self, filter_key: str | None = None) -> rdflib.Graph:
        """Attempts to find a graph which matches a given filter key and returns the default blank graph if no graph is found

        Args:
            filter_key (str | None, optional): Filter key to use to try to get a graph. Defaults to None.

        Returns:
            rdflib.Graph: _description_
        """
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
        """Serializes all graphs associated with creator object

        Args:
            filepath_pattern (str): Pattern to format using provided filter key to generate filename (for local serialization) or path name (for s3 serialization)
            to_s3 (bool, optional): If true, upload graphs to s3. Defaults to False.
            client (Any | None, optional): s3 client used in upload. Defaults to None.
            bucket (str | None, optional): s3 bucket to which graphs will be uploaded. Defaults to None.

        Raises:
            ValueError: If no graph objects were created by the creator object, fails with ValueError
        """
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


def query_repo(
    repository: str,
    query: str,
    base_url: str = "http://localhost:7200",
    infer: bool = True,
    sameAs: bool = False,
    limit: int = 1000,
    offset: int = 0,
) -> requests.Response:
    """Queries GraphDB repository

    Args:
        repository (str): ID string of repository
        query (str): Query string
        base_url (_type_, optional): GraphDB URL. Defaults to "http://localhost:7200".
        infer (bool, optional): If true, use inference to imply classes of resources in repository (to see type of inference, check repository settings). Defaults to True.
        sameAs (bool, optional): If true, use GraphDB owl:sameAs optimization. If false, do not use optimization. Defaults to False.
        limit (int, optional): Limit of resources to retrieve. Defaults to 1000.
        offset (int, optional): Offset to use when retrieving resources (skips provided number of resources). Defaults to 0.

    Returns:
        requests.Response: Response of GraphDB server
    """
    # sameAs requires infer
    if not infer and sameAs:
        sameAs = False
    # encode query string
    endpoint = f"{base_url}/repositories/{repository}"
    params = {"query": query, "infer": infer, "sameAs": sameAs, "limit": limit, "offset": offset}
    url_encoded_params = urlencode(params, encoding="utf-8")
    r = requests.post(endpoint, data=url_encoded_params, headers={"Content-Type": "application/x-www-form-urlencoded"})
    return r


def create_repo(repository: str, base_url: str = "http://localhost:7200", **kwargs) -> requests.Response:
    """Creates GraphDB repository with default repository parameters, stored in utils/constants.py, unless overriden using kwargs

    Args:
        repository (str): ID string of repository
        base_url (str, optional): GraphDB URL. Defaults to "http://localhost:7200".

    Returns:
        requests.Response: Response of GraphDB server
    """
    endpoint = f"{base_url}/rest/repositories"
    params = DEFAULT_REPOSITORY_CONFIG
    params.update({"id": repository})
    params.update(kwargs)
    params_string = json.dumps(params)
    r = requests.post(endpoint, data=params_string, headers={"Content-Type": "application/json"})
    return r


def verify_repo(repository: str, base_url: str = "http://localhost:7200") -> bool:
    """Verifies that repository exists in GraphDB instance

    Args:
        repository (str): ID string of repository
        base_url (str, optional): GraphDB URL. Defaults to "http://localhost:7200".

    Raises:
        ValueError: If repository is not found, raise error
        exc: General exception to raise upon unexpected failure point

    Returns:
        bool: Returns true if successfully verified
    """
    endpoint = f"{base_url}/rest/repositories"
    r = requests.get(endpoint)
    data = r.json()
    try:
        ids = [repo.get("id") for repo in data]
        if repository in ids:
            return True
        else:
            raise ValueError("Repository not found")
    except Exception as exc:
        raise exc


def delete_repo(repository: str, base_url: str = "http://localhost:7200", location: str = "") -> requests.Response:
    """Deletes GraphDB repository

    Args:
        repository (str): ID string of repository
        base_url (str, optional): GraphDB URL. Defaults to "http://localhost:7200".
        location (str, optional): Remote location of repository. Defaults to "", or no remote location.

    Returns:
        requests.Response: GraphDB server response
    """
    endpoint = f"{base_url}/rest/repositories/{repository}?location={location}"
    r = requests.delete(endpoint)
    return r


def load_to_graphdb(graph: rdflib.Graph, repository: str, base_url: str = "http://localhost:7200") -> requests.Response:
    """Loads provided rdflib.Graph data to GraphDB repository

    Args:
        graph (rdflib.Graph): Graph data to upload
        repository (str): ID string of target repository
        base_url (str, optional): GraphDB URL. Defaults to "http://localhost:7200".

    Returns:
        requests.Response: GraphDB server response
    """
    endpoint = f"{base_url}/repositories/{repository}/statements"
    with TemporaryDirectory() as tempdir:
        tempf = os.path.join(tempdir, "output.ttl")
        graph.serialize(tempf, format="turtle")
        with open(tempf, "rb") as f:
            r = requests.post(endpoint, data=f, headers={"Content-Type": "application/x-turtle"})
    return r


def enable_geosparql(
    repository: str, base_url: str = "http://localhost:7200", infer: bool = False, sameAs: bool = False
) -> requests.Response:
    """Enables GeoSPARQL support in GraphDB repository

    Args:
        repository (str): ID string of repository
        base_url (str, optional): GraphDB URL. Defaults to "http://localhost:7200".
        infer (bool, optional): If true, use inference to imply classes of resources in repository (to see type of inference, check repository settings). Defaults to True.
        sameAs (bool, optional): If true, use GraphDB owl:sameAs optimization. If false, do not use optimization. Defaults to False.

    Returns:
        requests.Response: GraphDB response
    """
    # sameAs requires infer
    if not infer and sameAs:
        sameAs = False
    endpoint = f"{base_url}/repositories/{repository}/statements"
    enable_query = """
    PREFIX geoSparql: <http://www.ontotext.com/plugins/geosparql#>
    INSERT DATA { [] geoSparql:enabled "true" . }
    """
    params = {"update": enable_query, "infer": infer, "sameAs": sameAs}
    url_encoded_params = urlencode(params)
    r = requests.post(endpoint, data=url_encoded_params, headers={"Content-Type": "application/x-www-form-urlencoded"})
    return r
