import logging

import requests
from rdflib import Graph


def create_ckan_resource(
    download_url: str,
    format: str,
    compress_format: str | None,
    description: str,
    s3: bool,
) -> dict:
    args_dict = {}
    if s3:
        args_dict["access_rights"] = "Access to distribution requires access to parent s3 bucket"
    if compress_format:
        args_dict["compress_format"] = compress_format
    args_dict.update(
        {
            "url": download_url,
            "format": format,
            "description": description,
        }
    )
    return args_dict


def query_ckan_catalog(catalog_url: str) -> Graph:
    logging.info(f"Querying CKAN catalog {catalog_url}")
    graph = Graph()
    if catalog_url.endswith(".ttl"):
        with requests.get(catalog_url) as resp:
            graph.parse(source=resp.content, format="ttl")
            return graph
    else:
        raise ValueError(f"Expected catalog url ending in '.ttl', got {catalog_url}")
