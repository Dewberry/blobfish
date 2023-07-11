"""General functions to interface with CKAN instance"""
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
    """Creates the general dict form of a dcat:Distribution instance as expected in CKAN

    Args:
        download_url (str): Download URL
        format (str): URI of data format
        compress_format (str | None): URI of compression format, if there is compression
        description (str): Description of distribution
        s3 (bool): True if distribution is on s3 and has implied access rights constraint

    Returns:
        dict: Distribution data
    """
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
    """Queries provided catalog for RDF data

    Args:
        catalog_url (str): URL of dataset catalog

    Raises:
        ValueError: Catalog URL doesn't have expected format

    Returns:
        Graph: Parsed catalog graph
    """
    logging.info(f"Querying CKAN catalog {catalog_url}")
    graph = Graph()
    if catalog_url.endswith(".ttl"):
        with requests.get(catalog_url) as resp:
            graph.parse(source=resp.content, format="ttl")
            return graph
    else:
        raise ValueError(f"Expected catalog url ending in '.ttl', got {catalog_url}")
