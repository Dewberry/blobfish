""" Script to handle creation of CONUS composites from s3 mirrors of AORC precip files utilizing and adding to transfer job metadata """

import pathlib
from rdflib import RDF, OWL, XSD, DCAT, DCTERMS, PROV, Graph
from typing import cast

from ..pyrdf import AORC

def create_graph(ttl_directory: str) -> Graph:
    g = Graph()
    g.bind("dcat", DCAT)
    g.bind("dct", DCTERMS)
    g.bind("prov", PROV)
    for filepath in pathlib.Path(ttl_directory).glob("*.ttl"):
        g.parse(filepath)
    return g

def query_metadata(g: Graph) -> None:
    # TODO: implement query, maybe with subquery, which gets distinct PeriodOfTime BNodes designating temporal coverage for source dataset (with at least 2 source datasets sharing the coverage)
    # Then loop through temporal nodes and query for associated source datasets -> mirror datasets & distributions to use in composite creation
    pass

def main(ttl_directory: str) -> None:
    g = create_graph(ttl_directory)
    query_metadata(g)

if __name__ == "__main__":
    main("mirrors")


