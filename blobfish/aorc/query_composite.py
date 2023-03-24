""" Script to test SPARQL query ease of use for retrieving metadata """
import datetime
import rdflib
from ..pyrdf import AORC

def create_graph(ttl: str) -> rdflib.Graph:
    g = rdflib.Graph()
    g.parse(ttl)
    return g


def get_composites_time_range(
    ttl: str, start_time: datetime.datetime, end_time: datetime.datetime
) -> rdflib.query.Result:
    graph = create_graph(ttl)
    """Gets composite datasets which have a temporal coverage which falls within the start and end time. Also gets the distribution URLS of the source datasets used to create the composite dataset.

    Args:
        start_time (datetime.datetime): Start of period of interest
        end_time (datetime.datetime): End of period of interest
    """
    query = """
    SELECT  ?cdata ?sdist
    WHERE {
        ?cdata rdf:type aorc:CompositeDataset .
        ?cdata aorc:isCompositeOf ?mdata .
        ?mdata aorc:hasSourceDataset ?sdata .
        ?sdata dcat:distribution ?sdist .
        ?cdata dct:temporal ?t .
        ?t dcat:startDate ?stdate .
        ?t dcat:endDate ?edate .
        FILTER("isostart"^^xsd:dateTime <= ?stdate && "isoend"^^xsd:dateTime >= ?edate)
    }
    """.replace(
        "isostart", start_time.isoformat()
    ).replace(
        "isoend", end_time.isoformat()
    )
    result = graph.query(query, initNs={"rdf": rdflib.RDF, "aorc": AORC, "dcat": rdflib.DCAT, "dct": rdflib.DCTERMS})
    return result

def filter_on_year(year: int)


if __name__ == "__main__":
    get_composites_time_range("mirrors/composite.ttl", datetime.datetime(1979, 2, 1), datetime.datetime(1979, 2, 1, 5))
