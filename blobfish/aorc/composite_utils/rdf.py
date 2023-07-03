from rdflib import Graph, DCTERMS, DCAT
import datetime
from classes.namespaces import AORC
from classes.composite import RetrievedMirror


def isolate_mirror_datasets(
    catalog_graph: Graph, start_date: datetime.datetime, end_date: datetime.datetime
) -> list[RetrievedMirror]:
    # Make sure namespaces are bound to graph
    catalog_graph.bind("aorc", AORC)
    catalog_graph.bind("dct", DCTERMS)
    catalog_graph.bind("dcat", DCAT)
    query = """
    SELECT ?mda ?sd ?ed ?url
    WHERE   {
        ?mda a aorc:MirrorDataset .
        ?mda dcat:distribution ?mdi .
        ?mda dct:temporal ?t .
        ?mdi dcat:downloadUrl ?url .
        ?t dcat:startDate ?sd .
        ?t dcat:endDate ?ed .
    }
    """
    results = catalog_graph.query(query)
    mirror_datasets = []
    for row in results:
        if str(row.sd) == start_date.isoformat() and str(row.ed) == end_date.isoformat():
            mirror_datasets.append(RetrievedMirror(row.mda, row.url))
    return mirror_datasets
