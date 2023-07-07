import datetime

from classes.namespaces import AORC
from general_utils.ckan import query_ckan_catalog
from rdflib import DCAT, DCTERMS, XSD, Literal, URIRef


def retrieve_composite_dataset_uris(
    ckan_base_url: str, start: datetime.datetime, end: datetime.datetime
) -> list[URIRef]:
    if not ckan_base_url.endswith("/"):
        ckan_base_url += "/"
    mirror_catalog_url = ckan_base_url + "aorc_CompositeDataset/catalog.ttl"
    catalog_graph = query_ckan_catalog(mirror_catalog_url)
    query_string = """
        SELECT  ?cda
        WHERE   {
            ?cda a aorc:CompositeDataset .
            ?cda dct:temporal ?t .
            ?t dcat:startDate ?sd .
            ?t dcat:endDate ?ed .
        }
        FILTER(?start <= ?sd && ?ed <= ?end)
    """
    start_date_literal = Literal(start.isoformat(), datatype=XSD.dateTime)
    end_date_literal = Literal(end.isoformat(), datatype=XSD.dateTime)
    results = catalog_graph.query(
        query_string,
        initNs={"aorc": AORC, "dcat": DCAT, "dct": DCTERMS},
        initBindings={"start": start_date_literal, "end": end_date_literal},
    )
    composite_uris = [result_row.cda for result_row in results]
    return composite_uris
