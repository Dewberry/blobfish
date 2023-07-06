from typing import Iterator

from classes.composite import RetrievedMirror
from classes.namespaces import AORC, LOCN
from general_utils.ckan import query_ckan_catalog
from rdflib import DCAT, DCTERMS, Graph, Literal
from rdflib.namespace._GEO import GEO


def retrieve_mirror_dataset_metadata(ckan_base_url: str, rfc_count: int) -> Iterator[list[RetrievedMirror]]:
    if not ckan_base_url.endswith("/"):
        ckan_base_url += "/"
    mirror_catalog_url = ckan_base_url + "aorc_MirrorDataset/catalog.ttl"
    catalog_graph = query_ckan_catalog(mirror_catalog_url)
    for start_date_literal, end_date_literal in verify_date_rfc_count(catalog_graph, rfc_count):
        query_string = """
            SELECT  ?mda ?url ?res ?wkt
            WHERE   {
                ?mda a aorc:MirrorDataset .
                ?mda dcat:distribution/dcat:downloadURL ?url .
                ?mda dct:temporal ?t .
                ?t dcat:startDate ?sd .
                ?t dcat:endDate ?ed .
                ?mda dcat:spatialResolutionInMeters ?res .
                ?mda aorc:hasRFC/locn:geometry/geo:asWKT ?wkt .
            }
        """
        results = catalog_graph.query(
            query_string,
            initNs={"aorc": AORC, "dcat": DCAT, "geo": GEO, "locn": LOCN, "dct": DCTERMS},
            initBindings={"sd": start_date_literal, "ed": end_date_literal},
        )
        mirror_meta_list = [
            RetrievedMirror(
                result_row.mda, result_row.url, start_date_literal, end_date_literal, result_row.res, result_row.wkt
            )
            for result_row in results
        ]
        yield mirror_meta_list


def verify_date_rfc_count(catalog_graph: Graph, rfc_count: int) -> Iterator[tuple[Literal, Literal]]:
    query_string = """
        SELECT ?sd ?ed
                    (COUNT(?s) AS ?date_count)
        WHERE   {
            ?s a aorc:MirrorDataset .
            ?s dct:temporal ?t .
            ?t dcat:startDate ?sd .
            ?t dcat:endDate ?ed .
        }
        GROUP BY ?sd ?ed
    """
    for result_row in catalog_graph.query(query_string, initNs={"dcat": DCAT, "aorc": AORC, "dct": DCTERMS}):
        if int(result_row.date_count) != rfc_count:
            raise ValueError(
                f"Expected start date {result_row.sd} and end date {result_row.ed} to have number of matches equal to number of RFCs ({rfc_count}); Instead got {result_row.date_count}"
            )
            # pass
        else:
            yield result_row.sd, result_row.ed
