""" Script to establish links between data documented in RASCAT ontology created to document storm modeling """
import rdflib
import datetime
from rdflib import DCAT, DCTERMS
import enum
from collections.abc import Generator
from .pyrdf._AORC import AORC


class TranspositionStatistic(enum.Enum):
    MAX = enum.auto()
    MEAN = enum.auto()
    MIN = enum.auto()
    SUM = enum.auto()
    NORM_MEAN = enum.auto()


def query_events(rdf_url: str) -> tuple[datetime.datetime, datetime.datetime] | None:
    g = rdflib.Graph()
    g.parse(rdf_url, format="ttl")
    results = g.query("""
    SELECT ?start ?end
    WHERE {
        ?s a rascat:HydroEvent .
        ?s rascat:startDateTime ?start .
        ?s rascat:endDateTime ?end
    }
    ORDER BY DESC(?start)
    LIMIT 1
    """, initNs={"rascat": "http://raw.githubusercontent.com/arc-pts/ffrd-metadata/main/rascat.ttl#"})
    for result in results:
        start_dt = datetime.datetime.fromisoformat(str(result[0]))
        end_dt = datetime.datetime.fromisoformat(str(result[1]))
        return start_dt, end_dt


def get_jsonld_graph(jsonld_url: str) -> rdflib.Graph:
    g = rdflib.Graph()
    g.parse(jsonld_url, format="json-ld")
    return g

def filter_dss_date(g: rdflib.Graph, stat: TranspositionStatistic, filter_start_dt: datetime.datetime, filter_end_dt: datetime.datetime) -> Generator[tuple[rdflib.URIRef, rdflib.Literal], None, None]:
    if stat == TranspositionStatistic.MAX:
        stat_term = AORC.maximumPrecipitation
    elif stat == TranspositionStatistic.MEAN:
        stat_term = AORC.meanPrecipitation
    elif stat == TranspositionStatistic.MIN:
        stat_term = AORC.minimumPrecipitation
    elif stat == TranspositionStatistic.NORM_MEAN:
        stat_term = AORC.normalizedMeanPrecipitation
    results = g.query("""
    SELECT ?url ?t ?val
    WHERE {
        ?ds dcat:distribution ?dist .
        ?dist dcat:downloadURL ?url .
        ?ds dc:temporal ?t .
        ?ds aorc:transpositionStatistics ?stats .
        ?stats ?stat_type ?val
    }
    ORDER BY DESC(?val)
    """, initNs={"dcat": DCAT, "dc": DCTERMS, "aorc": AORC}, initBindings={"stat_type": stat_term})
    for result in results:
        # Get url
        url = result[0]
        # Parse start date from temporal property
        dt_string, _ = str(result[1]).split("/P")
        reference_date = datetime.datetime.fromisoformat(dt_string)
        val = result[2]
        if filter_start_dt <= reference_date <= filter_end_dt:
            yield url, val

def main():
    event_dts = query_events("https://raw.githubusercontent.com/arc-pts/ffrd-metadata/main/kanawha/kanawha.ttl")
    if event_dts:
        json_ld_graph = get_jsonld_graph("https://ckan.dewberryanalytics.com/dataset/9b08c661-3b9c-441f-a4a0-6e1a9c865475/resource/16212ad7-32f9-49c4-b78e-d136bc1425d5/download/2016.jsonld")
        for res in filter_dss_date(json_ld_graph, TranspositionStatistic.NORM_MEAN, *event_dts):
            print(res)

if __name__ == "__main__":
    main()