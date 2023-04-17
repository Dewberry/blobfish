""" Script to establish links between data documented in RASCAT ontology created to document storm modeling """
import rdflib
import datetime
from rdflib import DCAT, DCTERMS


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


def query_dss(jsonld_url: str, filter_start_dt: datetime.datetime, filter_end_dt: datetime.datetime):
    g = rdflib.Graph()
    g.parse(jsonld_url, format="json-ld")
    results = g.query("""
    SELECT ?url ?t
    WHERE {
        ?ds dcat:distribution ?dist .
        ?dist dcat:downloadURL ?url .
        ?ds dc:temporal ?t .
    }
    """, initNs={"dcat": DCAT, "dc": DCTERMS})
    for result in results:
        # Get url
        url = result[0]
        # Parse start date from temporal property
        dt_string, _ = str(result[1]).split("/P")
        reference_date = datetime.datetime.fromisoformat(dt_string)
        if filter_start_dt <= reference_date <= filter_end_dt:
            yield url, reference_date

def main():
    event_dts = query_events("https://raw.githubusercontent.com/arc-pts/ffrd-metadata/main/kanawha/kanawha.ttl")
    if event_dts:
        for res in query_dss("https://ckan.dewberryanalytics.com/dataset/b71d2803-2f4b-4f9f-97ee-f1de6db42f54/resource/9f105ba0-ba67-485c-b495-ad2ed4f1b3ad/download/2016.jsonld", *event_dts):
            print(res)

if __name__ == "__main__":
    main()