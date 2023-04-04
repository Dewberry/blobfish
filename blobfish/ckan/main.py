from rdflib import URIRef, DCAT
from rdflib.query import Result

from .load import RDFHandler
from .query import SpatialRDFHandler, identify_rfc_alias
from ..pyrdf import AORC


def get_mirror_uris(rfc_alias: str, rdf_handler: RDFHandler) -> Result:
    rfc_uri = URIRef(f"https://www.weather.gov/{rfc_alias.lower()}rfc")
    query = """
    SELECT ?s3
    WHERE {
        ?rfc ^aorc:hasRFC ?p .
        ?p dcat:dataset ?sda .
        ?sda ^aorc:hasSourceDataset ?s3 .
    }
    """
    namespaces = {"aorc": AORC, "dcat": DCAT}
    variables = {"rfc": rfc_uri}
    res = rdf_handler.query(query, namespaces, variables)
    return res

if __name__ == "__main__":
    import argparse

    # Set up command line argument parsing
    parser = argparse.ArgumentParser(
        prog="Mirror Dataset Identifier",
        description="Takes longitude and latitude point and returns the AORC s3 mirror datasets which have a spatial coverage which includes the given point",
        )

    parser.add_argument('-x', '--longitude', help="x coordinate for point of interest", required=True, type=float)
    parser.add_argument('-y', '--latitude', help="y coordinate for point of interest", required=True, type=float)
    parser.add_argument('-f', '--file', help="Path to ttl file to load as rdflib graph in place of CKAN rdf data", required=False, type=str)
    parser.add_argument('-m', '--method', help="If plain, use method of retrieving RFC offices which does not invoke GeoSPARQL functions. If geo, use method which invokes GeoSPARQL functions", choices=["plain", "geo"], default="plain", required=False, type=str)

    args = parser.parse_args()

    if args.method == "plain":
        if args.file:
            handler = RDFHandler(init_ttl=args.file)
        else:
            handler = RDFHandler()
            handler.load_graph()
        rfc_alias = identify_rfc_alias(args.longitude, args.latitude)
        for s3_uri in get_mirror_uris(rfc_alias, handler):
            print(s3_uri)
    else:
        if args.file:
            handler = SpatialRDFHandler(init_ttl=args.file)
        else:
            handler = SpatialRDFHandler()
            handler.load_graph()
        handler.add_spatial_coverages()
        for s3_uri in handler.identify_rfc_datasets(args.longitude, args.latitude):
            print(s3_uri)
