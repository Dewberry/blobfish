from datetime import datetime
from pandas import date_range
from rdflib import Graph, Literal
from rdflib.namespace import RDF, XSD, OWL
import sys

from .const import (
    FIRST_RECORD,
    FTP_ROOT,
    MIRROR_ROOT,
    RFC_INFO_LIST,
    SOURCE_CATALOG,
)
from ..pyrdf._AORC import AORC


def add_rfc_individuals(g: Graph) -> Graph:
    """
    Add named individuals to the aorc ontology to facilitate
    development of the data pipeline for creating CompositeGrids
    """
    for rfc_info in RFC_INFO_LIST:
        # example {rfc_info.alias}{self.RFC.fragment} == "ABRFC"
        subj = AORC._NS[f"{rfc_info.alias}{AORC.RFC.fragment}"]
        g.add((subj, RDF.type, AORC.RFC))
        g.add((subj, AORC.hasRFCAlias, Literal(rfc_info.alias)))
        g.add((subj, AORC.hasRFCName, Literal(rfc_info.name)))
    return g


class AORCSource(AORC):
    """
    Analysis of Record for Calibration (AORC)

    Utilities for developing an RDF dataset using the _AORC ontology in data pipelines
    for the storm-sniffer repository.
    """

    def __init__(
        self,
        dtype: str = "precipitation",
        ontology_src: str = "../blobfish/semantics/rdf/aorc.ttl",
    ):
        # Ontology Graph
        self._ontology_src = ontology_src
        self.ontology = Graph().parse(self._ontology_src, format="ttl")

        # Ontology Graph with named individuals
        self.graph = add_rfc_individuals(self.ontology)

        self.__repr__ = "AORCDB"

    def ftp_subdir(self, rfc_alias: str, dtype: str = "precipitation") -> str:
        """
        Specifies the dir on the NOAA server (FTP_ROOT) for Precipitation vs Temperature vs other datasets
        """
        if dtype == "precipitation":
            return f"AORC_{rfc_alias}RFC_4km/{rfc_alias}RFC_precip_partition/"
        elif dtype == "temperature":
            pass
        else:
            raise TypeError(f"unrecognized dataset `{dtype}`, dtype must be one of `['precipitation', 'temperature']`")

    def dtm_to_year_month(self, dtm: datetime) -> str:
        """
        Returns formatted string used in creating filepaths
        """
        return f"{dtm.year:02}{dtm.month:02}"

    def source_data(self, rfc_alias: str, dtm: datetime, dtype: str = "precipitation") -> str:
        """
        Generate the uid used as the named individual of the DataSource class
        """
        if dtype == "precipitation":
            return f"p{self.dtm_to_year_month(dtm)}{rfc_alias}"
        else:
            raise ValueError(f"unknown dtype `{dtype}`, expected `precipitation`")

    def source_data_filename(self, rfc_alias: str, dtm: datetime) -> str:
        """
        Generate DataSource filename
        """
        return f"AORC_APCP_4KM_{rfc_alias}RFC_{self.dtm_to_year_month(dtm)}.zip"

    def source_dataset_uri(self, rfc_alias: str, dtm: datetime) -> str:
        """
        Generate Datasource URI
        """
        return f"{self.ftp_subdir(rfc_alias)}{self.source_data_filename(rfc_alias, dtm)}"

    def mirror_dataset_uri(self, rfc_alias: str, dtm: datetime) -> str:
        """
        Generate Mirror URI
        """
        return f"{self.ftp_subdir(rfc_alias)}{self.source_data_filename(rfc_alias, dtm)}"


def create_source_data_catalog(creation_date: str, dtype: str = "precipitation"):
    """
    Create database to store source data information
    Step 1: Instantiate classes
    Step 2: Add properties....

    Attributes:

    creation_date: str (example: "%Y-%m-%d")
    """
    a = AORCSource()

    # Create a new graph and add namespace mappings
    # g = a.graph
    g = Graph()
    g = add_rfc_individuals(g)

    # Iterate over the RFC's and add each dataset (assumed to be on the ftp!) to the catalog file
    for rfc, _, _ in a.graph.triples((None, RDF.type, AORC.RFC)):
        rfc_alias = a.graph.value(rfc, AORC.hasRFCAlias)

        # Iterate over the monthly data_source files expected on the ftp
        for dtm in date_range(start=FIRST_RECORD, end=creation_date, freq="M"):

            # Create URI's for each class individual (assiging unique record for datasource)
            source_dataset = SOURCE_CATALOG[a.source_data(rfc_alias, dtm)]
            source_uri = FTP_ROOT[a.source_dataset_uri(rfc_alias, dtm)]
            mirror_uri = MIRROR_ROOT[a.mirror_dataset_uri(rfc_alias, dtm)]

            # Add individuals
            g.add((source_dataset, RDF.type, OWL.NamedIndividual))
            g.add((source_dataset, RDF.type, AORC.SourceDataset))

            # g.add((source_uri, RDF.type, OWL.NamedIndividual))
            # g.add((source_uri, RDF.type, AORC.SourceURI))

            # g.add((mirror_uri, RDF.type, OWL.NamedIndividual))
            # g.add((mirror_uri, RDF.type, AORC.MirrorURI))

            # Add Object Properties
            g.add((source_dataset, AORC.hasRefDate, Literal(a.dtm_to_year_month(dtm), datatype=XSD.date)))
            g.add((source_dataset, AORC.hasRFC, rfc))
            g.add((source_dataset, AORC.hasSourceDatasetURI, source_uri))
            g.add((source_dataset, AORC.hasMirrorDatasetURI, mirror_uri))
    return g


def source_catalog_to_file(g: Graph, filepath: str):
    """
    Create a local copy of the ftp_db
    TODO: add dtype for temperature
    """
    g.bind("aorc", AORC._NS)
    g.bind("aorccat", SOURCE_CATALOG)
    # g.bind("aorcdb", FTP_ROOT)
    # g.bind("aorcmirror", MIRROR_ROOT)
    with open(filepath, "w") as f:
        f.write(g.serialize(format="ttl"))
