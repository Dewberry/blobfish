""" Utilities used in creating RDF metadata for s3 mirror creation """
# Make sure script can access common classes
import sys

sys.path.append("../classes")

from classes.common import AORCDataURL
import rdflib

def skolemize_source_dataset(source_dataset: AORCDataURL) -> str:
    skolemized_node_id = f"SourceDataset_{source_dataset.rfc_alias.upper()}_{source_dataset.data_start_dt.strftime('%Y%m')}"
    return skolemized_node_id


def create_source_dataset(source_dataset: AORCDataURL) -> rdflib.IdentifiedNode:
    return rdflib.BNode()


def create_mirror_dataset(mirror_dataset: AORCDataURL) -> rdflib.IdentifiedNode:
    return rdflib.URIRef()

