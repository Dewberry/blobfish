import pathlib
import datetime
from dataclasses import dataclass
from rdflib import DCAT, DCTERMS, PROV, Graph

from .composite import CompositeMembershipMetadata
from ..pyrdf import AORC
from ..utils.cloud_utils import get_mirrored_content


@dataclass
class CompletedCompositeMetadata(CompositeMembershipMetadata):
    composite_last_modified: datetime.datetime
    composite_s3_path: str

    def from_serialized(self, serialized_dict: dict):
        pass


def create_graph(ttl_directory: str) -> Graph:
    g = Graph()
    g.bind("dcat", DCAT)
    g.bind("dct", DCTERMS)
    g.bind("prov", PROV)
    g.bind("aorc", AORC)
    for filepath in pathlib.Path(ttl_directory).glob("*.ttl"):
        g.parse(filepath)
    return g


def main(ttl_directory: str, bucket: str, prefix: str) -> None:
    g = create_graph(ttl_directory)
    for obj in get_mirrored_content(bucket, prefix):
        print(obj)


if __name__ == "__main__":
    main("mirrors", "tempest", "test/transforms")
