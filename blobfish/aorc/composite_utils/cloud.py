from classes.composite import RetrievedMirror
from rdflib import URIRef


def acquire_mirror_netcdfs(target_dir: str, mirror_metadata: list[RetrievedMirror]) -> tuple[URIRef, list[str]]:
    # Downloads and unzips zipped netCDF datasets, returns tuple of mirror dataset uri and paths of nc files extracted
    pass
