import boto3
from dataclasses import dataclass
from datetime import datetime
import io
from pandas import date_range
import pathlib as pl
import requests
from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, XSD, OWL
import sys
import time
from typing import List, Iterator
import warnings

warnings.filterwarnings("ignore")
import xarray as xr
import zarr
from zipfile import ZipFile

from ..pyrdf._AORC import AORC
from ..utils.blobstore import list_s3_keys, s3_key_exists

from .const import (
    MIRROR_ROOT,
    COMPOSITE_CATALOG,
    COMPOSITE_ROOT,
)


class AORCMirror(AORC):
    """
    Analysis of Record for Calibration (AORC)

    Utilities for creating the CompositeGrid and writing to an S3 mirror

    Parameters:
    ftp_db_source: source .ttl listing available SourceDataset's from the ftp
    """

    def __init__(
        self,
        session: boto3.Session,
        dtype: str = "precipitation",
        ftp_db_source: str = "mirrors/AORC-ftp-db.ttl",
        ontology_src: str = "semantics/rdf/aorc.ttl",
        bucket_name="tempest",
    ):
        # Mirror Resources (AWS)
        self._ontology_src = ontology_src
        self.ontology = Graph().parse(self._ontology_src, format="ttl")
        self.graph = Graph().parse(ftp_db_source, format="ttl")
        self.s3_client = session.client("s3")
        self.s3_resource = session.resource("s3")
        self.bucket_name = bucket_name

    def data_source_to_mirror(self, source_dataset: URIRef, override: str = False):
        """
        Copy SourceData files in a given list (source_datasets) from the ftp to an s3 mirror
        TODO: add override
        """

        source_uri = self.graph.value(source_dataset, AORC.hasSourceDatasetURI)
        mirror_uri = self.graph.value(source_dataset, AORC.hasMirrorDatasetURI)

        s3_prefix = mirror_uri.replace(f"s3://{self.bucket_name}/", "")
        if s3_key_exists(s3_prefix) and not override:
            print(f"s3mirror copy exists for: {s3_prefix}")
        else:
            print(f"no s3mirror for: {s3_prefix}")

            r = requests.get(str(source_uri), stream=True, verify=False)
            bucket = self.s3_resource.Bucket(self.bucket_name)
            bucket.upload_fileobj(
                r.raw,
                s3_prefix,
                ExtraArgs={"Metadata": {"source": source_dataset, "ontology": self.ontology._NS}},
            )
            return self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_prefix)

    def ndata_sources_to_mirror(self, source_datasets: List[str], override: str = False):
        """
        Copy SourceData files in a given list (source_datasets) from the ftp to an s3 mirror
        TODO: add override
        """
        responses = []
        for source_dataset in source_datasets:
            r = self.data_source_to_mirror(source_dataset)
            responses.append(r)
        return r

    def composite_grid_path(self, dtm: datetime):
        date_string = dtm.strftime(format="%Y%m%d%H")
        return f"s3://{self.bucket_name}/transforms/aorc/precipitation/{dtm.year}/{date_string}.zarr"

    def data_source_map(self, source_datasets: List[str], dtm: datetime):
        """
        Create map of data_sources from FTP to Mirror
        """
        date_string = dtm.strftime(format="%Y%m%d%H")
        file_map = {}
        for dataset in source_datasets:
            s3_mirror_dataset = self.graph.value(dataset, AORC.hasMirrorDatasetURI)
            rfc = self.graph.value(dataset, AORC.hasRFC)
            rfc_alias = self.graph.value(rfc, AORC.hasRFCAlias)
            dst_prefix = self.composite_grid_path(dtm)

            src_prefix = s3_mirror_dataset.replace("s3://tempest/", "")

            file_map[src_prefix] = f"""AORC_APCP_{rfc_alias}RFC_{date_string}.nc4"""

        return dst_prefix, file_map

    def add_git_info(self, script_path: str, git_repo_info: tuple):
        """
        TODO: add check if exists before adding...
        """
        repo_url, commit_hash, branch_name = git_repo_info
        repo = URIRef(repo_url)
        script = AORC._NS[script_path]
        hash = AORC._NS[commit_hash]
        self.graph.add((script, RDF.type, AORC.Script))
        self.graph.add((script, OWL.Annotation, Literal(f"current branch {branch_name}")))
        self.graph.add((repo, RDF.type, AORC.CodeRepository))
        self.graph.add((script, AORC.hasCodeRepositroy, repo))
        self.graph.add((hash, RDF.type, AORC.CommitHash))
        self.graph.add((script, AORC.hasCommitHash, hash))

    def composite_grid_to_graph(self, dtm: datetime, script_path: str, git_repo_info: tuple):
        """
        update graph
        """
        dst_s3_key = self.composite_grid_path(dtm)
        composite_grid_uid = pl.Path(dst_s3_key).name
        composite_grid_uri = COMPOSITE_ROOT[composite_grid_uid]
        self.graph.add((COMPOSITE_CATALOG[composite_grid_uid], RDF.type, AORC.CompositeGrid))
        self.graph.add((COMPOSITE_CATALOG[composite_grid_uid], AORC.hasCompositeGridURI, composite_grid_uri))

        # Repository / script info
        self.add_git_info(script_path, git_repo_info)
        self.graph.add((COMPOSITE_CATALOG[composite_grid_uid], AORC.hasCreationScript, AORC._NS[script_path]))

        return composite_grid_uid

    def link_composite_grid_sources_in_graph(self, composite_grid_uid, source_datasets, dtm):
        for source_dataset in source_datasets:
            rfc = self.graph.value(source_dataset, AORC.hasRFC)
            rfc_alias = self.graph.value(rfc, AORC.hasRFCAlias)
            source_grid = f"""AORC_APCP_{rfc_alias}RFC_{dtm.strftime(format="%Y%m%d%H")}.nc4"""
            # self.graph.add((MIRROR_ROOT[source_grid], RDF.type, AORC.SourceGrid))
            self.graph.add((source_dataset, AORC.hasSourceGrid, MIRROR_ROOT[source_grid]))
            self.graph.add((COMPOSITE_CATALOG[composite_grid_uid], AORC.hasSourceGrid, MIRROR_ROOT[source_grid]))

    def create_composite_grid(self, file_map) -> xr.Dataset:
        """
        Opens files for a given hour and merges to return a single xarray dataset
        for all regions in the AORC dataset
        """
        xdatasets = []
        for s3_zip, netcdf_file in file_map.items():
            if not self.s3_key_exists(s3_zip):
                raise (KeyError(f"{s3_zip} does not exist in bucket {self.bucket_name}"))
            data = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=s3_zip,
            )
            bytes = data["Body"].read()

            with ZipFile(io.BytesIO(bytes)) as zip_file:
                ds = xr.open_dataset(zip_file.open(netcdf_file), chunks="auto")
                ds.rio.write_crs(4326, inplace=True)
            print(f"appending dataset {netcdf_file}")
            xdatasets.append(ds)

        print(f"returning xdatasets: {xdatasets}")
        return xr.merge(xdatasets, compat="no_conflicts", combine_attrs="drop_conflicts")

    def composite_grid_to_mirror(self, xdata: xr.Dataset, s3_zarr_file: str):
        """
        Write zarr file to S3
        """
        print(f"Copying {s3_zarr_file} fo mirror")
        store = zarr.storage.FSStore(s3_zarr_file)
        return xdata.to_zarr(store, mode="w")
