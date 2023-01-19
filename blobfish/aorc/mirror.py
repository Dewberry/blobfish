import boto3
from dataclasses import dataclass
from datetime import datetime
import io
from pandas import date_range
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
    FIRST_RECORD,
    FTP_ROOT,
    MIRROR_ROOT,
    RFC_INFO_LIST,
    SOURCE_CATALOG,
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
        # self.ontology = Graph().parse(self._ontology_src, format="ttl")
        self.graph = Graph().parse(ftp_db_source, format="ttl")
        self.s3_client = session.client("s3")
        self.s3_resource = session.resource("s3")
        self.bucket_name = bucket_name

    def copy_to_mirror(self, source_datasets: List[str], override: str = False):
        """
        Copy SourceData files in a given list (source_datasets) from the ftp to an s3 mirror
        TODO: add override
        """

        for dataset in source_datasets:

            source_uri = str(self.graph.value(dataset, AORC.hasSourceDatasetURI))
            mirror_uri = self.graph.value(dataset, AORC.hasMirrorDatasetURI)
            prefix = mirror_uri.replace(f"s3://{self.bucket_name}/", "")
            if s3_key_exists(prefix) and not override:
                print(f"s3mirror copy exists for: {prefix}")
                continue
            else:
                print(f"no s3mirror for: {prefix}")

                r = requests.get(source_uri, stream=True, verify=False)
                bucket = self.s3_resource.Bucket(self.bucket_name)
                bucket.upload_fileobj(
                    r.raw,
                    prefix,
                    ExtraArgs={"Metadata": {"source": dataset, "ontology": self.ontology._NS}},
                )
                print(r.text)
                time.sleep(1)
                # add graph upate here ?
            # return self.s3_client.head_object(Bucket=self.bucket_name, Key=prefix)

    def create_xrdataset_for_one_hour(self, file_map) -> xr.Dataset:
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

    def write_zarrfile_to_mirror(self, xdata: xr.Dataset, s3_zarr_file: str):
        """
        Write zarr file to S3
        """
        print(f"Copying {s3_zarr_file} fo mirror")
        store = zarr.storage.FSStore(s3_zarr_file)
        return xdata.to_zarr(store, mode="w")


# def copy_sources_to_mirror(self, source_datasets: List[str], dtm: datetime):
#     """
#     Copy multiple files from source to mirror
#     """
#     file_map = {}
#     for dataset in source_datasets:
#         s3_dataset_copy = self.graph.value(dataset, AORC.hasMirrorDatasetURI)
#         rfc = self.graph.value(dataset, AORC.hasRFC)
#         rfc_alias = self.graph.value(rfc, AORC.hasRFCAlias)
#         date_string = dtm.strftime(format="%Y%m%d%H")
#         dst_prefix = f"s3://{self.bucket_name}/transforms/aorc/precipitation/{dtm.year}/{date_string}.zarr"
#         src_prefix = s3_dataset_copy.replace("s3://tempest/", "")
#         file_map[src_prefix] = f"""AORC_APCP_{rfc_alias}RFC_{date_string}.nc4"""
#     return dst_prefix, file_map


# def add_mirror_sources_to_graph(self, source_datasets: List[URIRef], dtm: datetime):
#     """
#     Copy file from source to mirror
#     """
#     # file_map = {}
#     date_string = dtm.strftime(format="%Y%m%d%H")
#     composite_grid_name = f"{dtm.year}/{date_string}.zarr"
#     dst_s3_key = f"s3://{self.bucket_name}/transforms/aorc/{composite_grid_name}"

#     composite_grid_uri = COMPOSITE_ROOT[composite_grid_name]
#     self.graph.add((composite_grid_uri, RDF.type, AORC.CompositeGrid))
#     self.graph.add((composite_grid_uri, AORC.hasCompositeGridURI, URIRef(dst_s3_key)))

#     NS = AORC._NS
#     for dataset in source_datasets:
#         # s3_dataset_copy = self.graph.value(dataset, AORC.hasMirrorDatasetURI)
#         # print(f"dataset {dataset}")
#         rfc = self.graph.value(dataset, AORC.hasRFC)
#         rfc_alias = self.graph.value(rfc, AORC.hasRFCAlias)
#         source_grid = f"""AORC_APCP_{rfc_alias}RFC_{date_string}.nc4"""
#         self.graph.add((NS[source_grid], RDF.type, AORC.SourceGrid))
#         self.graph.add((NS[source_grid], AORC.isSourceGridOf, dataset))
#         self.graph.add((MIRROR_ROOT[cg_name], AORC.hasSourceGrid, NS[source_grid]))
#         # print(dataset, self.graph.value(dataset, AORC.hasSourceGrid))
#         # src_prefix = s3_dataset_copy.replace("s3://tempest/", "")
#         # file_map[src_prefix] = f"""AORC_APCP_{rfc_alias}RFC_{date_string}.nc4"""
#     return dst_s3_key  # , file_map
