""" Synchronous operations for creating AORC data mirror on s3"""
# Make sure script can access common classes
import sys

sys.path.append("../classes")

import datetime
import io
import os
import tarfile
from collections.abc import Iterator
from tempfile import TemporaryDirectory

import fiona
import requests
from classes.common import AORCDataURL, RFCInfo
from shapely.geometry import (GeometryCollection, LinearRing, LineString,
                              MultiLineString, MultiPoint, MultiPolygon, Point,
                              Polygon, shape)


def create_rfc_list(rfc_tar_shp_url: str) -> list[tuple[str, Point | MultiPoint | LineString | MultiLineString | Polygon | MultiPolygon | LinearRing | GeometryCollection]]:
    with requests.get(rfc_tar_shp_url, stream=True) as resp:
        if resp.ok:
            with TemporaryDirectory() as tmpdir:
                with io.BytesIO(resp.content) as stream:
                    with tarfile.open(fileobj=stream, mode="r:gz") as tar:
                        tar.extractall(tmpdir)
                        for member in tar.getmembers():
                            if member.name.endswith(".shp"):
                                shp_path = os.path.join(tmpdir, member.name)
                                with fiona.open(shp_path, driver="ESRI Shapefile") as f:
                                    rfc_features = [(feature["properties"]["NAME"], shape(feature["geometry"])) for feature in f]
                                    return rfc_features
                        else:
                            raise FileNotFoundError("No shapefile found in tarfile")
        else:
            raise requests.exceptions.ConnectionError(f"Request response status indicates faillure: {resp.status_code}")


def create_potential_urls(rfcs: list[RFCInfo], start_dt: datetime.datetime, base_url: str) -> Iterator[AORCDataURL]:
    end_dt = datetime.datetime.now()
    end_dt = datetime.datetime(end_dt.year, end_dt.month, 1)
    start_dt = datetime.datetime(start_dt.year, start_dt.month, 1)
    current_dt = start_dt
    while current_dt < end_dt:
        for rfc in rfcs:
            potential_url = f"{base_url}/AORC_{rfc.alias}RFC_4km/{rfc.alias}RFC_precip_partition/AORC_APCP_4KM_{rfc.alias}RFC_{current_dt.strftime('%Y%m')}.zip"
            yield AORCDataURL(potential_url, current_dt, rfc.alias)
        if current_dt.month < 12:
            current_dt = datetime.datetime(current_dt.year, current_dt.month + 1, 1)
        else:
            current_dt = datetime.datetime(current_dt.year + 1, 1, 1)


def check_zipped_nc_meta(zip_fn: str) -> dict:
    return {}
