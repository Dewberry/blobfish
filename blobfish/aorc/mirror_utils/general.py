""" Synchronous operations for creating AORC data mirror on s3"""
from __future__ import annotations

import sys
from urllib.parse import quote

# Make sure script can access common classes


sys.path.append("../classes")

import datetime
import io
import os
import tarfile
from collections.abc import Iterator
from tempfile import TemporaryDirectory

import fiona
import requests
from classes.common import AORCDataURL
from shapely.geometry import (
    GeometryCollection,
    LinearRing,
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
    shape,
)


def create_rfc_list(
    rfc_tar_shp_url: str,
) -> list[
    tuple[
        str,
        Point | MultiPoint | LineString | MultiLineString | Polygon | MultiPolygon | LinearRing | GeometryCollection,
    ]
]:
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
                                    rfc_features = [
                                        (feature["properties"]["NAME"], shape(feature["geometry"])) for feature in f
                                    ]
                                    return rfc_features
                        else:
                            raise FileNotFoundError("No shapefile found in tarfile")
        else:
            raise requests.exceptions.ConnectionError(f"Request response status indicates faillure: {resp.status_code}")


def create_potential_urls(
    rfc_alias_list: list[str], start_dt: datetime.datetime, base_url: str
) -> Iterator[AORCDataURL]:
    end_dt = datetime.datetime.now()
    end_dt = datetime.datetime(end_dt.year, end_dt.month, 1)
    start_dt = datetime.datetime(start_dt.year, start_dt.month, 1)
    current_dt = start_dt
    while current_dt < end_dt:
        for rfc_alias in rfc_alias_list:
            potential_url = f"{base_url}/AORC_{rfc_alias}RFC_4km/{rfc_alias}RFC_precip_partition/AORC_APCP_4KM_{rfc_alias}RFC_{current_dt.strftime('%Y%m')}.zip"
            yield AORCDataURL(potential_url, rfc_alias)
        if current_dt.month < 12:
            current_dt = datetime.datetime(current_dt.year, current_dt.month + 1, 1)
        else:
            current_dt = datetime.datetime(current_dt.year + 1, 1, 1)


def upload_mirror_to_ckan(
    ckan_base_url: str,
    api_key: str,
    owner_org: str,
    dataset_id: str,
    title: str,
    last_modified: datetime.datetime,
    docker_file: str,
    compose_file: str,
    docker_image: str,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    temporal_resolution: str,
    spatial_resolution: float,
    rfc_alias: str,
    rfc_full_name: str,
    rfc_wkt: str,
    command_list: list[str],
    source_dataset: dict | list,
    **kwargs,
) -> int:
    if not ckan_base_url.endswith("/"):
        ckan_base_url = ckan_base_url[:-1]
    upload_endpoint = f"{ckan_base_url}/api/3/action/package_create"
    headers = {"Authorization": api_key, "Content-Type": "application/json"}
    docker_repo, digest_hash = parse_image(docker_image)
    git_repo, commit_hash = parse_file(compose_file)
    data = {
        "name": dataset_id,
        "owner_org": owner_org,
        "title": title,
        "private": False,
        "url": quote(dataset_id),
        "last_modified": last_modified,
        "docker_file": docker_file,
        "compose_file": compose_file,
        "docker_image": docker_image,
        "git_repo": git_repo,
        "commit_hash": commit_hash,
        "docker_repo": docker_repo,
        "digest_hash": digest_hash,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "temporal_resolution": temporal_resolution,
        "spatial_resolution": spatial_resolution,
        "rfc_alias": rfc_alias,
        "rfc_full_name": rfc_full_name,
        "rfc_wkt": rfc_wkt,
        "command_list": command_list,
        "source_dataset": source_dataset,
    }
    data.update(kwargs)
    response = requests.post(upload_endpoint, headers=headers, json=data)
    return response.status_code


def parse_image(docker_image: str) -> tuple[str, str]:
    """Function to parse docker image digest hash and base repository from docker image name

    Args:
        docker_image (str): Docker image name

    Returns:
        tuple[str, str]: Docker repo and digest hash, respectively
    """
    pass


def parse_file(compose_file: str) -> tuple[str, str]:
    """Function to parse git commit hash and base repository from path to compose file on remote

    Args:
        compose_file (str): Compose file remote path

    Returns:
        tuple[str, str]: Git repo and commit hash, respectively
    """
    pass
