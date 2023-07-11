"""Geospatial, cloud, and network request functions used during transposition dataset creation"""
import datetime
import json
import logging

import requests
from shapely.geometry import MultiPolygon, Polygon, shape
from shapely import convex_hull


def stream_geojson_from_s3(s3_resource, bucket: str, key: str) -> Polygon | MultiPolygon:
    logging.info(f"Streaming geojson data from s3://{bucket}/{key}")
    response = s3_resource.meta.client.get_object(Bucket=bucket, Key=key)
    geojson_data = response["Body"].read().decode("utf-8")
    geojson = json.loads(geojson_data)
    if len(geojson["features"]) > 1:
        logging.warning(f"Multiple geometries found for {key}, taking first geometry")
    feature = geojson["features"][0]
    geometry = shape(feature["geometry"])
    if geometry.geom_type not in ("MultiPolygon", "Polygon"):
        raise TypeError(f"Expected geom type of either polygon or multipolygon, got {geometry.geom_type}")
    return convex_hull(geometry)


def get_dss_last_modification(s3_resource, bucket: str, dss_key: str) -> datetime.datetime:
    obj = s3_resource.Object(bucket, dss_key)
    obj.load()
    return obj.last_modified.replace(tzinfo=None)


def upload_transposition_to_ckan(
    ckan_base_url: str,
    api_key: str,
    dataset_id: str,
    name: str,
    owner_org: str,
    title: str,
    url: str,
    notes: str,
    last_modified: str,
    docker_file: str,
    compose_file: str,
    docker_image: str,
    git_repo: str,
    commit_hash: str,
    docker_repo: str,
    digest_hash: str,
    start_time: str,
    end_time: str,
    spatial_resolution: float,
    transposition_region_name: str,
    transposition_region_wkt: str,
    watershed_region_name: str,
    watershed_region_wkt: str,
    max_precipitation_point_name: str | None,
    max_precipitation_point_wkt: str,
    image: str,
    cell_count: int,
    mean_precipitation: float,
    max_precipitation: float,
    min_precipitation: float,
    sum_precipitation: float,
    normalized_mean_precipitation: float | None,
    command_list: list[str],
    composite_normalized_datasets: dict[str, str | None],
    resources: list[dict],
    **kwargs,
) -> int:
    if not ckan_base_url.endswith("/"):
        ckan_base_url = ckan_base_url[:-1]
    upload_endpoint = f"{ckan_base_url}/api/3/action/package_create"
    headers = {"Authorization": api_key, "Content-Type": "application/json"}
    # Make sure last modified date has no timezone
    data = {
        "dataset_id": dataset_id,
        "type": "aorc_TranspositionDataset",
        "name": name,
        "owner_org": owner_org,
        "title": title,
        "private": False,
        "url": url,
        "notes": notes,
        "last_modified": last_modified,
        "docker_file": docker_file,
        "compose_file": compose_file,
        "docker_image": docker_image,
        "git_repo": git_repo,
        "commit_hash": commit_hash,
        "docker_repo": docker_repo,
        "digest_hash": digest_hash,
        "start_time": start_time,
        "end_time": end_time,
        "spatial_resolution": spatial_resolution,
        "transposition_region_name": transposition_region_name,
        "transposition_region_wkt": transposition_region_wkt,
        "watershed_region_name": watershed_region_name,
        "watershed_region_wkt": watershed_region_wkt,
        "max_precipitation_point_name": max_precipitation_point_name,
        "max_precipitation_point_wkt": max_precipitation_point_wkt,
        "image": image,
        "cell_count": cell_count,
        "mean_precipitation": mean_precipitation,
        "max_precipitation": max_precipitation,
        "min_precipitation": min_precipitation,
        "sum_precipitation": sum_precipitation,
        "normalized_mean_precipitation": normalized_mean_precipitation,
        "command_list": command_list,
        "composite_normalized_datasets": composite_normalized_datasets,
        "resources": resources,
    }
    data.update(kwargs)

    response = requests.post(upload_endpoint, headers=headers, json=data)
    return response.status_code
