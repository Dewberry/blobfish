import logging

import requests
import shapely.wkt
from shapely.geometry import MultiPolygon


def upload_composite_to_ckan(
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
    location_name: str,
    location_wkt: str,
    command_list: list[str],
    mirror_dataset: str,
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
        "type": "aorc_CompositeDataset",
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
        "location_name": location_name,
        "location_wkt": location_wkt,
        "command_list": command_list,
        "mirror_datasets": mirror_dataset,
        "resources": resources,
    }
    data.update(kwargs)

    logging.info(f"Uploading aorc:CompositeDataset instance to {upload_endpoint}: {data}")

    response = requests.post(upload_endpoint, headers=headers, json=data)
    return response.status_code


def create_composite_wkt(mirror_wkts: list[str]) -> str:
    logging.info("Merging geometries from RFC regions into composite coverage area")
    polys = [shapely.wkt.loads(wkt) for wkt in mirror_wkts]
    multipoly = MultiPolygon(polys)
    return multipoly.convex_hull.wkt
