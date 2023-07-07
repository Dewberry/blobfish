""" Script to construct a mirror of NOAA AORC precipitation data on s3 """
from __future__ import annotations

import datetime

import boto3
from const import FIRST_RECORD, FTP_HOST, RFC_INFO_LIST, RFC_TAR_SHP_URL
from classes.common import BasicDescriptors
from classes.mirror import RFCFeature
from shapely import convex_hull
from urllib.parse import quote


def get_rfc_features() -> dict[str, RFCFeature]:
    rfc_feature_dict = {}
    for rfc_info in RFC_INFO_LIST:
        for shp_name, shp_geom in create_rfc_list(RFC_TAR_SHP_URL):
            # Strip RFC from name retrieved from shapefile
            shp_name_stripped = shp_name.replace("RFC", "")
            if shp_name_stripped == rfc_info.alias:
                if shp_geom.geom_type in ["Polygon", "MultiPolygon"]:
                    hull = convex_hull(shp_geom)
                    rfc_feature_dict[rfc_info.alias] = RFCFeature(rfc_info.name, hull)
                    break
                else:
                    raise TypeError(
                        f"Received RFC geometry of unexpected type; expected polygon or multipolygon, got {shp_geom.geom_type}"
                    )
    return rfc_feature_dict


def create_mirror_dataset_identifiers(
    start_date: datetime.datetime, end_date: datetime.datetime, rfc_alias: str, rfc_name: str
) -> BasicDescriptors:
    dataset_id = f"mirror_{rfc_alias}_{start_date.strftime('%Y%m')}".lower()
    dataset_name = dataset_id
    proper_rfc_name = " ".join([r.capitalize() for r in rfc_name.split()])
    start_time_formatted = start_date.strftime("%Y-%m-%d")
    end_time_formatted = end_date.strftime("%Y-%m-%d")
    dataset_title = f"{proper_rfc_name} Mirror Dataset, {start_time_formatted} to {end_time_formatted}"
    dataset_description = f"A mirror dataset of AORC precipitation data covering the river forecast center (RFC) region {proper_rfc_name}, from {start_time_formatted} to {end_time_formatted}"
    dataset_url = quote(dataset_id)
    descriptors = BasicDescriptors(dataset_title, dataset_id, dataset_name, dataset_url, dataset_description)
    return descriptors


if __name__ == "__main__":
    import os

    from dotenv import load_dotenv
    from mirror_utils.aio import stream_zips_to_s3, verify_urls
    from mirror_utils.array import check_metadata
    from mirror_utils.general import create_potential_urls, create_rfc_list, upload_mirror_to_ckan
    from mirror_utils.rdf import create_source_dataset, timedelta_to_xsd_duration
    from general_utils.cloud import create_s3_resource
    from general_utils.provenance import retrieve_meta, get_command_list
    from general_utils.ckan import create_ckan_resource
    from classes.namespaces import IANA_APP, EU

    load_dotenv()

    access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
    secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    default_region = os.environ["AWS_DEFAULT_REGION"]

    s3_resource = create_s3_resource(access_key_id, secret_access_key, default_region)

    command_list = get_command_list()
    print(f"Command list: {command_list}")

    # Retrieve docker and git details
    prov_meta = retrieve_meta()

    start_dt = datetime.datetime.strptime(FIRST_RECORD, "%Y-%m-%d")
    rfc_features_dict = get_rfc_features()
    potential_urls = create_potential_urls(rfc_features_dict.keys(), start_dt, FTP_HOST)
    verified_urls = [verified_url for verified_url in verify_urls(potential_urls)]
    for streamed_zip in stream_zips_to_s3(verified_urls, s3_resource, "tempest"):
        nc4_meta = check_metadata(s3_resource, "tempest", streamed_zip.s3_key())
        rfc_feature = rfc_features_dict[streamed_zip.rfc_alias]
        source_dataset = create_source_dataset(
            streamed_zip.additional_args["url"],
            streamed_zip.additional_args["last_modified"],
            rfc_feature.name,
            streamed_zip.rfc_alias,
            rfc_feature.geom,
            nc4_meta.start_time,
            nc4_meta.end_time,
            nc4_meta.temporal_resolution,
            nc4_meta.spatial_resolution_meters,
        )
        source_dataset_ttl = source_dataset.serialize(format="ttl")
        descriptors = create_mirror_dataset_identifiers(
            nc4_meta.start_time, nc4_meta.end_time, streamed_zip.rfc_alias, rfc_feature.name
        )
        resources = [
            create_ckan_resource(
                streamed_zip.url,
                EU.NETCDF,
                IANA_APP.zip,
                "Distribution of zipped NetCDF file containing precipitation data",
                True,
            )
        ]
        upload_mirror_to_ckan(
            os.environ["CKAN_URL"],
            os.environ["CKAN_API_KEY"],
            descriptors.dataset_id,
            descriptors.name,
            os.environ["CKAN_DATA_GROUP"],
            descriptors.title,
            descriptors.url,
            descriptors.notes,
            streamed_zip.last_modified.replace(tzinfo=None).isoformat(),
            prov_meta.remote_docker_file,
            prov_meta.remote_compose_file,
            prov_meta.docker_image,
            prov_meta.git_repo,
            prov_meta.commit_hash,
            prov_meta.docker_repo,
            prov_meta.digest_hash,
            nc4_meta.start_time.isoformat(),
            nc4_meta.end_time.isoformat(),
            str(timedelta_to_xsd_duration(nc4_meta.temporal_resolution)),
            nc4_meta.spatial_resolution_meters,
            streamed_zip.rfc_alias,
            rfc_feature.name,
            rfc_feature.geom.wkt,
            command_list,
            source_dataset_ttl,
            resources,
        )
