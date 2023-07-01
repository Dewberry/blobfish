""" Script to construct a mirror of NOAA AORC precipitation data on s3 """
from __future__ import annotations

import datetime
from dataclasses import asdict, dataclass

import boto3
from const import FIRST_RECORD, FTP_HOST, RFC_INFO_LIST, RFC_TAR_SHP_URL
from shapely.geometry import MultiPolygon, Polygon


@dataclass
class RFCFeature:
    name: str
    geom: Polygon | MultiPolygon


def get_rfc_features() -> dict[str, RFCFeature]:
    rfc_feature_dict = {}
    for rfc_info in RFC_INFO_LIST:
        print(f"Matching {rfc_info} to geometry from shapefile")
        for shp_name, shp_geom in create_rfc_list(RFC_TAR_SHP_URL):
            # Strip RFC from name retrieved from shapefile
            shp_name_stripped = shp_name.replace("RFC", "")
            if shp_name_stripped == rfc_info.alias:
                if shp_geom.geom_type in ["Polygon", "MultiPolygon"]:
                    rfc_feature_dict[rfc_info.alias] = RFCFeature(rfc_info.name, shp_geom)
                    break
                else:
                    raise TypeError(
                        f"Received RFC geometry of unexpected type; expected polygon or multipolygon, got {shp_geom.geom_type}"
                    )
    return rfc_feature_dict


def create_s3_resource(access_key_id: str, secret_access_key: str, region_name: str):
    session = boto3.Session(access_key_id, secret_access_key, region_name=region_name)
    resource = session.resource("s3")
    return resource


if __name__ == "__main__":
    import os

    from dotenv import load_dotenv
    from mirror_utils.aio import stream_zips_to_s3, verify_urls
    from mirror_utils.array import check_metadata
    from mirror_utils.general import create_potential_urls, create_rfc_list
    from mirror_utils.rdf import create_source_dataset_jsonld

    load_dotenv()

    access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
    secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    default_region = os.environ["AWS_DEFAULT_REGION"]

    s3_resource = create_s3_resource(access_key_id, secret_access_key, default_region)

    start_dt = datetime.datetime.strptime(FIRST_RECORD, "%Y-%m-%d")
    rfc_features_dict = get_rfc_features()
    potential_urls = create_potential_urls(rfc_features_dict.keys(), start_dt, FTP_HOST)
    verified_urls = [verified_url for verified_url in verify_urls(potential_urls)][:2]
    for streamed_zip in stream_zips_to_s3(verified_urls, s3_resource, "tempest"):
        nc4_meta = check_metadata(s3_resource, "tempest", streamed_zip.s3_key())
        rfc_feature = rfc_features_dict[streamed_zip.rfc_alias]
        source_dataset_json_ld = create_source_dataset_jsonld(
            streamed_zip.url,
            streamed_zip.last_modified,
            rfc_feature.name,
            streamed_zip.rfc_alias,
            rfc_feature.geom,
            nc4_meta.start_time,
            nc4_meta.end_time,
            nc4_meta.temporal_resolution,
            nc4_meta.spatial_resolution_meters,
        )
        print(source_dataset_json_ld)
