""" Script to construct a mirror of NOAA AORC precipitation data on s3 """
import datetime
from dataclasses import dataclass

import boto3
from const import FIRST_RECORD, FTP_HOST, RFC_INFO_LIST, RFC_TAR_SHP_URL
from mirror_utils.aio import stream_zips_to_s3, verify_urls
from mirror_utils.general import create_potential_urls, create_rfc_list
from shapely.geometry import MultiPolygon, Polygon


@dataclass
class RFCFeature:
    alias: str
    name: str
    geom: Polygon | MultiPolygon

def get_rfc_features() -> list[RFCFeature]:
    rfc_feature_list = []
    for rfc_info in RFC_INFO_LIST:
        print(f"Matching {rfc_info} to geometry from shapefile")
        for shp_name, shp_geom in create_rfc_list(RFC_TAR_SHP_URL):
            # Strip RFC from name retrieved from shapefile
            shp_name_stripped = shp_name.replace("RFC", "")
            if shp_name_stripped == rfc_info.alias:
                if shp_geom.geom_type in ["Polygon", "MultiPolygon"]:
                    rfc_feature_list.append(RFCFeature(rfc_info.alias, rfc_info.name, shp_geom))
                    break
                else:
                    raise TypeError(f"Received RFC geometry of unexpected type; expected polygon or multipolygon, got {shp_geom.geom_type}")
    return rfc_feature_list

def create_s3_resource(access_key_id: str, secret_access_key: str, region_name: str):
    session = boto3.Session(access_key_id, secret_access_key, region_name=region_name)
    resource = session.resource("s3")
    return resource


if __name__ == "__main__":
    import os

    from dotenv import load_dotenv

    access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
    secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    default_region = os.environ["AWS_DEFAULT_REGION"]

    s3_resource = create_s3_resource(access_key_id, secret_access_key, default_region)

    start_dt = datetime.datetime.strptime(FIRST_RECORD, "%Y-%m-%d")
    rfc_features = get_rfc_features()
    potential_urls = create_potential_urls(rfc_features, start_dt, FTP_HOST)
    verified_urls = [verified_url for verified_url in verify_urls(potential_urls)][:50]
    for streamed_zip in stream_zips_to_s3(verified_urls, s3_resource, "tempest"):
        print(streamed_zip)
