import datetime
from urllib.parse import quote

from classes.common import BasicDescriptors


def create_transposition_dataset_identifiers(
    dataset_id: str,
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    watershed_name: str,
    transposition_name: str,
) -> BasicDescriptors:
    dataset_name = dataset_id
    start_time_formatted = start_date.strftime("%Y-%m-%d")
    end_time_formatted = end_date.strftime("%Y-%m-%d")
    dataset_title = (
        f"{watershed_name} Transposition Dataset, {start_time_formatted} to {end_time_formatted} ({transposition_name})"
    )
    dataset_description = f"Results of stochaistic storm transposition modeling for the watershed {watershed_name} within the transposition region {transposition_name}"
    dataset_url = quote(dataset_id)
    descriptors = BasicDescriptors(dataset_title, dataset_id, dataset_name, dataset_url, dataset_description)
    return descriptors


if __name__ == "__main__":
    import os

    from const import MS_INDEX, HEC_DSS_URI
    from dotenv import load_dotenv
    from general_utils.cloud import create_s3_resource, extract_bucketname_and_keyname
    from general_utils.provenance import get_command_list, retrieve_meta
    from general_utils.ckan import create_ckan_resource
    from transposition_utils.general import (
        stream_geojson_from_s3,
        upload_transposition_to_ckan,
        get_dss_last_modification,
    )
    from transposition_utils.ms import create_meilisearch_client, retrieve_ms_data
    from transposition_utils.rdf import retrieve_composite_dataset_uris

    load_dotenv()

    ms_host = os.environ["MEILI_HOST"]
    ms_api_key = os.environ["MEILI_KEY"]
    access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
    secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    default_region = os.environ["AWS_DEFAULT_REGION"]
    ckan_base_url = os.environ["CKAN_URL"]

    s3_resource = create_s3_resource(access_key_id, secret_access_key, default_region)
    ms_client = create_meilisearch_client(ms_host, ms_api_key)
    command_list = get_command_list()
    prov_meta = retrieve_meta()

    default_spatial_resolution = 4000

    for hit in retrieve_ms_data(ms_client, MS_INDEX):
        dataset_id = hit.id
        descriptors = create_transposition_dataset_identifiers(
            dataset_id, hit.start_time, hit.end_time, hit.watershed_name, hit.transposition_name
        )
        watershed_bucket, watershed_key = extract_bucketname_and_keyname(hit.watershed_geo_s3)
        watershed_geom = stream_geojson_from_s3(s3_resource, watershed_bucket, watershed_key)
        transposition_bucket, transposition_key = extract_bucketname_and_keyname(hit.transposition_geo_s3)
        transposition_geom = stream_geojson_from_s3(s3_resource, transposition_bucket, transposition_key)
        dss_bucket, dss_key = extract_bucketname_and_keyname(hit.dss_s3)
        dss_last_modification = get_dss_last_modification(s3_resource, dss_bucket, dss_key)
        composite_datasets = retrieve_composite_dataset_uris(ckan_base_url, hit.start_time, hit.end_time)
        composite_normalized_dict = {}
        if hit.norm_mean_precip:
            for ds in composite_datasets:
                composite_normalized_dict[str(ds)] = hit.atlas_s3
            else:
                composite_normalized_dict[str(ds)] = None
        resources = [
            create_ckan_resource(
                hit.dss_s3,
                HEC_DSS_URI,
                None,
                "Distribution of HEC-DSS file containing transposed model output",
                True,
            )
        ]
        upload_transposition_to_ckan(
            ckan_base_url,
            os.environ["CKAN_API_KEY"],
            descriptors.dataset_id,
            descriptors.name,
            os.environ["CKAN_DATA_GROUP"],
            descriptors.title,
            descriptors.url,
            descriptors.notes,
            dss_last_modification.isoformat(),
            prov_meta.remote_docker_file,
            prov_meta.remote_compose_file,
            prov_meta.docker_image,
            prov_meta.git_repo,
            prov_meta.commit_hash,
            prov_meta.docker_repo,
            prov_meta.digest_hash,
            hit.start_time.isoformat(),
            hit.end_time.isoformat(),
            default_spatial_resolution,
            hit.transposition_name,
            transposition_geom.wkt,
            hit.watershed_name,
            watershed_geom.wkt,
            None,
            hit.max_precip_point.wkt,
            hit.image_s3,
            hit.cell_count,
            hit.mean_precip,
            hit.max_precip,
            hit.min_precip,
            hit.total_precip,
            hit.norm_mean_precip,
            command_list,
            composite_datasets,
            resources,
        )
