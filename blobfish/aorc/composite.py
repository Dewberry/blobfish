import datetime
import os
from tempfile import TemporaryDirectory
from urllib.parse import quote

from classes.common import BasicDescriptors


def create_composite_dataset_identifiers(
    start_date: datetime.datetime, end_date: datetime.datetime, location_name: str
) -> BasicDescriptors:
    dataset_id = f"composite_{start_date.strftime('%Y%m%d%H')}".lower()
    dataset_name = dataset_id
    start_time_formatted = start_date.strftime("%Y-%m-%d %H:%M")
    end_time_formatted = end_date.strftime("%Y-%m-%d %H:%M")
    dataset_title = f"{location_name} Composite Dataset, {start_time_formatted} to {end_time_formatted}"
    dataset_description = f"A composite dataset of AORC precipitation data covering {location_name}, from {start_time_formatted} to {end_time_formatted}"
    dataset_url = quote(dataset_id)
    descriptors = BasicDescriptors(dataset_title, dataset_id, dataset_name, dataset_url, dataset_description)
    return descriptors


def create_composite_s3_path(bucket: str, start_time: datetime.datetime) -> str:
    return f"s3://{bucket}/transforms/aorc/precipitation/{start_time.year}/{start_time.strftime('%Y%m%d%H')}.zarr"


if __name__ == "__main__":
    from classes.composite import DatasetTracker
    from composite_utils.array import create_composite_dataset, upload_zarr
    from composite_utils.cloud import check_zarr_modification, stream_s3_zipped
    from composite_utils.general import create_composite_wkt, upload_composite_to_ckan
    from composite_utils.rdf import retrieve_mirror_dataset_metadata
    from const import RFC_INFO_LIST, ZARR_CURRENT_VERSION_URI
    from dotenv import load_dotenv
    from general_utils.ckan import create_ckan_resource
    from general_utils.cloud import create_s3_resource
    from general_utils.provenance import get_command_list, retrieve_meta

    load_dotenv()

    bucket = "tempest"
    access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
    secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    default_region = os.environ["AWS_DEFAULT_REGION"]
    ckan_base_url = os.environ["CKAN_URL"]

    s3_resource = create_s3_resource(access_key_id, secret_access_key, default_region)
    command_list = get_command_list()
    prov_meta = retrieve_meta()
    rfc_count = len(RFC_INFO_LIST)
    for mirror_list in retrieve_mirror_dataset_metadata(ckan_base_url, rfc_count):
        mirror_wkts = [str(mirror.wkt) for mirror in mirror_list]
        composite_wkt = create_composite_wkt(mirror_wkts)
        composite_location_name = "Contiguous United States"
        with TemporaryDirectory() as tmpdir:
            with DatasetTracker() as tracker:
                for mirror in mirror_list:
                    nc_paths = stream_s3_zipped(s3_resource, mirror.url, tmpdir)
                    tracker.register_netcdfs(mirror.uri, nc_paths)
                for nc_paths, uris, start_time in tracker.group_data_by_time():
                    end_time = start_time + datetime.timedelta(hours=1)
                    composite_dataset = create_composite_dataset(nc_paths)
                    zarr_s3_path = create_composite_s3_path(bucket, start_time)
                    upload_zarr(zarr_s3_path, composite_dataset)
                    composite_last_modified = check_zarr_modification(s3_resource, zarr_s3_path)

                    # Create upload to CKAN
                    descriptors = create_composite_dataset_identifiers(
                        start_time,
                        end_time,
                        composite_location_name,
                    )
                    resources = [
                        create_ckan_resource(
                            mirror.url,
                            ZARR_CURRENT_VERSION_URI,
                            None,
                            "Distribution of s3 zarr containing precipitation data",
                            True,
                        )
                    ]
                    upload_composite_to_ckan(
                        ckan_base_url,
                        os.environ["CKAN_API_KEY"],
                        descriptors.dataset_id,
                        descriptors.name,
                        os.environ["CKAN_DATA_GROUP"],
                        descriptors.title,
                        descriptors.url,
                        descriptors.notes,
                        composite_last_modified.replace(tzinfo=None).isoformat(),
                        prov_meta.remote_docker_file,
                        prov_meta.remote_compose_file,
                        prov_meta.docker_image,
                        prov_meta.git_repo,
                        prov_meta.commit_hash,
                        prov_meta.docker_repo,
                        prov_meta.digest_hash,
                        start_time.isoformat(),
                        end_time.isoformat(),
                        mirror.resolution,
                        composite_location_name,
                        composite_wkt,
                        command_list,
                        uris,
                        resources,
                    )


"""
Pseudocode

Get command list
Get provenance data (docker, git details)
Query CKAN endpoint for catalog of MirrorDataset instances
Query retrieved graph to get unique start dates from datasets
For each unique start date:
    Query mirror dataset catalog graph for datasets with start date of current iteration value, retrieving the following attributes - (
        mirror dataset uri
        downloadURL
        rfc geometry
        start date
        end date
        spatial resolution
    Ensure uniform end date (time periods align exactly)
    Ensure quantity of unique datasets is equal to expected number (should be same as list of RFCs in constants)
    Create multipolygon from rfc geoms, create convex hull of multipolygon to act as new spatial extent
    With temporary directory as td:
        With dataset tracker as tracker:
            For each unique mirror dataset:
                Stream zipped data from s3 as bytes
                Extract to td
                Register association between mirror dataset and netCDF file in tracker
            Execute query which groups all netCDF paths and parent mirror dataset URI paths by common timestamp
            For each unique timestamp, concat nc paths, and concat mirror paths:
                Split concat nc paths and concat mirror paths into lists of strings
                Create xarray multifile dataset from nc paths
                Save xarray multifile dataset to s3 as zarr
                Load s3 object metadata with head request
                Record last modification date from s3
                Use retrieved spatial resolution, last modification, timestamp, end time (timestamp + 1 hour), provenance metadata, lsit of mirror dataset URIs, location name ("CONUS"), and merged convex hull WKT to create JSON of composite metadata
                Send POST request to CKAN instance
"""
