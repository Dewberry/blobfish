from const import FIRST_RECORD
from general_utils.ckan import query_ckan_catalog
from general_utils.provenance import get_command_list, retrieve_meta
from rdflib import Graph

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
