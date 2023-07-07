"""
Pseudocode

Get command list
Get provenance metadata
Connect to meilisearch database
Query database to retrieve: (
    start time,
    duration,
    s3 paths to watershed and transposition geojsons,
    s3 path of image PNG,
    watershed name,
    transposition region name,
    mean precip,
    max precip,
    min precip,
    total precip,
    normalized mean precip,
    maximum precip point
    )
If normalized mean was fetched:
    Deduce ATLAS file used to normalize based on time period, spatial region
Deduce s3 path of DSS file
For hit in results (if I understand NoSQL):
    Download DSS file
    with DSS file opened:
        Check spatial resolution
        Make sure number of records matches duration
    Create convex hulls of transposition region and watershed region
    Create JSON of transposition dataset metadata
    Make POST request to CKAN instance
"""
