"""
Pseudocode

Get command list
Get provenance metadata
Connect to meilisearch database
Query database to retrieve: (
    start time,
    duration,
    s3 paths to watershed, transposition, and DSS file,
    ATLAS14 dataset used to normalize precip,
    watershed name,
    transposition region name,
    mean precip,
    max precip,
    min precip,
    total precip,
    normalized mean precip,
    maximum precip point
    )
For row in results (if I understand NoSQL):
    with DSS file opened:
        Check spatial resolution
        Make sure number of records matches duration
    Create convex hulls of transposition region and watershed region
    Create JSON of transposition dataset metadata
    Make POST request to CKAN instance
"""
