## AORC
Holds the main scripts as well as the utility scripts and classes that are used to upload metadata necessary for the creation of metadata to CKAN

### /classes
Holds classes used by individual processes as well as classes shared between processes in the AORC pipeline

### /composite_utils
Holds utility scripts used by the composite task of the AORC pipeline

### /general_utils
Holds general utility scripts used by multiple processes of the AORC pipeline

### /mirror_utils
Holds utility scripts used by the mirror task of the AORC pipeline

### /transposition_utils
Holds utility scripts used by the transposition metadata collection task of the AORC pipeline

### composite.py
Main script for the composite creation task
Responsible for querying the available mirror datasets from CKAN, aligning the data based on shared temporal coverage, and converting the monthly zipped data to hourly zarr format data, as well as collecting and uploading relevant metadata from this process

### const.py
Holds constants used in the AORC pipeline, including URIs for data formats, RFC info, data portal URLs, etc.

### mirror.py
Main script for the mirror creation task
Responsible for verifying the total available data from NOAA, its asynchronous acquistion, and its upload to s3, as well as collecting and uploading relevant metadata not only for the source data but also the mirror datasets created in the process

### transposition_meta.py
Main script for collecting, parsing, and submitting metadata created during stochastic storm transposition models to CKAN for serialization as RDF