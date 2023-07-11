# blobfish

## Summary
This repository is a proof of concept to evaluate the usability of resource description framework (RDF) metadata in documenting extensible data pipelines. The data pipeline documented by this repository is the mirroring, transformation, and subsequent use of NOAA AORC gridded precipitation data for stochaistic storm transposition (SST) modeling.

## Pipeline Description
This pipeline can be broken into 3 stages

### Mirror
The first stage is taking the AORC data from an FTP server, operated by NOAA, and putting it on s3. This is to allow for easier access and less potential network interruptions. This data is zipped into packages covering one month each and is partitioned by river forecast center (RFC).

### Composite
The second stage is unzipping the montly data into series of hourly netCDF data, aligning the datasets temporally, and merging the data to a contiguous coverage of the united states, rather than separated by RFC region.

### Transposition
The third stage is using the data now that it has been transformed into a more convenient format. Our use case is utilizing the precipitation data in SST hydrological modeling. This repo does not encompass the data processing, but rather consumes the metadata produced during this process and converts these to a compatible format for use as RDF

## RDF Implementation
The RDF creation is shifted from the data processing side to the CKAN server in order to ensure that changes made on CKAN are directly reflected in RDF representations of AORC datasets. An extension, 'ckanext-aorc', handles the AORC datasets in both their human readable (HTML) format and machine readable (Turtle) formats. This extension is in this repo at the ckanext-aorc/ directory

## Usage

### CKAN Extension
To enable the CKAN extension for handling AORC data, follow instructions found at ckanext-aorc/README.md

### Produce datasets
In order to use this repo to produce metadata for AORC data, you must be using a linux-style OS that can execute shell scripts like 'init.sh'
If you are on a linux machine, follow these steps in order
- create a .env file with credentials for AWS account used in mirroring, Meilisearch credentials for database tracking SST model metadata, and CKAN credentials to use for uploading datasets
- execute the init.sh shell script with the parameter of the remote docker image that will be used (this repo is tracked on docker.io/njroberts/blobfish-python)
- eg: <source init.sh njroberts/blobfish-python:1.0.0>
- attach to the created docker container
- execute one of the main scripts (mirror.py, composite.py, or transposition_meta.py)