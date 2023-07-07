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
With the exception of the source datasets, the metadata created by this repo is not RDF, but rather plain JSON (not JSON-LD, an RDF format). This is because this repo works in tandem with extensions created on the CKAN api specifically for parsing and serializing the uploaded metadata into an RDF format.

These extensions can be found (here)[https://example.org]

## Usage
First, populate your .env variables as shown in .env.example in order to connect to AWS, Meilisearch, and CKAN
In order to use this repo to produce metadata for AORC data, you must be using a linux-style OS that can execute shell scripts like 'init.sh'
If you are on a linux machine, follow these steps in order
- execute the init.sh shell script with the parameter of the remote docker image that will be used (this repo is tracked on docker.io/njroberts/blobfish-python)
- eg: <source init.sh njroberts/blobfish-python:1.0.0>
- attach to the created docker container
- execute one of the main scripts (mirror.py, composite.py, or transposition_meta.py)