### Summary

#### aorc/
 aorc/const - constants used in transfer

 aorc/main - old script which stitches together const, mirror, and source

 aorc/mirror - old script which mirrors data from NOAA AORC directory to s3 and creates CONUS grids from matching hourly data

 aorc/ontology - script used to generate Turtle file with AORC ontology defined as subclasses of existing RDF vocabularies

 aorc/parse - script to parse metadata from s3 uploads and create Turtle file documenting transfer using AORC ontology

 aorc/source - old script used to generate graph objects used in main.py

#### pyrdf/
pyrdf - Defines ontologies used in metadata creation

#### utils/
utils/blobstore - utility for dealing with s3 key listing

utils/dockerinfo - utility for getting script name relative to docker working directory, docker tag

utils/logger - utility for logging setup
