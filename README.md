# blobfish

A collection of ontologies and scripts for generating mirrors for use in data pipelines to support the development of geospatial datasets used in risk and resilience studies.


---

#### Ontologies:

[AORC](http://htmlpreview.github.io/?https://github.com/Dewberry/blobfish/blob/aorc/semantics/html/aorc/index.html): Analysis of Record for Calibration precipitation and temperature datasets.

---

#### Scripts:

[AORC](./blobfish/aorc/):
 * [composite](./blobfish/aorc/composite.py) - Script for creating CONUS-level composite gridded datasets composed of mirrored AORC data which utilizes RDF metadata created during the mirroring process
 * [const](./blobfish/aorc/const.py) - Constants used in AORC processing or parsing
 * [parse_composite](./blobfish/aorc/parse_composite.py) - Script for parsing metadata from s3 objects of created composite gridded dataset to add onto existing RDF metadata created during the mirroring process in order to document the relationship between the mirrored datasets, the composited datasets, and the compositing process
 * [parse_transfer](./blobfish/aorc/parse_transfer.py) - Script for parsing metadata from s3 objects of mirrored datasets to create RDF metadata documenting the relationship between the mirror, the source, and the mirroring process
 * [transfer](./blobfish/aorc/transfer.py) - Script for transferring data from AORC http server to s3 bucket

 ##### Setup

 To run the scripts in this repo, you should have a .env file in the same directory as this repo which has the following keys:

 ```
AWS_ACCESS_KEY_ID=access_id_here
AWS_SECRET_ACCESS_KEY=access_key_here
AWS_DEFAULT_REGION=aws_region_here
TAG=docker_image_tag
HASH=docker_image_hash
 ```

 The workflow during development was to launch a docker container using the specified tag and hash from the docker hub image https://hub.docker.com/layers/njroberts/blobfish-python/ and use the container to run the scripts in the following sequence in order to both complete the composite process and create RDF TTL files documenting the metadata for the jobs:

 ```
python -m blobfish.aorc.transfer
python -m blobfish.aorc.parse_transfer
python -m blobfish.aorc.composite
python -m blobfish.aorc.parse_composite
 ```