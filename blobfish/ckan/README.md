## CKAN

A collection of scripts used to demonstrate how RDF metadata on a CKAN instance can be used in graph data queries to identify different attributes about AORC data



### How to Use:
To use the code in this directory, use the main.py script in module form like so, where lon and lat are replaced with longitude and latitude values within the US:
```
python -m blobfish.ckan.main -x {lon} -y {lat}
```
The output of this will show you the s3 URI for all AORC resources that belong to the RFC office which corresponds to the point provided