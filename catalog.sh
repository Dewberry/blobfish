
#!/bin/bash

# example usage: ./catalog.sh aorc
set -euo pipefail

# CATALOG=aorc
CATALOG=$1

python -m rdf2py ${CATALOG} semantics/rdf/${CATALOG}.ttl
python -m blobfish.${CATALOG}.main
python -m docgen ${CATALOG} semantics/rdf/${CATALOG}.ttl