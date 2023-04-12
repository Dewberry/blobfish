""" Script to parse storm transposition model documentation and create JSON-LD metadata """

import json
from ..utils.cloud_utils import get_s3_content
from typing import Any

def get_documentation(bucket: str, documentation_prefix: str, client: Any | None = None):
    for resp in get_s3_content(bucket, documentation_prefix, False, client):
        print(resp)