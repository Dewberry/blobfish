""" Script to parse storm transposition model documentation and create JSON-LD metadata """

import json
from ..utils.cloud_utils import get_s3_content, get_object_body_string
from collections.abc import Generator
from typing import Any, cast
from dataclasses import dataclass, field
import re

@dataclass
class TranspositionStart:
    datetime: str
    timestamp: int
    calendar_year: int
    water_year: int
    season: str

@dataclass
class TranspositionStatistics:
    count: int
    mean: float
    max: float
    min: float
    sum: float
    norm_mean: float

@dataclass
class TranspositionMetadata:
    source: str
    watershed_name: str
    transposition_domain_name: str
    watershed_source: str
    transposition_domain_source: str
    create_time: str

@dataclass
class TranspositionGeometry:
    x_delta: int
    y_delta: int
    center_x: float
    center_y: float

@dataclass
class TranspositionDocumentation:
    _start: dict
    start: TranspositionStart = field(init=False)
    duration: int
    _stats: dict
    stats: TranspositionStatistics = field(init=False)
    _metadata: dict
    metadata: TranspositionMetadata = field(init=False)
    _geom: dict
    geom: TranspositionGeometry = field(init=False)

    def __post_init__(self):
        self.start = TranspositionStart(**self._start)
        self.stats = TranspositionStatistics(**self._stats)
        self.metadata = TranspositionMetadata(**self._metadata)
        self.geom = TranspositionGeometry(**self._geom)

@dataclass
class TemplateInputs:
    context: str
    describedBy: str
    watershedName: str
    startDate: str
    dssModified: str
    doiUrl: str
    centerY: str
    centerX: str
    startDateIsoformat: str
    season: str
    waterYear: int
    watershedWKT: str
    dssKey: str
    transpositionRegionWKT: str
    imgModified: str
    imgKey: str
    count: int
    mean: float
    max: float
    min: float
    sum: float
    normMean: float
    zarrPrefix: str
    scriptPath: str
    dockerPath: str

class JSONLDConstructor:
    TEMPLATE = """{
    "@context": "{context}",
    "@type": "dcat:Catalog",
    "conformsTo": "https://project-open-data.cio.gov/v1.1/schema",
    "describedBy": "{describedBy}",
    "dataset": [
        {
            "title": "Storm Transposition Model, {watershedName} {startDate}",
            "description": "Data documenting modeling of transposition region for {watershedName}, {startDate}",
            "keyword": [
                "aorc",
                "transposition",
                "{watershedName}",
                "storm",
                "hydrological",
                "modeling",
                "hydrology",
                "precipitation"
            ],
            "modified": "{dssModified}",
            "publisher": {
                "@type": "org:Organization",
                "name": "Federal Emergency Management Agency",
                "suborganizationOf": {
                    "@type": "org:Organization",
                    "name": "Department of Homeland Security",
                    "suborganizationOf": {
                        "@type": "org:Organization",
                        "name": "United States Government"
                    }
                }
            },
            "contactPoint": {
                "@type": "vcard:Contact",
                "fn": "Nicholas Roberts",
                "hasEmail": "nrjoberts@dewberry.com"
            },
            "identifier": "{doiUrl}",
            "accessLevel": "public",
            "bureauCode": "024:070",
            "programCode": "024:006",
            "rights": "This dataset is located on an s3 bucket which requires an AWS account which has been granted access to said bucket",
            "spatial": "{centerY}, {centerX}",
            "temporal": "{startDateIsoformat}/P{duration}H",
            "distribution": [
                {
                    "@type": "dcat:Distribution",
                    "description": "DSS file created by storm transposition model for {watershedName}, {startDate}",
                    "downloadURL": "{dssKey}",
                    "format": "Data Storage System",
                    "mediaType": "application/octet"
                }
            ],
            "language": [
                "en"
            ],
            "season": "{season}",
            "waterYear": {waterYear},
            "watershed": {
                "@type": "geo:Geometry",
                "description": "Geometry of the watershed being transposed",
                "region": "{watershedWKT}"
                "name": "{watershedName}"
            },
            "transposition": {
                "@type": "geo:Geometry",
                "description": "Geometry of the region in which the watershed is being transposed",
                "region": "{transpositionRegionWKT}"
            },
            "image": {
                "@type": "dcat:Dataset",
                "title": "Storm Transposition Model, {watershedName} {startDate}",
                "modified": "{imgModified}",
                "description": "Image of watershed transposition results for {watershedName} watershed, {startDate}",
                "distribution": [
                    {
                        "@type": "dcat:Distribution",
                        "description": "PNG file for {watershedName} {startDate} model",
                        "downloadURL": "{imgKey}",
                        "format": "PNG Image",
                        "mediaType": "image/png"
                    }
                ]
            },
            "stats": {
                "@type": "aorc:TranspositionStatistics",
                "count": {count},
                "mean": {mean},
                "max": {max},
                "min": {min},
                "sum": {sum},
                "normMean": {normMean}
            },
            "source": [
                {
                    "@type": "aorc:CompositeDataset",
                    "@id": "{zarrPrefix}"
                }
            ],
            "wasGeneratedBy": {
                "@type": "aorc:TranspositionJob",
                "wasStartedBy": {
                    "@type": "aorc:TranspositionScript",
                    "@id": "{scriptPath}
                    "dockerImage": "{dockerPath}"
                    "description": "The script which is accessible at its IRI path once inside the docker image to which it belongs"
                },
                "used": [
                    {
                        "@type": "aorc:CompositeDataset",
                        "@id": "{zarrPrefix}"
                    }
                ]
            }
        }
    ]
}"""
    def __init__(self, context: str, describedBy: str, bucket: str, documentation_prefix: str) -> None:
        self.context = context
        self.describedBy = describedBy
        self.bucket = bucket
        self.documentation_prefix = documentation_prefix

    def __create_dss(self, key: str):
        # Get assumed s3 key for dss file using key of documentation file
        pass

    def __fetch_modification(self, key: str):
        # Get modification date of specified key
        pass

    def __fetch_wkt(self, key: str):
        # Load geojson from s3 and convert to wkt
        pass

    def __create_png(self, key: str):
        # Get assumed s3 key for png file using key of documentation file
        pass

    def __identify_composite_dataset(self, date: str):
        # Retrieve s3 path for datasets associated with date given
        pass

    def __extract_metadata(self, key: str, documentation: TranspositionDocumentation) -> None:
        pass

    def __format_template(self, script_path: str, docker_path: str):
        pass

def get_documentation(bucket: str, documentation_prefix: str, client: Any | None = None) -> Generator[TranspositionDocumentation, None, None]:
    docs_pattern = re.compile(r'.*\.json$')
    for resp in get_s3_content(bucket, documentation_prefix, True, client):
        key = cast(str, resp.get("Key"))
        if re.match(docs_pattern, key):
            streaming_body = get_object_body_string(bucket, key, client)
            string_body = streaming_body.read().decode("utf-8")
            json_body = json.loads(string_body)
            documentation = TranspositionDocumentation(json_body.get("start"), json_body.get("duration"), json_body.get("stats"), json_body.get("metadata"), json_body.get("geom"))
            yield documentation

if __name__ == "__main__":
    from dotenv import load_dotenv
    from ..utils.cloud_utils import get_client

    load_dotenv()

    client = get_client()

    for docs in get_documentation("tempest", "watersheds/indian-creek/indian-creek-transpo-area-v01/72h/docs/", client):
        print(docs)