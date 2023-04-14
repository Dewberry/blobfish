""" Script to parse storm transposition model documentation and create JSON-LD metadata """

import json
import re
import datetime
from collections.abc import Generator
from typing import Any, cast
from shapely.geometry import shape
from .classes import TranspositionDocumentation, TemplateInputs
from ..utils.cloud_utils import (
    get_s3_content,
    get_object_property,
    extract_bucketname_and_keyname,
    check_exists,
    upload_body,
    ObjectProperties,
)


class DatasetConstructor:
    DATASET_TEMPLATE = """
        {{
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
            "publisher": {{
                "@type": "org:Organization",
                "name": "Federal Emergency Management Agency",
                "suborganizationOf": {{
                    "@type": "org:Organization",
                    "name": "Department of Homeland Security",
                    "suborganizationOf": {{
                        "@type": "org:Organization",
                        "name": "United States Government"
                    }}
                }}
            }},
            "contactPoint": {{
                "@type": "vcard:Contact",
                "fn": "Nicholas Roberts",
                "hasEmail": "nrjoberts@dewberry.com"
            }},
            "identifier": "{doiUrl}",
            "accessLevel": "public",
            "bureauCode": "024:070",
            "programCode": "024:006",
            "rights": "This dataset is located on an s3 bucket which requires an AWS account which has been granted access to said bucket",
            "spatial": "{centerY}, {centerX}",
            "temporal": "{startDateIsoformat}/P{duration}H",
            "distribution": [
                {{
                    "@type": "dcat:Distribution",
                    "description": "DSS file created by storm transposition model for {watershedName}, {startDate}",
                    "downloadURL": "{dssKey}",
                    "format": "Data Storage System",
                    "mediaType": "application/octet"
                }}
            ],
            "language": [
                "en"
            ],
            "season": "{season}",
            "waterYear": {waterYear},
            "watershed": {{
                "@type": "geo:Geometry",
                "description": "Geometry of the watershed being transposed",
                "region": "{watershedWKT}",
                "name": "{watershedName}"
            }},
            "transposition": {{
                "@type": "geo:Geometry",
                "description": "Geometry of the region in which the watershed is being transposed",
                "region": "{transpositionRegionWKT}"
            }},
            "image": {{
                "@type": "dcat:Dataset",
                "title": "Storm Transposition Model, {watershedName} {startDate}",
                "modified": "{imgModified}",
                "description": "Image of watershed transposition results for {watershedName} watershed, {startDate}",
                "distribution": [
                    {{
                        "@type": "dcat:Distribution",
                        "description": "PNG file for {watershedName} {startDate} model",
                        "downloadURL": "{imgKey}",
                        "format": "PNG Image",
                        "mediaType": "image/png"
                    }}
                ]
            }},
            "stats": {{
                "@type": "aorc:TranspositionStatistics",
                "count": {count},
                "mean": {mean},
                "max": {max},
                "min": {min},
                "sum": {sum},
                "normMean": {normMean}
            }},
            "wasGeneratedBy": {{
                "@type": "aorc:TranspositionJob",
                "wasStartedBy": {{
                    "@type": "aorc:TranspositionScript",
                    "@id": "{scriptPath}",
                    "dockerImage": "{dockerPath}",
                    "description": "The script which is accessible at its IRI path once inside the docker image to which it belongs"
                }}
            }}
        }}
    """
    COMPOSITE_S3_TEMPLATE = "s3://tempest/transforms/aorc/precipitation/{y}/{ymdh}.zarr/"

    def __init__(
        self,
        context: str,
        describedBy: str,
        bucket: str,
        documentation_prefix: str,
        script_path: str,
        docker_path: str,
        doi_url: str,
        client: Any | None = None,
        limit: int | None = None,
        year: int | None = None,
    ) -> None:
        self.context = context
        self.describedBy = describedBy
        self.bucket = bucket
        self.documentation_prefix = documentation_prefix
        self.client = client
        self.script_path = script_path
        self.docker_path = docker_path
        self.doi_url = doi_url
        self.datasets = []
        self.limit = limit
        self.year = year

    def __get_key(self) -> Generator[str, None, None]:
        i = 0
        docs_pattern = re.compile(r".*\.json$")
        if self.year:
            docs_pattern = re.compile(r".*yr\d{4}\.json$".replace("yr", str(self.year)))
        for resp in get_s3_content(self.bucket, self.documentation_prefix, True, self.client):
            key = resp.get("Key")
            if key:
                if re.match(docs_pattern, key):
                    if self.limit:
                        if i < self.limit:
                            yield key
                            i += 1
                        else:
                            break
                    else:
                        yield key

    def __get_documentation(self, key: str) -> TranspositionDocumentation:
        streaming_body = get_object_property(self.bucket, key, ObjectProperties.BODY, client)
        string_body = streaming_body.read().decode("utf-8")
        json_body = json.loads(string_body)
        documentation = TranspositionDocumentation(
            json_body.get("start"),
            json_body.get("duration"),
            json_body.get("stats"),
            json_body.get("metadata"),
            json_body.get("geom"),
        )
        return documentation

    @staticmethod
    def __create_dss(key: str) -> str:
        # Get assumed s3 key for dss file using key of documentation file
        path_parts = key.split("/")
        json_file = path_parts[-1]
        dss_dir = "dss"
        dss_file = json_file.replace(".json", ".dss")
        path_parts[-2:] = [dss_dir, dss_file]
        dss_s3_path = "/".join(path_parts)
        return dss_s3_path

    def __fetch_modification(self, key: str) -> str:
        # Get modification date of specified key
        last_modification = get_object_property(self.bucket, key, ObjectProperties.LAST_MODIFIED, self.client)
        last_modification_dt = cast(datetime.datetime, last_modification)
        return last_modification_dt.isoformat()

    def __fetch_wkt(self, geojson_s3_path: str) -> str:
        # Load geojson from s3 and convert to wkt
        bucket, key = extract_bucketname_and_keyname(geojson_s3_path)
        streaming_body = get_object_property(bucket, key, ObjectProperties.BODY, self.client)
        string_body = streaming_body.read().decode("utf-8")
        json_body = json.loads(string_body)
        features = json_body.get("features")
        if len(features) > 1:
            raise ValueError(f"Expected GeoJSON object with a single feature, got {len(features)} features")
        geometry = features[0].get("geometry")
        wkt = shape(geometry).wkt
        return wkt

    def __create_png(self, key: str) -> str:
        # Get assumed s3 key for png file using key of documentation file
        path_parts = key.split("/")
        json_file = path_parts[-1]
        png_dir = "pngs"
        png_file = json_file.replace(".json", ".png")
        path_parts[-2:] = [png_dir, png_file]
        png_s3_path = "/".join(path_parts)
        return png_s3_path

    def __format_composite(self, start: datetime.datetime, duration: int) -> list[dict]:
        # Formats provided composite s3 path template with provided date
        current = start
        end = start + datetime.timedelta(hours=duration)
        composite_datsets = []
        while current < end:
            y = current.year
            ymdh = current.strftime("%Y%m%d%H")
            zarr_path = self.COMPOSITE_S3_TEMPLATE.format(y=y, ymdh=ymdh)
            composite_dataset = {"@type": "aorc:CompositeDataset", "@id": zarr_path}
            s3_path = zarr_path + ".zmetadata"
            composite_bucket, composite_key = extract_bucketname_and_keyname(s3_path)
            if check_exists(composite_bucket, composite_key, self.client):
                composite_datsets.append(composite_dataset)
            current += datetime.timedelta(hours=1)
        return composite_datsets

    def __format_template(self, inputs: TemplateInputs):
        return self.DATASET_TEMPLATE.format(
            watershedName=inputs.watershedName,
            startDate=inputs.startDate,
            dssModified=inputs.dssModified,
            doiUrl=inputs.doiUrl,
            centerY=inputs.centerY,
            centerX=inputs.centerX,
            startDateIsoformat=inputs.startDateIsoformat,
            duration=inputs.duration,
            season=inputs.season,
            waterYear=inputs.waterYear,
            watershedWKT=inputs.watershedWKT,
            dssKey=inputs.dssKey,
            transpositionRegionWKT=inputs.transpositionRegionWKT,
            imgModified=inputs.imgModified,
            imgKey=inputs.imgKey,
            count=inputs.count,
            mean=inputs.mean,
            max=inputs.max,
            min=inputs.min,
            sum=inputs.sum,
            normMean=inputs.normMean,
            scriptPath=inputs.scriptPath,
            dockerPath=inputs.dockerPath,
        )

    def populate_datasets(self) -> None:
        for obj_key in self.__get_key():
            doc = self.__get_documentation(obj_key)
            dss_key = self.__create_dss(obj_key)
            dss_modified = self.__fetch_modification(dss_key)
            img_key = self.__create_png(obj_key)
            img_modified = self.__fetch_modification(img_key)
            composite_paths = self.__format_composite(doc.format_start_date(), doc.duration)
            watershed_wkt = self.__fetch_wkt(doc.metadata.watershed_source)
            transposition_wkt = self.__fetch_wkt(doc.metadata.transposition_domain_source)
            template_inputs = TemplateInputs(
                doc.metadata.watershed_name,
                doc.start.datetime,
                dss_modified,
                self.doi_url,
                str(doc.geom.center_y),
                str(doc.geom.center_x),
                doc.format_start_date().isoformat(),
                doc.duration,
                doc.start.season,
                doc.start.water_year,
                watershed_wkt,
                create_s3_path(self.bucket, dss_key),
                transposition_wkt,
                img_modified,
                create_s3_path(self.bucket, img_key),
                doc.stats.count,
                doc.stats.mean,
                doc.stats.max,
                doc.stats.min,
                doc.stats.sum,
                doc.stats.norm_mean,
                self.script_path,
                self.docker_path,
            )
            dataset_string = self.__format_template(template_inputs)
            dataset_json = json.loads(dataset_string)
            dataset_json["source"] = composite_paths
            self.datasets.append(dataset_json)

    def as_catalog(self) -> dict:
        catalog = {
            "@context": self.context,
            "@type": "dcat:Catalog",
            "conformsTo": "https://project-open-data.cio.gov/v1.1/schema",
            "describedBy": self.describedBy,
            "datasets": self.datasets,
        }
        return catalog

    def serialize_to_file(self, file_path: str) -> None:
        dictionary = json.dumps(self.as_catalog(), indent=4)
        with open(file_path, "w") as outfile:
            outfile.write(dictionary)

    def serialize_to_s3(self, key: str) -> None:
        dictionary = json.dumps(self.as_catalog(), indent=4)
        upload_body(self.bucket, key, dictionary, self.client)


def create_s3_path(bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key}"


if __name__ == "__main__":
    from dotenv import load_dotenv
    import logging
    from ..utils.cloud_utils import get_client
    from ..utils.logger import set_up_logger

    load_dotenv()

    set_up_logger(level=logging.INFO)

    client = get_client()
    for year in range(1979, 2023):
        logging.info(f"Creating JSON-LD catalog for {year}")
        constructor = DatasetConstructor(
            "https://ckan.dewberryanalytics.com/dataset/56bb8b57-e9e7-41bf-a347-c8a467ba1c68/resource/3101b1f8-db5d-43fe-9159-1342be080480/download/sstcatalog.jsonld",
            "https://ckan.dewberryanalytics.com/dataset/56bb8b57-e9e7-41bf-a347-c8a467ba1c68/resource/0b6b1035-cede-49a9-9dc3-36dcbcb50cbc/download/sstcatalog.json",
            "tempest",
            "watersheds/indian-creek/indian-creek-transpo-area-v01/72h/docs/",
            "extract_storms_v2.py",
            "https://hub.docker.com/layers/placeholder",
            "http://dx.doi.org/nonfunctioning/doi/example",
            client,
            None,
            year,
        )
        constructor.populate_datasets()
        constructor.serialize_to_s3(f"graphs/aorc/precip/transposition/{year}.jsonld")
