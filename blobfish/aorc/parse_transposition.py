""" Script to parse storm transposition model documentation and create JSON-LD metadata """

import json
import re
import datetime
import requests
from jsonschema import validate, Draft4Validator
from collections.abc import Generator
from typing import Any, cast
from shapely.geometry import shape, box
from .classes import TranspositionDocumentation
from ..utils.cloud_utils import (
    get_s3_content,
    get_object_property,
    extract_bucketname_and_keyname,
    check_exists,
    upload_body,
    ObjectProperties,
)


class JSONLDConstructor:
    def __init__(
        self,
        context: str,
        describedBy: str,
        bucket: str,
        documentation_prefix: str,
        script_path: str,
        docker_path: str,
        contact_name: str,
        contact_email: str,
        client: Any | None = None,
        limit: int | None = None,
    ) -> None:
        self.context = context
        self.describedBy = describedBy
        self.bucket = bucket
        self.documentation_prefix = documentation_prefix
        self.client = client
        self.script_path = script_path
        self.docker_path = docker_path
        self.contact_name = contact_name
        self.contact_email = contact_email
        self.datasets = []
        self.limit = limit
        self.constructed = None

    def __get_key(self) -> Generator[str, None, None]:
        i = 0
        pattern_string = r".*\.json$"
        docs_pattern = re.compile(pattern_string)
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

    def populate_datasets(self) -> None:
        logging.info(f"Populating datasets from s3://{self.bucket} with prefix {self.documentation_prefix}")
        for obj_key in self.__get_key():
            dataset = DatasetConstructor(
                self.bucket,
                obj_key,
                self.script_path,
                self.docker_path,
                self.contact_name,
                self.contact_email,
                self.client,
            )
            ds_json = dataset.create()
            self.datasets.append(ds_json)

    def construct(self) -> None:
        logging.info("Constructing catalog")
        catalog = {
            "@context": self.context,
            "@type": "dcat:Catalog",
            "conformsTo": "https://project-open-data.cio.gov/v1.1/schema",
            "describedBy": self.describedBy,
            "dataset": self.datasets,
        }
        self.constructed = catalog

    def validate(self) -> bool:
        if self.constructed:
            schema_url = str(self.constructed.get("describedBy"))
            catalog_schema_json = requests.get(schema_url).json()
            logging.info("Validating created catalog")
            validate(self.constructed, catalog_schema_json, Draft4Validator)
            return True
        else:
            raise TypeError("Can't validate, catalog missing. Try constructing first.")

    def serialize_to_s3(self, key: str) -> None:
        if self.constructed:
            dictionary = json.dumps(self.constructed, indent=4)
            upload_body(self.bucket, key, dictionary, self.client)
        else:
            raise TypeError("Can't serialize, catalog missing. Try constructing first.")

    def serialize_to_file(self, file_path: str) -> None:
        if self.constructed:
            dictionary = json.dumps(self.constructed, indent=4)
            with open(file_path, "w") as outfile:
                outfile.write(dictionary)
        else:
            raise TypeError("Can't serialize, catalog missing. Try constructing first.")


class DatasetConstructor:
    def __init__(
        self,
        bucket: str,
        key: str,
        script_path: str,
        docker_path: str,
        contact_name: str,
        contact_email,
        client: Any | None,
    ) -> None:
        self.bucket = bucket
        self.key = key
        self.client = client
        self.documentation = self.__get_documentation()
        self.contact_point = {"@type": "vcard:Contact", "fn": contact_name, "hasEmail": contact_email}
        self.transpositionJob = {
            "@type": "aorc:TranspositionJob",
            "wasStartedBy": {
                "@type": "aorc:TranspositionScript",
                "@id": script_path,
                "dockerImage": {"@type": "aorc:DockerImage", "@id": docker_path},
                "description": "The script which is accessible at its IRI path once inside the docker image to which it belongs",
            },
        }

    @staticmethod
    def __create_s3_path(bucket: str, key: str) -> str:
        return f"s3://{bucket}/{key}"

    def __get_documentation(self) -> TranspositionDocumentation:
        logging.info(f"Getting documentation for {self.key}")
        streaming_body = get_object_property(self.bucket, self.key, ObjectProperties.BODY, client)
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

    def __create_dss(self) -> str:
        # Get assumed s3 key for dss file using key of documentation file
        path_parts = self.key.split("/")
        json_file = path_parts[-1]
        dss_dir = "dss"
        dss_file = json_file.replace(".json", ".dss")
        path_parts[-2:] = [dss_dir, dss_file]
        dss_s3_path = "/".join(path_parts)
        return dss_s3_path

    def __fetch_modification(self, target_key) -> str:
        # Get modification date of specified key
        last_modification = get_object_property(self.bucket, target_key, ObjectProperties.LAST_MODIFIED, self.client)
        last_modification_dt = cast(datetime.datetime, last_modification)
        return last_modification_dt.isoformat()

    def __fetch_wkt(self, geojson_s3_path: str, with_name: bool = False, bbox: bool = False) -> str | tuple[str, str]:
        # Load geojson from s3 and convert to wkt
        bucket, key = extract_bucketname_and_keyname(geojson_s3_path)
        streaming_body = get_object_property(bucket, key, ObjectProperties.BODY, self.client)
        string_body = streaming_body.read().decode("utf-8")
        json_body = json.loads(string_body)
        features = json_body.get("features")
        if len(features) > 1:
            raise ValueError(f"Expected GeoJSON object with a single feature, got {len(features)} features")
        geometry = features[0].get("geometry")
        geometry_shape = shape(geometry)
        if bbox:
            bounds = geometry_shape.bounds
            wkt = box(*bounds).wkt
        else:
            wkt = geometry_shape.wkt
        if with_name:
            name = json_body.get("name")
            return wkt, name
        return wkt

    def __create_png(self) -> str:
        # Get assumed s3 key for png file using key of documentation file
        path_parts = self.key.split("/")
        json_file = path_parts[-1]
        png_dir = "pngs"
        png_file = json_file.replace(".json", ".png")
        path_parts[-2:] = [png_dir, png_file]
        png_s3_path = "/".join(path_parts)
        return png_s3_path

    def __find_source_zarrs(self) -> list[dict]:
        # Get assumed s3 key for zarr files used in generation of modeled precip
        logging.info("Finding AORC precipitation files associated with watershed and transposition domain")
        sources = []
        start_date = self.documentation.format_start_date()
        current_date = start_date
        end_date = start_date + datetime.timedelta(hours=self.documentation.duration)
        while current_date < end_date:
            path = f"s3://tempest/transforms/aorc/precipitation/{current_date.year}/{current_date.strftime('%Y%m%d%H')}.zarr"
            composite = {"@type": "aorc:CompositeDataset", "@id": path}
            bucket, key = extract_bucketname_and_keyname(path)
            key += "/.zmetadata"
            if check_exists(bucket, key, self.client):
                sources.append(composite)
            else:
                logging.warning(f"Check on path {path} returned false, path does not exist")
            current_date += datetime.timedelta(hours=1)
        return sources

    def create(self) -> dict:
        logging.info(f"Creating dataset using documentation from {self.key}")
        dataset = {}
        dataset[
            "title"
        ] = f"Storm Transposition Data, {self.documentation.metadata.watershed_name} {self.documentation.format_start_date().strftime('%Y-%m-%d')}"
        dataset[
            "description"
        ] = f"Data documenting development of transposition region for {self.documentation.metadata.watershed_name}, {self.documentation.format_start_date().strftime('%Y-%m-%d')}"
        dataset["keyword"] = [
            "aorc",
            "transposition",
            self.documentation.metadata.watershed_name,
            "storm",
            "hydrological",
            "modeling",
            "hydrology",
            "precipitation",
        ]
        dataset["modified"] = self.documentation.format_start_date().isoformat()
        dataset["publisher"] = {
            "@type": "org:Organization",
            "name": "PTS",
            "suborganizationOf": {
                "@type": "org:Organization",
                "name": "FEMA",
                "suborganizationOf": {
                    "@type": "org:Organization",
                    "name": "Department of Homeland Security",
                    "suborganizationOf": {"@type": "org:Organization", "name": "United States Government"},
                },
            },
        }
        dataset["contactPoint"] = self.contact_point
        dataset["identifier"] = "http://dx.doi.org/nonfunctioning/doi/example"
        dataset["accessLevel"] = "public"
        dataset["bureauCode"] = ["024:70"]
        dataset["programCode"] = ["024:006"]
        dataset[
            "rights"
        ] = "This dataset is located on an s3 bucket which requires an AWS account which has been granted access to said bucket"
        dataset["spatial"] = f"{self.documentation.geom.center_y}, {self.documentation.geom.center_x}"
        dataset["temporal"] = f"{self.documentation.format_start_date().isoformat()}Z/PT{self.documentation.duration}H"
        dataset["distribution"] = [
            {
                "@type": "dcat:Distribution",
                "description": f"DSS file created by storm transposition model for {self.documentation.metadata.watershed_name}, {self.documentation.format_start_date().strftime('%Y-%m-%d')}",
                "downloadURL": f"{self.__create_s3_path(self.bucket, self.__create_dss())}",
                "format": "Data Storage System",
                "mediaType": "application/octet",
            }
        ]
        dataset["language"] = ["en"]
        dataset["season"] = self.documentation.start.season
        dataset["waterYear"] = self.documentation.start.water_year
        dataset["watershed"] = {
            "@type": "geo:Geometry",
            "description": f"Geometry of the bounding box for {self.documentation.metadata.watershed_name}",
            "region": self.__fetch_wkt(self.documentation.metadata.watershed_source, bbox=True),
            "name": self.documentation.metadata.watershed_name,
        }
        dataset["transposition"] = {
            "@type": "geo:Geometry",
            "description": f"Geometry of the bounding box which contains the transposition region associated with watershed",
            "region": self.__fetch_wkt(self.documentation.metadata.transposition_domain_source, bbox=True),
            "name": self.documentation.metadata.transposition_domain_name,
        }
        dataset["image"] = {
            "@type": "dcat:Dataset",
            "title": f"Storm Transposition Dataset, {self.documentation.metadata.watershed_name} {self.documentation.format_start_date().strftime('%Y-%m-%d')}",
            "modified": f"{self.__fetch_modification(self.__create_png())}",
            "description": f"Image of watershed transposition results for {self.documentation.metadata.watershed_name} watershed, {self.documentation.format_start_date().strftime('%Y-%m-%d')}",
            "distribution": [
                {
                    "@type": "dcat:Distribution",
                    "description": f"PNG file for {self.documentation.metadata.watershed_name} {self.documentation.format_start_date().strftime('%Y-%m-%d')} dataset",
                    "downloadURL": f"{self.__create_s3_path(self.bucket, self.__create_png())}",
                    "format": "PNG Image",
                    "mediaType": "image/png",
                }
            ],
        }
        dataset["stats"] = {
            "@type": "aorc:TranspositionStatistics",
            "count": self.documentation.stats.count,
            "mean": self.documentation.stats.mean,
            "max": self.documentation.stats.max,
            "min": self.documentation.stats.min,
            "sum": self.documentation.stats.sum,
            "normMean": self.documentation.stats.norm_mean,
        }
        dataset["wasGeneratedBy"] = self.transpositionJob
        dataset["source"] = self.__find_source_zarrs()
        return dataset


if __name__ == "__main__":
    from dotenv import load_dotenv
    import logging
    from ..utils.cloud_utils import get_client
    from ..utils.logger import set_up_logger

    load_dotenv()

    set_up_logger(level=logging.INFO)

    for year in range(1979, 2023):
        client = get_client()
        logging.info(f"Creating JSON-LD catalog for {year}")
        constructor = JSONLDConstructor(
            "https://ckan.dewberryanalytics.com/dataset/56bb8b57-e9e7-41bf-a347-c8a467ba1c68/resource/3101b1f8-db5d-43fe-9159-1342be080480/download/sstcatalog.jsonld",
            "https://ckan.dewberryanalytics.com/dataset/56bb8b57-e9e7-41bf-a347-c8a467ba1c68/resource/0b6b1035-cede-49a9-9dc3-36dcbcb50cbc/download/sstcatalog.json",
            "tempest",
            f"watersheds/kanawha/kanawha-transpo-area-v01/72h/docs/{year}0201",
            "extract_storms_v2.py",
            "172866912423.dkr.ecr.us-east-1.amazonaws.com/stormcloud",
            "Seth Lawler",
            "mailto:slawler@dewberry.com",
            client,
            None,
        )
        constructor.populate_datasets()
        constructor.construct()
        if constructor.validate():
            constructor.serialize_to_s3(f"graphs/aorc/precip/transposition/kanawha-transpo-area-v01/{year}.jsonld")
