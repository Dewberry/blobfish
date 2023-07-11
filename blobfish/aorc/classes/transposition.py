"""Classes used in producing transposition dataset metadata"""
import datetime
import os
from dataclasses import dataclass, field

from numpy import str_

from general_utils.cloud import extract_bucketname_and_keyname
from shapely.geometry import Point


@dataclass
class TranspositionMetadata:
    id: str
    duration: int
    start_time: datetime.datetime
    end_time: datetime.datetime
    watershed_name: str
    watershed_geo_s3: str
    transposition_name: str
    transposition_geo_s3: str
    image_s3: str
    cell_count: int
    mean_precip: float
    max_precip: float
    min_precip: float
    total_precip: float
    norm_mean_precip: float | None
    max_precip_x: float
    max_precip_y: float
    max_precip_point: Point = field(init=False)
    dss_s3: str = field(init=False)
    atlas_s3: str = field(init=False)

    def __get_s3_base(self) -> tuple[str, list[str], str]:
        geo_bucket, geo_key = extract_bucketname_and_keyname(self.transposition_geo_s3)
        key_parts = geo_key.split("/")
        return geo_bucket, key_parts[:-1], key_parts[-1]

    def __create_dss_s3_path(self) -> str:
        bucket, key_parts, transpo_file_name = self.__get_s3_base()
        transpo_folder = transpo_file_name.replace(".geojson", "")
        path = os.path.join(bucket, *key_parts, transpo_folder, "72h", "dss", self.start_time.strftime("%Y%m%d.dss"))
        return f"s3://{path}"

    def __create_atlas_s3_path(self) -> str:
        return "s3://tempest/noaa-atlas-14/2yr03da.vrt"

    def __create_max_precip_point(self) -> Point:
        return Point(self.max_precip_x, self.max_precip_y)

    def __post_init__(self):
        self.max_precip_point = self.__create_max_precip_point()
        self.dss_s3 = self.__create_dss_s3_path()
        self.atlas_s3 = self.__create_atlas_s3_path()
