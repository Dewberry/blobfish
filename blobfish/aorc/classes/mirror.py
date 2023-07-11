"""Classes used in mirroring process"""
import datetime
from dataclasses import dataclass
from shapely.geometry import Polygon, MultiPolygon


@dataclass
class RFCFeature:
    name: str
    geom: Polygon | MultiPolygon


@dataclass
class RFCInfo:
    """
    Data Property: Regional Forecast Center (RFC) names and aliases
    """

    alias: str
    name: str


@dataclass
class AORCDataURL:
    url: str
    rfc_alias: str
    last_modified: datetime.datetime = None
    additional_args: dict = None

    def s3_key(self, prefix: str = "mirrors/aorc/precip/") -> str:
        """Construct assumed s3 key for mirror dataset

        Args:
            prefix (str, optional): prefix to attach to zip file. Defaults to "mirrors/aorc/precip/".

        Returns:
            str: mirror dataset s3 key
        """
        if prefix[-1] != "/":
            prefix += "/"
        url_parts = self.url.split("/")
        return f"{prefix}{'/'.join(url_parts[-3:])}"
