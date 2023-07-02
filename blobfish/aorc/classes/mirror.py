import datetime
from dataclasses import dataclass


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

    def s3_key(self, prefix: str = "test/mirrors/aorc/precip/") -> str:
        if prefix[-1] != "/":
            prefix += "/"
        url_parts = self.url.split("/")
        return f"{prefix}{'/'.join(url_parts[-3:])}"
