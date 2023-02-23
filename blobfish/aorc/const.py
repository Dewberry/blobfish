from dataclasses import dataclass
from rdflib import Namespace

# First available dataset in the AORC repository
FIRST_RECORD = "1979-02-01"

# FTP server host common prefix
FTP_HOST = "https://hydrology.nws.noaa.gov/pub/aorc-historic/"

# Dataset Namespaces
SOURCE_CATALOG = Namespace("s3://tempest/catalogs/aorc/precip/source/#")
MIRROR_CATALOG = Namespace("s3://tempest/catalogs/aorc/precip/mirror/#")
# COMPOSITE_CATALOG = Namespace("s3://tempest/catalogs/aorc/precip/composite/#")

# # Root Directories for Data
# FTP_ROOT = Namespace("https://hydrology.nws.noaa.gov/pub/aorc-historic/")
# MIRROR_ROOT = Namespace("s3://tempest/mirrors/aorc/precip/")
# COMPOSITE_ROOT = Namespace("s3://tempest/composites/aorc/precip/")


@dataclass
class RFCInfo:
    """
    Data Property: Regional Forecast Center (RFC) names and aliases
    """

    alias: str
    name: str


RFC_INFO_LIST = [
    RFCInfo("AB", "ARKANSAS RED BASIN"),
    RFCInfo("CB", "COLORADO BASIN"),
    RFCInfo("CN", "CALIFORNIA NEVADA"),
    RFCInfo("LM", "LOWER MISSISSIPPI"),
    RFCInfo("MA", "MID ATLANTIC"),
    RFCInfo("MB", "MISSOURI BASIN"),
    RFCInfo("NC", "NORTH CENTRAL"),
    RFCInfo("NE", "NORTHEAST"),
    RFCInfo("NW", "NORTHWEST"),
    RFCInfo("OH", "OHIO"),
    RFCInfo("SE", "SOUTHEAST"),
    RFCInfo("WG", "WEST GULF"),
]
