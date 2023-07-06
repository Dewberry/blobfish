from classes.mirror import RFCInfo

# First available dataset in the AORC repository
FIRST_RECORD = "1979-02-01"

# FTP server host common prefix
FTP_HOST = "https://hydrology.nws.noaa.gov/pub/aorc-historic"

# RFC Shapefile URL
RFC_TAR_SHP_URL = "https://www.nohrsc.noaa.gov/data/vector/master/rfc_us.tar.gz"

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

NOAA_URL = "https://noaa.gov/"

ZARR_CURRENT_VERSION_URI = "https://github.com/zarr-developers/zarr-python/tree/v2.14.2"
