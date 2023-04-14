import datetime
import enum
import logging
import requests
from dataclasses import dataclass, field
from dateutil.relativedelta import relativedelta
from .const import RFCInfo, FTP_HOST


class DataFormat(enum.Enum):
    """
    Enumerated type to constrain possible data formats for inputs and outputs when loading and serializing graph data
    """

    S3 = enum.auto()
    LOCAL = enum.auto()


class FTPError(Exception):
    "FTP site does not match expected structure"


@dataclass
class DatedPaths:
    """
    Paths associated with a specific datetime period
    """

    start_date: datetime.datetime
    end_date: datetime.datetime
    paths: list[str]

    def __post_init__(self):
        # Alter end date from referring to first hour of end date (00) to last hour of end date (23)
        # in order to capture all data from covered time period for dataset
        self.end_date = self.end_date.replace(hour=23)


@dataclass
class TranspositionStart:
    """
    Structured class mimicking format of "start" property in transposition documentation
    """

    datetime: str
    timestamp: int
    calendar_year: int
    water_year: int
    season: str


@dataclass
class TranspositionStatistics:
    """
    Structured class mimicking format of "stats" property in transposition documentation
    """

    count: int
    mean: float
    max: float
    min: float
    sum: float
    norm_mean: float


@dataclass
class TranspositionMetadata:
    """
    Structured class mimicking format of "metadata" property in transposition documentation
    """

    source: str
    watershed_name: str
    transposition_domain_name: str
    watershed_source: str
    transposition_domain_source: str
    create_time: str


@dataclass
class TranspositionGeometry:
    """
    Structured class mimicking format of "geom" property in transposition documentation
    """

    x_delta: int
    y_delta: int
    center_x: float
    center_y: float


class TranspositionDocumentation:
    """
    Structured class to add type control to transposition documentation ingestion
    """

    def __init__(self, start: dict, duration: int, stats: dict, metadata: dict, geom: dict) -> None:
        self.start = TranspositionStart(**start)
        self.duration = duration
        self.stats = TranspositionStatistics(**stats)
        self.metadata = TranspositionMetadata(**metadata)
        self.geom = TranspositionGeometry(**geom)

    def format_start_date(self) -> datetime.datetime:
        return datetime.datetime.strptime(self.start.datetime, "%Y-%m-%d %H:%M:%S")


@dataclass
class TemplateInputs:
    """
    Structured class with required fields for creating a string which can be loaded as a JSON-LD metadata document
    """

    watershedName: str
    startDate: str
    dssModified: str
    doiUrl: str
    centerY: str
    centerX: str
    startDateIsoformat: str
    duration: int
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
    scriptPath: str
    dockerPath: str


@dataclass
class CompositeConfig:
    """
    Configuration setup used to control how data is loaded for composite data metadata creation and how it is output, either to local path or s3 path
    """

    output_format: DataFormat
    out_dir: str
    out_path: str
    extended: bool = False
    input_format: DataFormat | None = None
    in_dir: str | None = None
    in_pattern: str | None = None


@dataclass
class CompletedCompositeMetadata:
    """
    Completed composite metadata, used to create RDF metadata for the composite dataset
    """

    start_time: str
    end_time: str
    docker_image_url: str
    members: str
    composite_last_modified: str
    composite_s3_directory: str
    composite_script: str
    public_uri: str = field(init=False)

    def __post_init__(self):
        bucket, *filename_parts = self.composite_s3_directory.replace("s3://", "").split("/")
        filename = "/".join(filename_parts)
        self.public_uri = f"https://{bucket}.s3.amazonaws.com/{filename}"
        self.composite_script = self.composite_script.replace("/", "_")

    def get_member_datasets(self) -> list[str]:
        return self.members.split(",")


@dataclass
class SourceURLObject:
    """
    Collects the date, RFC information, and parts of the source URL from the source data for later use in metadata creation
    """

    rfc_catalog_relative_url: str
    precip_partition_relative_url: str
    source_relative_url: str
    date: datetime.datetime
    rfc: RFCInfo


@dataclass
class BaseTransferMetadata:
    """Class to package metadata available using presumed FTP structure and information provided to TransferHandler object"""

    rfc_name: str
    rfc_alias: str
    rfc_catalog_uri: str
    precip_partition_uri: str
    source_uri: str
    mirror_uri: str
    ref_date: str
    docker_image_url: str
    mirror_script: str
    aorc_historic_uri: str = field(init=False)

    def __post_init__(self):
        """AORC historic URI is presumed to be the same as constant variable FTP_HOST"""
        self.aorc_historic_uri = FTP_HOST


@dataclass
class TransferMetadata(BaseTransferMetadata):
    """Class to package metadata available after the source file has been queried with an HTTP request"""

    aorc_historic_uri: str
    source_last_modified: str
    source_bytes: str


@dataclass
class TransferContext:
    """
    Contextual information which can be used to describe the process of the transfer from source to s3 mirror for a specific mirrored dataset
    """

    relative_mirror_uri: str
    metadata: BaseTransferMetadata


@dataclass
class CompletedTransferMetadata(TransferMetadata):
    """
    Metadata on transfer from source to s3 mirror that is complete, with details pulled from successful s3 upload
    """

    mirror_last_modified: str
    bucket: str
    mirror_public_uri: str = field(init=False)
    ref_end_date: str = field(init=False)
    rfc_office_uri: str = field(init=False)

    def __post_init__(self):
        """Completes public URI, calculates datetime object from the reference date in the file name, and formats information to be compatible for RDF serialization"""
        # Create public s3 address
        public_uri = f"https://{self.bucket}.s3.amazonaws.com/{self.mirror_uri}"
        self.mirror_uri = f"s3://{self.bucket}/{self.mirror_uri}"
        self.mirror_public_uri = public_uri

        # Calculate and format end duration for dataset
        ref_end_datetime = (
            datetime.datetime.strptime(self.ref_date, "%Y-%m-%d")
            + relativedelta(months=1, day=1)
            - datetime.timedelta(days=1)
        )
        self.ref_end_date = ref_end_datetime.strftime("%Y-%m-%d")

        # Format source last modified property
        if self.source_last_modified:
            self.source_last_modified = datetime.datetime.strptime(
                self.source_last_modified, "%a, %d %b %Y %H:%M:%S %Z"
            ).isoformat()

        # Format transfer script to make it parseable
        self.mirror_script = self.mirror_script.replace("/", "_")

        # Get validated page for RFC office
        self.rfc_office_uri = self.__validate_rfc_office_page()
        logging.info(f"Metadata completed for {self.mirror_uri}")

    def __validate_rfc_office_page(self) -> str:
        """Validates that the URL created by this function as a representation of the RFC office which is associated with a source dataset is valid

        Raises:
            requests.exceptions.RequestException: Raised when URL is not valid

        Returns:
            str: Validated RFC office URL
        """
        url = f"https://www.weather.gov/{self.rfc_alias.lower()}rfc"
        resp = requests.get(url, allow_redirects=True)
        if resp.ok:
            return url
        else:
            logging.error(f"rfc homepage url {url} not valid")
            raise requests.exceptions.RequestException
