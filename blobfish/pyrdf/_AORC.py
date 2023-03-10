from rdflib import URIRef, Namespace
from rdflib.namespace import DefinedNamespace


class AORC(DefinedNamespace):
    """
    AORC Ontology for S3 cloud mirror
    Created on 2023-01-21 16:04
    """

    """
    Classes:
    DockerImage - equivalent to DCMITTYPE.Software with property DCTERMS.hasPart
    MirrorDataset - equivalent to DCAT.Dataset with property PROV.wasGeneratedBy, DCTERMS.source, and DCTERMS.created
    MirrorURI - equivalent to DCAT.Distribution with property DCAT.downloadURL
    RFC - equivalent to FOAF.Organization with property DCTERMS.title, DCTERMS.alternative, and DCAT.landingPage
    TransferScript - equivalent to DCMITYPE.Software with property PROV.started, DCTERMS.isPartOf
    SourceDataset -> equivalent to DCAT.Dataset with property DCTERMS.accrualPeriodicity, DCTERMS.creator, and DCTERMS.temporal
    SourceURI - equivalent to DCAT.Distribution with property DCAT.downloadURL, DCAT.byteSize, DCAT.compressionFormat, DCAT.packageFormat, and DCTERMS.modified
    PrecipPartition - equivalent to DCAT.Catalog
    TransferJob - equivalent to PROV.Activity with property PROV.generated, PROV.used, PROV.wasStartedBy

    Data Properties
    hasDateCreated - equivalent to DCTERMS.created
    hasRFCAlias - equivalent to DCTERMS.alternative
    hasRFCName - equivalent to DCTERMS.title

    Object Properties
    hasCodeRepository - equivalent to DCTERMS.isVersionOf, property of CommitHash
    hasCreationScript - no equivalent, could be replaced by hasCreationActivity pointing to transfer activity which would be equivalent to PROV.wasGeneratedBy, which could then point to creationScript using PROV.wasStartedBy
    hasMirrorUri - no equivalent
    hasRFC - equivalent to DCTERMS.creator
    hasSourceUri - no equivalent, could be replaced by hasSourceDataset which would be equivalent to DCTERMS.source
    isCodeRepositoryOf - equivalent to DCTERMS.hasVersion
    isCommitHashOf - equivalent to DCTERMS.hasPart
    isRFCOf - no equivalent
    isSourceUriOf - no equivalent
    isMirrorUriOf - no equivalent, could be replaced by isMirrorDatasetOf which would be equivalent to DCTERMS.source
    isCreationScriptOf - equivalent to PROV.started
    """

    # Classes
    CodeRepository: URIRef  # -> DCAT.Catalog with DCAT.landingPage for web address, DCTERMS.license for license
    CommitHash: URIRef  # -> DCAT.Catalog with DCAT.landingPage of repo including /blob/{commitHash}, DCTERMS.identifier with commit hash, DCTERMS.isVersionOf pointing towards code repository, and DCAT.Resource recording script
    CompositeGrid: URIRef  # ->
    CompositeGridURI: URIRef  # ->
    MirrorDataset: URIRef  # -> DCAT.Dataset with DCAT.Distribution for s3 address (with format https://{bucketName}.s3-{awsRegionName}.amazonaws.com/), PROV.wasGeneratedBy pointing to PROV.Activity for transfer script execution, DCTERMS.source pointing to FTP original dataset, and DCTERMS.created for creation time
    MirrorURI: URIRef  # -> DCAT.Distribution for s3 address (with format https://{bucketName}.s3-{awsRegionName}.amazonaws.com/)
    RFC: URIRef  # -> FOAF.Organization with DCTERMS.title for full name, DCTERMS.alternative for alias, and DCAT.landingPage for URL of weather.gov page for RFC office resources (https://www.weather.gov/{alternativeAlias}/)
    Script: URIRef  # -> DCTERM.Software && DCAT.Resource && PROV.Entity which is recorded as member of DCAT.Catalog CommitHash; also pointed towards from transfer job PROV.Activity property PROV.wasStartedBy
    SourceDataset: URIRef  # -> PROV.Entity && DCAT.Dataset with DCAT.distribution for FTP NOAA address, DCTERMS.accrualPeriodicity for frequency of publication of .zip files (1 month), and is pointed towards from transfer job PROV.Activity property PROV.used
    SourceGrid: URIRef  # ->
    SourceURI: URIRef  # DCAT.Distribution for FTP NOAA address with DCAT.downloadURL, DCTERMS.modified for last date of modification, DCAT.byteSize, DCAT.compressFormat referencing zip file <https://www.iana.org/assignments/media-types/application/zip>, DCAT.packageFormat referencing netCDF file <https://publications.europa.eu/resource/authority/file-type/NETCDF>
    """___New "Classes":___
    - TransferJob: PROV.Activity with PROV.wasStartedBy pointing towards PROV.Entity for script used in transfer, PROV.used pointing towards PROV.Entity for source dataset
    - HistoricCatalog: DCAT.Catalog with DCAT.landingPage pointing towards FTP server hosting the AORC historic data directories for all RFC offices and DCAT.catalog with RFC office catalogs
    - RFCCatalog: DCAT.Catalog with DCAT.landingPage pointing towards FTP directory for all monthly temperature zip files and precip_partition directory, DCTERMS.creator referencing RFC office, DCAT.catalog referencing precip partition directory
    - PrecipPartition: DCAT.Catalog with DCTERMS.creator pointing towards RFC office organization, DCAT.landingPage pointing toward FTP directory for all monthly precip zip file source datasets, DCAT.keyword literal "precipitation", and DCAT.dataset referencing all source datsets
    """

    # Data Properties
    hasDateCreated: URIRef  # -> DCAT.created associated with mirror dataset
    hasRFCAlias: URIRef  # -> DCTERMS.alternative associated with RFC
    hasRFCName: URIRef  # -> DCTERMS.title associated with RFC
    hasRefDate: URIRef  # -> DCTERMS.temporal associated with FTP source and mirror

    # Object Properties
    hasCodeRepository: URIRef  # -> create CodeRepository as DCAT.Catalog, add property DCAT.dataset with scripts of interest to record membership
    hasCommitHash: URIRef  # -> DCTERMS.identifier linked to CodeRepository DCAT.Catalog
    hasCompositeGridURI: URIRef  # ->
    hasCreationScript: URIRef  # -> PROV.wasGeneratedBy pointing towards PROV.Activity of transfer job and with property DCTERMS.created recording creation time
    hasMirrorURI: URIRef  # -> DCAT.distribution
    hasRFC: URIRef  # -> DCTERMS.creator property associated with RFC catalog which holds all source datasets of interest
    hasSourceGrid: URIRef  # ->
    hasSourceURI: URIRef  # -> DCAT.distribution
    isCodeRepositoryOf: URIRef  # -> from code repo DCAT.Catalog, property DCTERMS.hasVersion points to DCAT.Catalog entities for commit hashes that were used
    isCommitHashOf: URIRef  # -> from commit hash DCAT.Catalog, property DCTERMS.isVersionOf points to DCAT.Catalog entity for code repository
    isCompositeGridURIOf: URIRef  # ->
    isRFCOf: URIRef  # -> query values which use DCTERMS.creator to point towards RFC FOAF.Organization instance
    isSourceGridOf: URIRef  # ->
    isSourceURIOf: URIRef  # -> query values which use DCAT.distribution to point towards source distribution DCAT.Distribution instance
    # To get mirror URI of source URI, query value using DCAT.distribution property to point towards source distribution DCAT.Distribution instance; then use retrieved source dataset value to query for entities that use the DCT.source property to point towards source dataset; then use the retrieved DCAT.Dataset instance for the mirror dataset to get its DCAT.distribution property, yielding mirror URI
    isMirrorURIOf: URIRef  # -> query value which uses DCAT.distribution to point towards mirror distribution DCAT.Distribution instance
    # To get source URI of mirror URI, query value which uses DCAT.distribution to point towards mirror distribution DCAT.Distribution instance; then use retrieved mirror DCAT.Dataset instance and get its source property; Then use the source DCAT.Dataset instance to get the distribution property, yiedling the source URI
    isCreationScriptOf: URIRef  # -> query values which use DCAT.wasStartedBy to point towards script PROV.Entity / DCAT.Dataset; then use retrieved transfer job PROV.Activity instances to query values which use the PROV.wasGeneratedBy property to point towards PROV.Activities

    _NS = Namespace("http://github.com/Dewberry/blobfish/semantics/rdf/aorc#")
