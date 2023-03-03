from rdflib import DCAT, DCTERMS, PROV, FOAF, RDF, URIRef, Namespace
from rdflib.namespace import DefinedNamespace


class AORC(DefinedNamespace):
    """
    AORC Ontology for S3 cloud mirror
    Created on 2023-01-21 16:04
    """

    # DCTERMS.

    # Classes
    CodeRepository: URIRef  # -> DCAT.Catalog with DCAT.landingPage for web address, DCTERMS.license for license, and DCAT.Dataset recording script
    CommitHash: URIRef  # -> DCAT.Catalog with DCAT.landingPage of repo including /blob/{commitHash}, DCTERMS.identifier with commit hash, and DCT.isVersionOf pointing towards code repository
    CompositeGrid: URIRef  # ->
    CompositeGridURI: URIRef  # ->
    MirrorDataset: URIRef  # -> DCAT.Dataset with DCAT.Distribution for s3 address (with format https://{bucketName}.s3-{awsRegionName}.amazonaws.com/), PROV.wasGeneratedBy pointing to PROV.Activity for transfer script execution, and DCTERMS.created for creation time
    MirrorURI: URIRef  # -> DCAT.Distribution for s3 address (with format https://{bucketName}.s3-{awsRegionName}.amazonaws.com/)
    RFC: URIRef  # -> FOAF.Organization with DCTERMS.title for full name, DCTERMS.alternative for alias, and DCAT.landingPage for URL of weather.gov page for RFC office resources (https://www.weather.gov/{alternativeAlias}/)
    Script: URIRef  # -> DCTERM.Software && DCAT.Resource && PROV.Entity which is recorded as member of DCAT.Catalog CommitHash; also pointed towards from transfer job PROV.Activity property PROV.wasStartedBy
    SourceDataset: URIRef  # -> PROV.Entity && DCAT.Dataset with DCAT.distribution for FTP NOAA address, DCTERMS.accrualPeriodicity for frequency of publication of .zip files (1 month), DCTERMS.creator referencing RFC office, and is pointed towards from transfer job PROV.Activity property PROV.used
    SourceGrid: URIRef  # ->
    SourceURI: URIRef  # DCAT.Distribution for FTP NOAA address with DCAT.downloadURL, DCTERMS.modified for last date of modification, DCAT.byteSize, DCAT.compressFormat of application/zip
    """___New "Classes":___
    - TransferJob: PROV.Activity with PROV.wasStartedBy pointing towards PROV.Entity for script used in transfer, PROV.used pointing towards PROV.Entity for source dataset
    - HistoricCatalog: DCAT.Catalog with DCAT.landingPage pointing towards FTP server hosting the AORC historic data directories for all RFC offices and DCAT.catalog with RFC office catalogs
    - RFCCatalog: DCAT.Catalog with DCAT.landingPage pointing towards FTP directory for all monthly temperature zip files and precip_partition directory, DCTERMS.creator referencing RFC office, DCAT.catalog referencing precip partition directory
    - PrecipPartition: DCAT.Catalog with DCAT.landingPage pointing toward FTP directory for all monthly precip zip file source datasets, DCAT.keyword literal "precipitation", and DCAT.dataset referencing all source datsets
    """

    # Data Properties
    hasDateCreated: URIRef  # -> PROV.atTime, used in PROV.qualifiedGeneration to reference datetime of creation
    hasRFCAlias: URIRef  # -> DCTERMS.alternative
    hasRFCName: URIRef  # -> DCTERMS.title
    hasRefDate: URIRef  # -> DCTERMS.temporal

    # Object Properties
    hasCodeRepository: URIRef  # -> create CodeRepository as DCAT.Catalog, add property DCAT.dataset with scripts of interest to record membership
    hasCommitHash: URIRef  # -> DCTERMS.identifier linked to CodeRepository DCAT.Catalog
    hasCompositeGridURI: URIRef  # ->
    hasCreationScript: URIRef  # -> PROV.wasGeneratedBy pointing towards PROV.Activity of transfer job and with property DCTERMS.created recording creation time
    hasMirrorURI: URIRef  # -> DCAT.distribution
    hasRFC: URIRef  # -> DCTERMS.creator property associated with RFC catalog which holds all source datasets of interest
    hasSourceGrid: URIRef  # ->
    hasSourceURI: URIRef  # -> query values which use DCAT.distribution property to point towards source URI DCAT.Distributions; then use retrieved source dataset value to query PROV.Activity instances which have PROV.used property pointing towards sourced dataset value; then use retrieved transfer job PROV.Activity to get property PROV.generated, which points towards mirror dataset; then use mirror dataset DCAT.distribution property to get mirror URI
    isCodeRepositoryOf: URIRef  # -> from code repo DCAT.Catalog, property DCTERMS.hasVersion points to DCAT.Catalog entities for commit hashes that were used
    isCommitHashOf: URIRef  # -> from commit hash DCAT.Catalog, property DCTERMS.isVersionOf points to DCAT.Catalog entity for code repository
    isCompositeGridURIOf: URIRef  # ->
    isRFCOf: URIRef  # -> query values which use DCTERMS.creator to point towards RFC FOAF.Organization instance
    isSourceGridOf: URIRef  # ->
    isSourceURIOf: URIRef  # -> query values which use DCAT.distribution to point towards source distribution DCAT.Distribution instance; then use retrieved source dataset values to get PROV.Activity entities that have property PROV.used pointing to source dataset; then use transfer job activity property PROV.generated to get mirror DCAT.Dataset value; then get dataset DCAT.distribution property to get mirror distribution
    isMirrorURIOf: URIRef  # -> query values which use DCAT.distribution to point towards mirror distribution DCAT.Distribution instance; then use retrieved mirror dataset values to get PROV.wasGeneratedBy property pointing towards transfer job PROV.Activity; then use transfer job PROV.Activity to get PROV.used property to get source dataset; then use source dataset DCAT.Dataset property DCAT.distribution to get source URI
    isCreationScriptOf: URIRef  # -> query values which use DCAT.wasStartedBy to point towards script PROV.Entity / DCAT.Dataset; then use retrieved transfer job PROV.Activity instances to query values which use the PROV.wasGeneratedBy property to point towards PROV.Activities

    _NS = Namespace("http://github.com/Dewberry/blobfish/semantics/rdf/aorc#")
