from rdflib import URIRef
from rdflib.namespace import DefinedNamespace, Namespace


class AORC(DefinedNamespace):
    # Classes
    CompositeDataset: URIRef
    DockerImage: URIRef
    MirrorDataset: URIRef
    MirrorDistribution: URIRef
    PrecipPartition: URIRef
    RFC: URIRef
    SourceDataset: URIRef
    SourceDistribution: URIRef
    TransferJob: URIRef
    TransferScript: URIRef

    # Data Properties
    hasRFCAlias: URIRef
    hasRFCName: URIRef

    # Object Properties
    hasCompositeDataset: URIRef
    hasDockerImage: URIRef
    hasMirrorDataset: URIRef
    hasRFC: URIRef
    hasSourceDataset: URIRef
    hasTransferScript: URIRef
    isCompositeOf: URIRef
    isDockerImageOf: URIRef
    isMirrorDatasetOf: URIRef
    isRFCOf: URIRef
    isSourceDatasetOf: URIRef
    isTransferScriptOf: URIRef
    transferred: URIRef
    wasTransferredBy: URIRef

    _NS = Namespace("https://github.com/Dewberry/blobfish/blob/aorc_refactor/semantics/rdf/mapped_aorc.ttl#")
