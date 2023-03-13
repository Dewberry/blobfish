from rdflib import URIRef
from rdflib.namespace import DefinedNamespace, Namespace


class AORC(DefinedNamespace):
    # Classes
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
    hasDockerImage: URIRef
    hasMirrorDataset: URIRef
    hasRFC: URIRef
    hasSourceDataset: URIRef
    hasTransferScript: URIRef
    transferred: URIRef
    isDockerImageOf: URIRef
    isMirrorDatasetOf: URIRef
    isRFCOf: URIRef
    isSourceDatasetOf: URIRef
    isTransferScriptOf: URIRef
    wasTransferredBy: URIRef

    _NS = Namespace(
        "https://htmlpreview.github.io/?https://github.com/Dewberry/blobfish/blob/aorc/semantics/html/aorc/index.html#"
    )
