from rdflib import URIRef
from rdflib.namespace import DefinedNamespace, Namespace


class AORC(DefinedNamespace):
    # Classes
    CompositeDataset: URIRef
    CompositeDistribution: URIRef
    CompositeJob: URIRef
    CompositeScript: URIRef
    DockerImage: URIRef
    MirrorDataset: URIRef
    MirrorDistribution: URIRef
    PrecipPartition: URIRef
    RFC: URIRef
    SourceDataset: URIRef
    SourceDistribution: URIRef
    TransferJob: URIRef
    TransferScript: URIRef
    TranspositionJob: URIRef
    TranspositionScript: URIRef
    TranspositionStatistics: URIRef

    # Data Properties
    cellCount: URIRef
    hasRFCAlias: URIRef
    hasRFCName: URIRef
    maximumPrecipitation: URIRef
    meanPrecipitation: URIRef
    minimumPrecipitation: URIRef
    normalizedMeanPrecipitation: URIRef
    sumPrecipitation: URIRef

    # Object Properties
    createdComposite: URIRef
    hasCompositeDataset: URIRef
    hasDockerImage: URIRef
    hasCompositeScript: URIRef
    hasMirrorDataset: URIRef
    hasRFC: URIRef
    hasSourceDataset: URIRef
    hasTransferScript: URIRef
    hasTranspositionScript: URIRef
    isCompositeOf: URIRef
    isCompositeScriptOf: URIRef
    isDockerImageOf: URIRef
    isMirrorDatasetOf: URIRef
    isRFCOf: URIRef
    isSourceDatasetOf: URIRef
    isTransferScriptOf: URIRef
    transferred: URIRef
    wasCompositedBy: URIRef
    wasTransferredBy: URIRef

    _NS = Namespace("https://github.com/Dewberry/blobfish/blob/aorc_refactor/semantics/rdf/aorc.ttl#")
