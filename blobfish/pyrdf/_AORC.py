from rdflib import URIRef, Namespace
from rdflib.namespace import DefinedNamespace


class AORC(DefinedNamespace):
    """
    AORC Ontology for S3 cloud mirror
    Created on 2023-01-21 16:04
    """

    # Classes
    CodeRepository: URIRef
    CommitHash: URIRef
    CompositeGrid: URIRef
    CompositeGridURI: URIRef
    MirrorURI: URIRef
    RFC: URIRef
    Script: URIRef
    SourceDataset: URIRef
    SourceGrid: URIRef
    SourceURI: URIRef

    # Data Properties
    hasDateCreated: URIRef
    hasRFCAlias: URIRef
    hasRFCName: URIRef
    hasRefDate: URIRef

    # Object Properties
    isMirrorURIOf: URIRef
    hasCodeRepositroy: URIRef
    hasCommitHash: URIRef
    hasCompositeGridURI: URIRef
    hasCreationScript: URIRef
    hasMirrorURI: URIRef
    hasRFC: URIRef
    hasSourceGrid: URIRef
    hasSourceURI: URIRef
    isCodeRepositoryOf: URIRef
    isCommitHashOf: URIRef
    isCompositeGridURIOf: URIRef
    isRFCOf: URIRef
    isSourceGridOf: URIRef
    isSourceURIOf: URIRef

    _NS = Namespace("http://github.com/Dewberry/blobfish/semantics/rdf/aorc#")
