from rdflib import URIRef
from rdflib.namespace import DefinedNamespace, Namespace


class AORC(DefinedNamespace):
    # Classes
    CommandList: URIRef
    CompositeDataset: URIRef
    DockerCompose: URIRef
    DockerContainer: URIRef
    DockerImage: URIRef
    DockerProcess: URIRef
    MirrorDataset: URIRef
    RFC: URIRef
    SourceDataset: URIRef
    TranspositionDataset: URIRef
    # Object Properties
    hasRFC: URIRef
    maxPrecipitationPoint: URIRef
    normalizedBy: URIRef
    transpositionRegion: URIRef
    watershedRegion: URIRef
    # Data Properties
    cellCount: URIRef
    maxPrecipitation: URIRef
    meanPrecipitation: URIRef
    normalizedMeanPrecipitation: URIRef
    sumPrecipitation: URIRef

    _NS = Namespace("https://raw.githubusercontent.com/Dewberry/blobfish/v0.9/semantics/rdf/aorc.ttl")


IANA_APP = Namespace("https://www.iana.org/assignments/media-types/application/")
EU = Namespace("https://publications.europa.eu/resource/authority/file-type/")
LOCN = Namespace("http://www.w3.org/ns/locn#")
