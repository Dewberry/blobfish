""" Script to write an ontology that is useful for dealing with AORC data sources """
import rdflib
from dataclasses import dataclass
from typing import List
from rdflib import RDFS, RDF, OWL, DCAT, DCTERMS, DCMITYPE, PROV, FOAF, XSD, URIRef, Literal, BNode
from rdflib.collection import Collection

from ..pyrdf._new import newAorc


@dataclass
class AORCParentRelation:
    aorc_class: URIRef
    parent_class: URIRef
    comment: str


@dataclass
class AORCDatatypePropertyRelation:
    aorc_datatype_property: URIRef
    equivalent_property: URIRef
    comment: str


@dataclass
class ObjectPropertyDescription:
    aorc_object_property: URIRef
    comment: str
    domain: URIRef | None = None
    range: URIRef | None = None


def define_subclasses(graph: rdflib.Graph) -> None:
    # Define classes of AORC namespace as subclasses of existing ontologies which they closely resemble
    subclass_comment_list = [
        AORCParentRelation(
            newAorc.DockerImage,
            DCMITYPE.Software,
            "A docker image hosted on a repository that can be used to generate mirror datasets",
        ),
        AORCParentRelation(
            newAorc.MirrorDataset,
            DCAT.Dataset,
            "An AORC dataset that has been copied from its original location on NOAA servers to s3",
        ),
        AORCParentRelation(
            newAorc.MirrorDistribution, DCAT.Distribution, "The access point for the mirrored dataset on s3"
        ),
        AORCParentRelation(
            newAorc.PrecipPartition,
            DCAT.Catalog,
            "The directory which directly holds all source datasets which are published by the creator RFC office associated with the PrecipPartition",
        ),
        AORCParentRelation(
            newAorc.RFC,
            FOAF.Organization,
            "The River Forecast Center which is associated with the coverage area for a catalog of precipitation data",
        ),
        AORCParentRelation(
            newAorc.SourceDataset, DCAT.Dataset, "The AORC dataset in its original location on the NOAA servers"
        ),
        AORCParentRelation(
            newAorc.SourceDistribution, DCAT.Distribution, "The access point for the mirrored dataset on s3"
        ),
        AORCParentRelation(
            newAorc.TransferJob,
            PROV.Activity,
            "The execution of the transfer script on the docker image instance which generated the mirror dataset(s)",
        ),
        AORCParentRelation(
            newAorc.TransferScript,
            DCMITYPE.Software,
            "A script contained within the docker image which was executed in order to mirror the dataset(s)",
        ),
    ]
    for relation_object in subclass_comment_list:
        graph.add((relation_object.aorc_class, RDF.type, OWL.Class))
        graph.add((relation_object.aorc_class, RDF.type, RDFS.Class))
        graph.add((relation_object.aorc_class, RDFS.subClassOf, relation_object.parent_class))
        graph.add(
            (
                relation_object.aorc_class,
                RDFS.comment,
                Literal(
                    relation_object.comment,
                    datatype=XSD.string,
                ),
            )
        )


def define_datatype_properties(graph: rdflib.Graph) -> None:
    # Define the data properties using equivalent properties
    data_properties_to_assign = [
        AORCDatatypePropertyRelation(
            newAorc.hasRFCAlias,
            DCTERMS.alternative,
            "The 2 character alias assigned to an RFC office (ex: 'LM' for Lower Mississippi River Forecast Office)",
        ),
        AORCDatatypePropertyRelation(
            newAorc.hasRFCName,
            DCTERMS.title,
            "The full region name for the RFC office (ex: 'LOWER MISSISSIPPI for Lower Mississippi River Forecast Center)",
        ),
    ]
    for relation_object in data_properties_to_assign:
        graph.add((relation_object.aorc_datatype_property, RDF.type, OWL.DatatypeProperty))
        graph.add((relation_object.aorc_datatype_property, OWL.equivalentProperty, relation_object.equivalent_property))
        graph.add((relation_object.aorc_datatype_property, RDFS.comment, Literal(relation_object.comment, datatype=XSD.string)))


def define_object_properties(graph: rdflib.Graph) -> None:
    # Define the object properties
    for prop in [
        ObjectPropertyDescription(
            newAorc.hasDockerImage,
            "Indicates what docker image to which the software belongs",
            DCMITYPE.Software,
            newAorc.DockerImage,
        ),
        ObjectPropertyDescription(
            newAorc.isDockerImageOf,
            "Indicates what software belong to the docker image",
        ),
        ObjectPropertyDescription(
            newAorc.hasSourceDataset,
            "Indicates the origin of the subject mirrored dataset",
            newAorc.MirrorDataset,
            newAorc.SourceDataset,
        ),
        ObjectPropertyDescription(
            newAorc.isSourceDatasetOf, "Indicates what mirror dataset the subject source dataset was used to generate"
        ),
        ObjectPropertyDescription(
            newAorc.hasMirrorDataset, "Indicates what mirror dataset the subject source dataset was used to generate"
        ),
        ObjectPropertyDescription(
            newAorc.isMirrorDatasetOf,
            "Indicates the origin of the subject mirrored dataset",
            newAorc.MirrorDataset,
            newAorc.SourceDataset,
        ),
        ObjectPropertyDescription(
            newAorc.hasRFC,
            "Indicates the RFC Office responsible for the publication of the subject data resource",
            DCAT.Resource,
            newAorc.RFC,
        ),
        ObjectPropertyDescription(
            newAorc.isRFCOf, "Indicates the data resources that have been published by the subject RFC Office"
        ),
        ObjectPropertyDescription(
            newAorc.hasTransferScript,
            "Indicates what scripts belong to the software",
            DCMITYPE.Software,
            newAorc.TransferScript,
        ),
        ObjectPropertyDescription(newAorc.isTransferScriptOf, "Indicates to what software the script belongs"),
        ObjectPropertyDescription(
            newAorc.transferred,
            "Indicates the mirror dataset that the subject job transferred",
            newAorc.TransferJob,
            newAorc.MirrorDataset,
        ),
        ObjectPropertyDescription(
            newAorc.wasTransferredBy,
            "Indicates the job that was responsible for transferring the subject mirror dataset",
        ),
    ]:
        graph.add((prop.aorc_object_property, RDF.type, OWL.ObjectProperty))
        graph.add((prop.aorc_object_property, RDF.type, RDF.Property))
        graph.add((prop.aorc_object_property, RDFS.comment, Literal(prop.comment, datatype=XSD.string)))
        if prop.domain:
            graph.add((prop.aorc_object_property, RDFS.domain, prop.domain))
        if prop.range:
            graph.add((prop.aorc_object_property, RDFS.range, prop.range))

    # Relate object properties to existing properties
    graph.add((newAorc.hasDockerImage, RDFS.subPropertyOf, DCTERMS.isPartOf))
    graph.add((newAorc.isDockerImageOf, RDFS.subPropertyOf, DCTERMS.hasPart))
    graph.add((newAorc.isDockerImageOf, OWL.inverseOf, newAorc.hasDockerImage))
    graph.add((newAorc.hasSourceDataset, RDFS.subPropertyOf, DCTERMS.source))
    graph.add((newAorc.isSourceDatasetOf, OWL.inverseOf, newAorc.hasSourceDataset))
    graph.add((newAorc.hasMirrorDataset, OWL.inverseOf, newAorc.hasSourceDataset))
    graph.add((newAorc.isMirrorDatasetOf, RDFS.subPropertyOf, DCTERMS.source))
    graph.add((newAorc.hasRFC, RDFS.subPropertyOf, DCTERMS.creator))
    graph.add((newAorc.isRFCOf, OWL.inverseOf, newAorc.hasRFC))
    graph.add((newAorc.hasTransferScript, RDFS.subPropertyOf, DCTERMS.hasPart))
    graph.add((newAorc.isTransferScriptOf, RDFS.subPropertyOf, DCTERMS.isPartOf))
    graph.add((newAorc.isTransferScriptOf, OWL.inverseOf, newAorc.hasTransferScript))
    graph.add((newAorc.transferred, RDFS.subPropertyOf, PROV.generated))
    graph.add((newAorc.wasTransferredBy, RDFS.subPropertyOf, PROV.wasGeneratedBy))
    graph.add((newAorc.wasTransferredBy, OWL.inverseOf, newAorc.transferred))


def disjoint_classes(graph: rdflib.Graph):
    aorc_classes = [
        newAorc.DockerImage,
        newAorc.MirrorDataset,
        newAorc.MirrorDistribution,
        newAorc.PrecipPartition,
        newAorc.RFC,
        newAorc.SourceDataset,
        newAorc.SourceDistribution,
        newAorc.TransferJob,
        newAorc.TransferScript,
    ]
    list_item = BNode()
    Collection(graph, list_item, aorc_classes)
    disjoint_item = BNode()
    graph.add((disjoint_item, RDF.type, OWL.AllDisjointClasses))
    graph.add((disjoint_item, OWL.members, list_item))


def create_graph(output_file: str, format: str = "ttl") -> None:
    # Create new graph object, bind prefixes
    g = rdflib.Graph()
    g.bind("aorc", newAorc)
    g.bind("rdf", RDF)
    g.bind("rdfs", RDFS)
    g.bind("owl", OWL)

    # Define namespace as new ontology
    namespace_uri = URIRef(newAorc._NS)
    g.add((namespace_uri, RDF.type, OWL.Ontology))

    # Define AORC classes as subclasses of well-established ontologies they resemble
    define_subclasses(g)

    # Define AORC datatype properties
    define_datatype_properties(g)

    # Define AORC object properties
    define_object_properties(g)

    # Define all classes as disjoint
    disjoint_classes(g)

    g.serialize(output_file, format=format)


if __name__ == "__main__":
    create_graph("logs/new_ont.ttl")
