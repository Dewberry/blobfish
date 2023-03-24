""" Script to write an ontology that is useful for dealing with AORC data sources """
import rdflib
from dataclasses import dataclass
from typing import List
from rdflib import RDFS, RDF, OWL, DCAT, DCTERMS, DCMITYPE, PROV, FOAF, XSD, URIRef, Literal, BNode
from rdflib.collection import Collection

from ..pyrdf._AORC import AORC


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
            AORC.CompositeDataset,
            DCAT.Dataset,
            "A CONUS netCDF dataset created by stitching together AORC data from all RFC offices for a single hour",
        ),
        AORCParentRelation(
            AORC.CompositeDistribution,
            DCAT.Distribution,
            "The access point for the .zarr directory containing the composite dataset of all RFC office data for an hour",
        ),
        AORCParentRelation(
            AORC.CompositeJob,
            PROV.Activity,
            "The execution of the composite script on the docker image instance which generated the composite dataset(s)",
        ),
        AORCParentRelation(
            AORC.CompositeScript,
            DCMITYPE.Software,
            "A script contained within the docker image which was executed in order to composite AORC dataset(s)",
        ),
        AORCParentRelation(
            AORC.DockerImage,
            DCMITYPE.Software,
            "A docker image hosted on a repository that can be used to generate mirror datasets",
        ),
        AORCParentRelation(
            AORC.MirrorDataset,
            DCAT.Dataset,
            "An AORC dataset that has been copied from its original location on NOAA servers to s3",
        ),
        AORCParentRelation(
            AORC.MirrorDistribution, DCAT.Distribution, "The access point for the mirrored dataset on s3"
        ),
        AORCParentRelation(
            AORC.PrecipPartition,
            DCAT.Catalog,
            "The directory which directly holds all source datasets which are published by the creator RFC office associated with the PrecipPartition",
        ),
        AORCParentRelation(
            AORC.RFC,
            FOAF.Organization,
            "The River Forecast Center which is associated with the coverage area for a catalog of precipitation data",
        ),
        AORCParentRelation(
            AORC.SourceDataset, DCAT.Dataset, "The AORC dataset in its original location on the NOAA servers"
        ),
        AORCParentRelation(
            AORC.SourceDistribution, DCAT.Distribution, "The access point for the mirrored dataset on s3"
        ),
        AORCParentRelation(
            AORC.TransferJob,
            PROV.Activity,
            "The execution of the transfer script on the docker image instance which generated the mirror dataset(s)",
        ),
        AORCParentRelation(
            AORC.TransferScript,
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
            AORC.hasRFCAlias,
            DCTERMS.alternative,
            "The 2 character alias assigned to an RFC office (ex: 'LM' for Lower Mississippi River Forecast Office)",
        ),
        AORCDatatypePropertyRelation(
            AORC.hasRFCName,
            DCTERMS.title,
            "The full region name for the RFC office (ex: 'LOWER MISSISSIPPI for Lower Mississippi River Forecast Center)",
        ),
    ]
    for relation_object in data_properties_to_assign:
        graph.add((relation_object.aorc_datatype_property, RDF.type, OWL.DatatypeProperty))
        graph.add((relation_object.aorc_datatype_property, OWL.equivalentProperty, relation_object.equivalent_property))
        graph.add(
            (
                relation_object.aorc_datatype_property,
                RDFS.comment,
                Literal(relation_object.comment, datatype=XSD.string),
            )
        )


def define_object_properties(graph: rdflib.Graph) -> None:
    # Define the object properties
    for prop in [
        ObjectPropertyDescription(
            AORC.createdComposite,
            "Indicates the job that was responsible for the creation of the subject composite image",
            AORC.CompositeJob,
            AORC.CompositeDataset,
        ),
        ObjectPropertyDescription(
            AORC.hasCompositeDataset,
            "Indicates that the subject dataset was used in the creation of the object composite dataset",
            DCAT.Dataset,
            AORC.CompositeDataset,
        ),
        ObjectPropertyDescription(
            AORC.hasDockerImage,
            "Indicates what docker image to which the software belongs",
            DCMITYPE.Software,
            AORC.DockerImage,
        ),
        ObjectPropertyDescription(
            AORC.hasCompositeScript,
            "Indicates what scripts belong to the software",
            DCMITYPE.Software,
            AORC.CompositeScript,
        ),
        ObjectPropertyDescription(
            AORC.hasMirrorDataset, "Indicates what mirror dataset the subject source dataset was used to generate"
        ),
        ObjectPropertyDescription(
            AORC.hasRFC,
            "Indicates the RFC Office responsible for the publication of the subject data resource",
            DCAT.Resource,
            AORC.RFC,
        ),
        ObjectPropertyDescription(
            AORC.hasSourceDataset,
            "Indicates the origin of the subject mirrored dataset",
            AORC.MirrorDataset,
            AORC.SourceDataset,
        ),
        ObjectPropertyDescription(
            AORC.hasTransferScript,
            "Indicates what scripts belong to the software",
            DCMITYPE.Software,
            AORC.TransferScript,
        ),
        ObjectPropertyDescription(
            AORC.isCompositeOf,
            "Indicates what datasets were combined to create the subject composite dataset",
            AORC.CompositeDataset,
            DCAT.Dataset,
        ),
        ObjectPropertyDescription(
            AORC.isDockerImageOf,
            "Indicates what software belong to the docker image",
        ),
        ObjectPropertyDescription(AORC.isCompositeScriptOf, "Indicates to what software the script belongs"),
        ObjectPropertyDescription(
            AORC.isMirrorDatasetOf,
            "Indicates the origin of the subject mirrored dataset",
            AORC.MirrorDataset,
            AORC.SourceDataset,
        ),
        ObjectPropertyDescription(
            AORC.isRFCOf, "Indicates the data resources that have been published by the subject RFC Office"
        ),
        ObjectPropertyDescription(
            AORC.isSourceDatasetOf, "Indicates what mirror dataset the subject source dataset was used to generate"
        ),
        ObjectPropertyDescription(AORC.isTransferScriptOf, "Indicates to what software the script belongs"),
        ObjectPropertyDescription(
            AORC.transferred,
            "Indicates the mirror dataset that the subject job transferred",
            AORC.TransferJob,
            AORC.MirrorDataset,
        ),
        ObjectPropertyDescription(
            AORC.wasCompositedBy, "Indicates the job that was responsible for creating the subject composite dataset"
        ),
        ObjectPropertyDescription(
            AORC.wasTransferredBy,
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
    graph.add((AORC.createdComposite, RDFS.subPropertyOf, PROV.generated))
    graph.add((AORC.isCompositeOf, RDFS.subPropertyOf, DCTERMS.source))
    graph.add((AORC.hasCompositeDataset, OWL.inverseOf, AORC.isCompositeOf))
    graph.add((AORC.hasCompositeScript, RDFS.subPropertyOf, DCTERMS.hasPart))
    graph.add((AORC.isCompositeScriptOf, RDFS.subPropertyOf, DCTERMS.isPartOf))
    graph.add((AORC.isCompositeScriptOf, OWL.inverseOf, AORC.hasCompositeScript))
    graph.add((AORC.hasDockerImage, RDFS.subPropertyOf, DCTERMS.isPartOf))
    graph.add((AORC.isDockerImageOf, RDFS.subPropertyOf, DCTERMS.hasPart))
    graph.add((AORC.isDockerImageOf, OWL.inverseOf, AORC.hasDockerImage))
    graph.add((AORC.hasSourceDataset, RDFS.subPropertyOf, DCTERMS.source))
    graph.add((AORC.isSourceDatasetOf, OWL.inverseOf, AORC.hasSourceDataset))
    graph.add((AORC.hasMirrorDataset, OWL.inverseOf, AORC.hasSourceDataset))
    graph.add((AORC.isMirrorDatasetOf, RDFS.subPropertyOf, DCTERMS.source))
    graph.add((AORC.hasRFC, RDFS.subPropertyOf, DCTERMS.creator))
    graph.add((AORC.isRFCOf, OWL.inverseOf, AORC.hasRFC))
    graph.add((AORC.hasTransferScript, RDFS.subPropertyOf, DCTERMS.hasPart))
    graph.add((AORC.isTransferScriptOf, RDFS.subPropertyOf, DCTERMS.isPartOf))
    graph.add((AORC.isTransferScriptOf, OWL.inverseOf, AORC.hasTransferScript))
    graph.add((AORC.transferred, RDFS.subPropertyOf, PROV.generated))
    graph.add((AORC.wasCompositedBy, RDFS.subPropertyOf, PROV.wasGeneratedBy))
    graph.add((AORC.wasCompositedBy, OWL.inverseOf, AORC.createdComposite))
    graph.add((AORC.wasTransferredBy, RDFS.subPropertyOf, PROV.wasGeneratedBy))
    graph.add((AORC.wasTransferredBy, OWL.inverseOf, AORC.transferred))


def disjoint_classes(graph: rdflib.Graph):
    aorc_classes = [
        AORC.CompositeDataset,
        AORC.CompositeDistribution,
        AORC.CompositeScript,
        AORC.CompositeJob,
        AORC.DockerImage,
        AORC.MirrorDataset,
        AORC.MirrorDistribution,
        AORC.PrecipPartition,
        AORC.RFC,
        AORC.SourceDataset,
        AORC.SourceDistribution,
        AORC.TransferJob,
        AORC.TransferScript,
    ]
    list_item = BNode()
    Collection(graph, list_item, aorc_classes)
    disjoint_item = BNode()
    graph.add((disjoint_item, RDF.type, OWL.AllDisjointClasses))
    graph.add((disjoint_item, OWL.members, list_item))


def create_graph(output_file: str, format: str = "ttl") -> None:
    # Create new graph object, bind prefixes
    g = rdflib.Graph()
    g.bind("aorc", AORC)
    g.bind("rdf", RDF)
    g.bind("rdfs", RDFS)
    g.bind("owl", OWL)

    # Define namespace as new ontology
    namespace_uri = URIRef(AORC._NS)
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
    create_graph("semantics/rdf/mapped_aorc.ttl")
