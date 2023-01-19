        
from rdflib import URIRef, Namespace         
from rdflib.namespace import DefinedNamespace         

        
class AORC(DefinedNamespace):         
	"""         
	AORC Ontology for S3 cloud mirror         
	Created on 2023-01-19 09:14         
	"""
         
	#Classes
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

	#Data Properties
	hasDateCreated: URIRef
	hasRFCAlias: URIRef
	hasRFCName: URIRef
	hasRefDate: URIRef

	#Object Properties
	IsMirrorURIOf: URIRef
	hasCompositeGridURI: URIRef
	hasMirrorURI: URIRef
	hasRFC: URIRef
	hasSourceGrid: URIRef
	hasSourceURI: URIRef
	isCompositeGridURIOf: URIRef
	isRFCOf: URIRef
	isSourceGridOf: URIRef
	isSourceURIOf: URIRef
	wasCreatedBy: URIRef

	_NS = Namespace("http://github.com/Dewberry/blobfish/semantics/rdf/aorc#")
