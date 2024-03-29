import sys
from rdflib import Graph
from rdflib.namespace import OWL
from datetime import datetime

# example usage: python rdf2py.py atlas14 semantics/rdf/atlas14.ttl

if __name__ == "__main__":
    args = sys.argv
    namespace = args[1]
    rdf_path = args[2]

    pyrdf_path = "blobfish/pyrdf/_{0}.py".format(namespace.upper())
    print(pyrdf_path, rdf_path.lower())

    g = Graph()
    g.parse(rdf_path.lower(), format="ttl")

    with open(pyrdf_path, "w") as f:

        header = '''\
        \nfrom rdflib import URIRef, Namespace \
        \nfrom rdflib.namespace import DefinedNamespace \
        \n
        \nclass {0}(DefinedNamespace): \
        \n\t""" \
        \n\t{0} Ontology for S3 cloud mirror \
        \n\tCreated on {1} \
        \n\t"""\n \
        '''.format(
            namespace.upper(), datetime.now().strftime(format="%Y-%m-%d %H:%M")
        )

        print(header, file=f)
        print("\t#Classes", file=f)
        for s, _, _ in g.triples((None, None, OWL.Class)):
            print(f"\t{s.fragment}: URIRef", file=f)

        print("\n\t#Data Properties", file=f)
        for s, _, _ in g.triples((None, None, OWL.DatatypeProperty)):
            print(f"\t{s.fragment}: URIRef", file=f)

        print("\n\t#Object Properties", file=f)
        for s, _, _ in g.triples((None, None, OWL.ObjectProperty)):
            print(f"\t{s.fragment}: URIRef", file=f)

        print(
            """\n\t_NS = Namespace("http://github.com/Dewberry/blobfish/semantics/rdf/{0}#")""".format(namespace),
            file=f,
        )
