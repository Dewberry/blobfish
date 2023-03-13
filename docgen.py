import os
import ontospy
from ontospy.gendocs.viz.viz_html_single import HTMLVisualizer

# example usage: python docgen.py atlas14 semantics/rdf/atlas14.ttl

if __name__ == "__main__":
    args = sys.argv
    namespace = args[1]
    rdf_path = args[2]

    output_path = os.path.join(os.getcwd(), f"semantics/html/{namespace}")
    model = ontospy.Ontospy(rdf_path, verbose=True)
    model.all_classes
    v = HTMLVisualizer(model, title=f"{namespace.upper()} Mirror")
    v.build(output_path=output_path)
    v.preview()
