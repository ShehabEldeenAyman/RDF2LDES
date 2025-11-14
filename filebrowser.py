from rdflib import Graph, Namespace, Literal, URIRef, BNode
from rdflib.namespace import XSD, RDF
import os
import time

base_path = "./data"

AS = Namespace("https://www.w3.org/ns/activitystreams#")
LDES = Namespace("https://w3id.org/ldes#")
TREE = Namespace("https://w3id.org/tree#")

eventstream_uri = URIRef("https://shehabeldeenayman.github.io/Mol_sluis_Dessel_Usecase/")
view_uri = URIRef("https://shehabeldeenayman.github.io/Mol_sluis_Dessel_Usecase/ldes/")

def create_base_graph():
    g = Graph()
    g.bind("as", AS)
    g.bind("ldes", LDES)
    g.bind("tree", TREE)
    g.bind("xsd", XSD)

    retention_bn = BNode()

    # Base LDES metadata
    g.add((eventstream_uri, RDF.type, LDES.EventStream))
    g.add((eventstream_uri, LDES.retentionPolicy, retention_bn))
    g.add((eventstream_uri, LDES.timestampPath, AS.published))
    g.add((eventstream_uri, LDES.versionCreateObject, AS.Create))
    g.add((eventstream_uri, LDES.versionDeleteObject, AS.Delete))
    g.add((eventstream_uri, LDES.versionOfPath, AS.object))
    g.add((eventstream_uri, TREE.view, view_uri))

    # retentionPolicy blank node
    g.add((retention_bn, RDF.type, LDES.LatestVersionSubset))
    g.add((retention_bn, LDES.amount, Literal(1, datatype=XSD.integer)))

    return g

def add_year_relations(g, year):
    # TREE relations for each year
    bn_ge = BNode()
    bn_lt = BNode()

    # >= year
    g.add((bn_ge, RDF.type, TREE.GreaterThanOrEqualToRelation))
    g.add((bn_ge, TREE.node, URIRef(f"{view_uri}{year}/")))
    g.add((bn_ge, TREE.path, AS.published))
    g.add((bn_ge, TREE.value, Literal(year, datatype=XSD.gYear)))

    # < year+1
    g.add((bn_lt, RDF.type, TREE.LessThanRelation))
    g.add((bn_lt, TREE.node, URIRef(f"{view_uri}{year}/")))
    g.add((bn_lt, TREE.path, AS.published))
    g.add((bn_lt, TREE.value, Literal(year + 1, datatype=XSD.gYear)))

def main():
    start = time.perf_counter()

    g = create_base_graph()

    # Detect all year folders under ./data
    years = []
    for name in os.listdir(base_path):
        full = os.path.join(base_path, name)
        if os.path.isdir(full) and name.isdigit() and len(name) == 4:
            years.append(int(name))

    years = sorted(years)

    print("Detected years:", years)

    # Add TREE relations for all detected years
    for y in years:
        add_year_relations(g, y)

    # Save combined file
    output_file = os.path.join(base_path, "all_years_metadata.trig")
    g.serialize(destination=output_file, format="trig")

    print("Saved:", output_file)
    print(f"Completed in {time.perf_counter() - start:.2f} seconds.")

if __name__ == "__main__":
    main()
