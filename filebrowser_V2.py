import os
from pathlib import Path
from rdflib import Graph, Namespace, Literal, URIRef, BNode
from rdflib.namespace import XSD, RDF
import time
from datetime import date, timedelta


directory = "./data/"
AS = Namespace("https://www.w3.org/ns/activitystreams#")
LDES = Namespace("https://w3id.org/ldes#")
TREE = Namespace("https://w3id.org/tree#")
eventstream_uri = URIRef("https://shehabeldeenayman.github.io/Mol_sluis_Dessel_Usecase/")
view_uri = URIRef("https://shehabeldeenayman.github.io/Mol_sluis_Dessel_Usecase/ldes/")

def delete_ldes_files():
    for root, dirs, files in os.walk(directory):
        if(Path(os.path.join(root, f"{Path(root).parts[-1]}.ttl"))).exists():
            os.remove(os.path.join(root, f"{Path(root).parts[-1]}.ttl"))

def create_ldes_files():
    for root, dirs, files in os.walk(directory):
        print("Current folder:", root)
        path = Path(root)
        print("Folder parts:", path.parts[-1]) #File name maybe
        #print("Subfolders:", dirs)
        #print("Subfolders:\n " + "\n ".join(dirs))
        # with open(os.path.join(root,f"{path.parts[-1]}.ttl"),'w') as file:
        #     pass
        with open(os.path.join(root,f"{path.parts[-1]}.ttl"),'a') as file: # we should move the with open with file write to after the for loop. the for loop will only creat the greater than less than relations. It will add them to the base graph initialized before the loop, then it will be added to the graph and written after the graph.
                print(f" Writing to file: {os.path.join(root,f'{path.parts[-1]}.ttl')}")
                file.write(create_base_graph().serialize(format="trig"))
        for d in dirs:
            print(" Subfolder:", d)
            
                #this is where we add actual metadata about the subfolder
                #file.write(f"{d}\n")
                

        #print("Files:", files)
        print("-" * 40)

def create_base_graph():
    g = Graph()
    g.bind("as", AS)
    g.bind("ldes", LDES)
    g.bind("tree", TREE)
    g.bind("xsd", XSD)

    retention_bn = BNode()

    g.add((eventstream_uri, RDF.type, LDES.EventStream))
    g.add((eventstream_uri, LDES.retentionPolicy, retention_bn))
    g.add((eventstream_uri, LDES.timestampPath, AS.published))
    g.add((eventstream_uri, LDES.versionCreateObject, AS.Create))
    g.add((eventstream_uri, LDES.versionDeleteObject, AS.Delete))
    g.add((eventstream_uri, LDES.versionOfPath, AS.object))
    g.add((eventstream_uri, TREE.view, view_uri))

    g.add((retention_bn, RDF.type, LDES.LatestVersionSubset))
    g.add((retention_bn, LDES.amount, Literal(1, datatype=XSD.integer)))

    return g

def create_greaterthan_lessthan_relation(num):
    bn_ge = BNode()
    bn_lt = BNode()

def main():
    start_time = time.perf_counter()
    
    delete_ldes_files()
    create_ldes_files()
    end_time = time.perf_counter()
    print(f"Processing completed in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()