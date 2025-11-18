import os
from pathlib import Path
# from rdflib import Graph, Namespace, Literal, URIRef, BNode
# from rdflib.namespace import XSD, RDF
import time

directory = "./data/"


for root, dirs, files in os.walk(directory):
    print("Current folder:", root)
    path = Path(root)
    print("Folder parts:", path.parts[-1]) #File name maybe
    #print("Subfolders:", dirs)
    #print("Subfolders:\n " + "\n ".join(dirs))
    with open(os.path.join(root,f"{path.parts[-1]}.ttl"),'w') as file:
        pass
    for d in dirs:
        print(" Subfolder:", d)
        with open(os.path.join(root,f"{path.parts[-1]}.ttl"),'a') as file:
            print(f" Writing to file: {os.path.join(root,f'{path.parts[-1]}.ttl')}")
            file.write(f"{d}\n")

    #print("Files:", files)
    print("-" * 40)