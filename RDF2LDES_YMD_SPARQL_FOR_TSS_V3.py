from rdflib import Graph,URIRef,Namespace,BNode,Literal,Dataset
from rdflib.namespace import XSD,RDF
from rdflib.term import BNode
import pandas as pd
import argparse
from collections import defaultdict
from datetime import datetime, timezone
import json
import os
import calendar
import time
from pathlib import Path


input_path = "./sources/Mol_Sluis_Dessel_data_TSS_per_day.ttl"
base_path = "./LDESTSS"

def load_graph(input_path):
    g = Graph()
    g.parse(input_path, format="turtle", publicID="https://example.org/")
    return g

def process_graph(graph):
    query = """
PREFIX tss: <https://w3id.org/tss#>
PREFIX sosa: <http://www.w3.org/ns/sosa/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?snippet ?fromTime ?toTime ?pointType ?pointsJson
       ?template ?sensor ?observedProperty
WHERE {
    ?snippet a tss:Snippet ;
             tss:about ?template ;
             tss:from ?fromTime ;
             tss:to ?toTime ;
             tss:pointType ?pointType ;
             tss:points ?pointsJson .

    ?template a tss:PointTemplate ;
              sosa:madeBySensor ?sensor ;
              sosa:observedProperty ?observedProperty .
}
"""
    result = graph.query(query)
    print(f"Total snippets processed: {len(result)}")
    return result


def divide_data(result):
    SOSA = Namespace("http://www.w3.org/ns/sosa/")
    EX = Namespace("http://example.org/")
    TSS = Namespace("https://w3id.org/tss#")

    grouped = defaultdict(list)

    # group by date
    for row in result:
        dt = datetime.fromisoformat(str(row['fromTime'].toPython()))
        key = (dt.year, dt.month, dt.day)
        grouped[key].append(row)

    # process one dataset per day
    for (year, month, day), rows in grouped.items():

        ds = Dataset()
        ds.bind("sosa", SOSA)
        ds.bind("ex", EX)
        ds.bind("tss", TSS)
        ds.bind("xsd", XSD)
        ds.bind("ldes", LDES)
        ds.bind("tree", TREE)
        ds.bind("as", AS)
        metadata_graph = ds.graph()
        metadata_graph = ds.default_context

        #retention_policy = BNode()

        metadata_graph.add((eventstream_uri, RDF.type, LDES.EventStream))
        metadata_graph.add((eventstream_uri, LDES.timestampPath, TSS["from"]))
        #metadata_graph.add((base_uri, LDES.versionCreateObject, AS.Create))
        #metadata_graph.add((base_uri, LDES.versionDeleteObject, AS.Delete))
        #metadata_graph.add((base_uri, LDES.versionOfPath, AS.object))
        #metadata_graph.add((base_uri, LDES.retentionPolicy, retention_policy))

        #metadata_graph.add((retention_policy, RDF.type, LDES.LatestVersionSubset))
        #metadata_graph.add((retention_policy, LDES.amount, Literal(1, datatype=XSD.integer)))
        #metadata_graph.add((base_uri, TREE.view, URIRef(f"{base_uri}{year:04d}/{month:02d}/{day:02d}/readings.trig")))
        metadata_graph.add((eventstream_uri, TREE.view, home_page))

        for row in rows:

            snippet_iri = row["snippet"]
            template_bnode = row["template"]        # this is already a BlankNode
            sensor = row["sensor"]
            observedProperty = row["observedProperty"]

            # Named graph for this snippet
            g_snip = ds.graph(snippet_iri)

            # -----------------------------
            # Add snippet triples
            # -----------------------------
            g_snip.add((snippet_iri, RDF.type, TSS.Snippet))
            g_snip.add((snippet_iri, TSS.about, template_bnode))
            g_snip.add((snippet_iri, TSS["from"], Literal(row["fromTime"].toPython(), datatype=XSD.dateTime)))
            g_snip.add((snippet_iri, TSS.to, Literal(row["toTime"].toPython(), datatype=XSD.dateTime)))
            g_snip.add((snippet_iri, TSS.pointType, Literal(row["pointType"])))
            g_snip.add((snippet_iri, TSS.points, Literal(row["pointsJson"])))

            # -----------------------------
            # Add PointTemplate into the SAME graph
            # -----------------------------
            g_snip.add((template_bnode, RDF.type, TSS.PointTemplate))
            g_snip.add((template_bnode, SOSA.madeBySensor, sensor))
            g_snip.add((template_bnode, SOSA.observedProperty, observedProperty))

            # -----------------------------
            # Add TSS member to metadat graph
            # -----------------------------
            metadata_graph.add((eventstream_uri,TREE.member,snippet_iri))
            #metadata_graph.add((snippet_iri,RDF.type, AS.Create))
            #metadata_graph.add((snippet_iri, AS.object, snippet_iri))
            metadata_graph.add((snippet_iri, TSS["from"], Literal(row["fromTime"].toPython(), datatype=XSD.dateTime)))



        # output path
        file_path = os.path.join(
            base_path,
            f"{year:04d}", f"{month:02d}", f"{day:02d}",
            "readings.trig"
        )
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        ds.serialize(destination=file_path, format="trig")



#RDF2LDES##############################################################################################
directory = "LDESTSS/"
AS = Namespace("https://www.w3.org/ns/activitystreams#")
LDES = Namespace("https://w3id.org/ldes#")
TREE = Namespace("https://w3id.org/tree#")
TSS = Namespace("https://w3id.org/tss#")
eventstream_uri = URIRef("https://shehabeldeenayman.github.io/Mol_sluis_Dessel_Usecase/LDESTSS/LDESTSS#eventstream") #change this everytime you change the base uri for hosting
base_uri = URIRef("https://shehabeldeenayman.github.io/Mol_sluis_Dessel_Usecase/")
home_page = URIRef("https://shehabeldeenayman.github.io/Mol_sluis_Dessel_Usecase/LDESTSS/LDESTSS.trig")

def delete_ldes_files():
    for root, dirs, files in os.walk(directory):
        if(Path(os.path.join(root, f"{Path(root).parts[-1]}.trig"))).exists():
            os.remove(os.path.join(root, f"{Path(root).parts[-1]}.trig"))

def create_ldes_files():
    for root, dirs, files in os.walk(directory):
        root = Path(root).as_posix()
        
        #print("Current folder:", root)
        write_log(f"Current folder: {root} \n")
        write_log(f"Current folder length: {len(Path(root).parts)} \n")
        path = Path(root)
        #print("Last Folder", path.parts[-1]) #File name maybe
        write_log(f"Last part of directory {path.parts[-1]} \n")
        #print("Subfolders:", dirs)
        #print("Subfolders:\n " + "\n ".join(dirs))
        # with open(os.path.join(root,f"{path.parts[-1]}.ttl"),'w') as file:
        #     pass
        temp_graph = create_base_graph()
        direct_subfolders = [Path(root) / d for d in dirs]
        for folder in direct_subfolders:
            write_log(f"  Subfolder: {folder.as_posix()}\n")
            #print(len(Path(folder).parts),"\n")
       
        for d in dirs:
            #print(" Subfolder:", d)                                                       
            #temp_graph.add((eventstream_uri, TREE.view, URIRef(f"{base_uri}{root}/{path.parts[-1]}.trig")))
            #temp_graph.add((eventstream_uri, TREE.view, URIRef("")))
            temp_graph.add((eventstream_uri, TREE.view,home_page ))
            
            write_log(f"Subfolder: {d} \n")
            bn_ge = BNode()
            bn_lt = BNode()
            #######tree relation
            temp_graph.add((URIRef(f"{base_uri}{root}/{path.parts[-1]}.trig"),TREE.relation,bn_ge))
            temp_graph.add((URIRef(f"{base_uri}{root}/{path.parts[-1]}.trig"),TREE.relation,bn_lt))

            #######
            temp_graph.add((bn_ge, RDF.type, TREE.GreaterThanOrEqualToRelation))

            if len(Path(os.path.join(root, f"{path.parts[-1]}.trig")).parts) <= 3: #this is the main data.ttl file
                temp_graph.add((bn_ge, TREE.node, URIRef(f"{base_uri}{root}/{d}/{d}.trig")))
                #
            if len(Path(os.path.join(root, f"{path.parts[-1]}.trig")).parts) > 3:
                temp_graph.add((bn_ge, TREE.node, URIRef(f"{base_uri}{root}/{d}/readings.trig")))

            temp_graph.add((bn_ge, TREE.path, TSS["from"]))

            if len(Path(os.path.join(root, f"{path.parts[0]}.trig")).parts) == 3:#writing in each year file. so we should be refrencing months.
                #print(Path(os.path.join(root, f"{path.parts[0]}.ttl"))) 
                #print(d) #this is the actual month.
                temp_graph.add((bn_ge,TREE.value,Literal(datetime(int(Path(os.path.join(root, f"{path.parts[0]}.trig")).parts[1]),int(d),1,0,0,0, tzinfo=timezone.utc), datatype=XSD.dateTime)))
                temp_graph.add((bn_lt,TREE.value,Literal(datetime(int(Path(os.path.join(root, f"{path.parts[0]}.trig")).parts[1]),int(d),1,0,0,0, tzinfo=timezone.utc), datatype=XSD.dateTime))) #this has a small bug that needs to be fixed

            if len(Path(os.path.join(root, f"{path.parts[0]}.trig")).parts) == 2: #writing in the main data file. it should be ok
                #print(d)
                temp_graph.add((bn_ge,TREE.value,Literal(datetime(int(d),1,1,0,0,0, tzinfo=timezone.utc), datatype=XSD.dateTime)))
                temp_graph.add((bn_lt,TREE.value,Literal(datetime(int(d)+1,1,1,0,0,0, tzinfo=timezone.utc), datatype=XSD.dateTime)))

            if len(Path(os.path.join(root, f"{path.parts[0]}.trig")).parts) == 4:#writing in the month file. so we should be refrencing days.
                #print(Path(os.path.join(root, f"{path.parts[0]}.ttl"))) 
                #print(Path(os.path.join(root, f"{path.parts[0]}.ttl")).parts[2])
                temp_graph.add((bn_ge,TREE.value,Literal(datetime(int(Path(os.path.join(root, f"{path.parts[0]}.trig")).parts[1]),int(Path(os.path.join(root, f"{path.parts[0]}.trig")).parts[2]),int(d),0,0,0, tzinfo=timezone.utc), datatype=XSD.dateTime)))
                temp_graph.add((bn_lt,TREE.value,Literal(datetime(int(Path(os.path.join(root, f"{path.parts[0]}.trig")).parts[1]),int(Path(os.path.join(root, f"{path.parts[0]}.trig")).parts[2]),int(d),0,0,0, tzinfo=timezone.utc), datatype=XSD.dateTime)))

            # if(len(Path(direct_subfolders).parts)==2):
            #     print(Path(direct_subfolders).parts[1]) #we should find a way to get the subfolders
                #temp_graph.add((bn_ge, TREE.value, Literal(timestamp, datatype=XSD.dateTime)))
            #still missing the date time value here

            temp_graph.add((bn_lt, RDF.type, TREE.LessThanRelation))
            if len(Path(os.path.join(root, f"{path.parts[-1]}.trig")).parts) <= 3:
                temp_graph.add((bn_lt, TREE.node, URIRef(f"{base_uri}{root}/{d}/{d}.trig")))
                #print(URIRef(f"{eventstream_uri}{root}/{d}/{d}.ttl"))
            if len(Path(os.path.join(root, f"{path.parts[-1]}.trig")).parts) > 3:
                #temp_graph.add((bn_lt, TREE.node, URIRef(f"{eventstream_uri}{root}/{d}?page=0")))
                temp_graph.add((bn_lt, TREE.node, URIRef(f"{base_uri}{root}/{d}/readings.trig")))

            temp_graph.add((bn_lt, TREE.path, TSS["from"]))

            #still missing the date time value here

            
                #this is where we add actual metadata about the subfolder
                #file.write(f"{d}\n")
        if len(Path(os.path.join(root, f"{path.parts[-1]}.trig")).parts) <= 4:
            with open(os.path.join(root,f"{path.parts[-1]}.trig"),'a') as file: # we should move the with open with file write to after the for loop. the for loop will only creat the greater than less than relations. It will add them to the base graph initialized before the loop, then it will be added to the graph and written after the graph.
                    #print(f" Writing to file: {os.path.join(root,f'{path.parts[-1]}.ttl')}")
                    write_log(f"path parts: {len(Path(os.path.join(root, f"{path.parts[-1]}.trig")).parts)} \n")
                    write_log(f" Writing to file: {Path(os.path.join(root,f'{path.parts[-1]}.trig')).as_posix()} \n")
                    file.write(temp_graph.serialize(format="trig"))

        #print("Files:", files)
        write_log("-" * 40)
        write_log("\n")
        #print("-" * 40)

from rdflib import ConjunctiveGraph

def create_base_graph():
    g = Dataset()     # not Graph()
    default = g.default_context

    default.bind("as", AS)
    default.bind("ldes", LDES)
    default.bind("tree", TREE)
    default.bind("xsd", XSD)
    default.bind("tss",TSS)

    retention_bn = BNode()

    default.add((eventstream_uri, RDF.type, LDES.EventStream))
    #default.add((eventstream_uri, LDES.retentionPolicy, retention_bn))
    default.add((eventstream_uri, LDES.timestampPath, TSS["from"]))
    #default.add((eventstream_uri, LDES.versionCreateObject, AS.Create))
    #default.add((eventstream_uri, LDES.versionDeleteObject, AS.Delete))
    #default.add((eventstream_uri, LDES.versionOfPath, AS.object))

    #default.add((retention_bn, RDF.type, LDES.LatestVersionSubset))
    #default.add((retention_bn, LDES.amount, Literal(1, datatype=XSD.integer)))

    return g   # return the CG


#def create_greaterthan_lessthan_relation(num):
    


def write_log(msg):
    with open("logs.txt",'a') as file:
        file.write(msg)
def delete_log():
    if(Path("logs.txt").exists()):
        os.remove("logs.txt")

#RDF2LDES##############################################################################################
        
def main():
######################################################################
    print("Starting processing...")
    start_time = time.perf_counter()
    original_graph = load_graph(input_path)
    result = process_graph(original_graph)
    divide_data(result)
    end_time = time.perf_counter()
    print(f"Processing completed in {end_time - start_time:.2f} seconds.")
######################################################################
    start_time = time.perf_counter()
    delete_log()
    delete_ldes_files()
    create_ldes_files()
    end_time = time.perf_counter()
    print(f"Processing completed in {end_time - start_time:.2f} seconds.")
######################################################################

if __name__ == "__main__":
    main()

