from rdflib import Graph,URIRef,Namespace,BNode,Literal
from rdflib.namespace import XSD,RDF
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
    process_graph_query = """
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
    result = graph.query(process_graph_query)
    print(f"Total snippets processed: {len(result)}")
    return result


def divide_data(result):
    SOSA = Namespace("http://www.w3.org/ns/sosa/")
    example = Namespace("http://example.org/")
    xsd = Namespace("http://www.w3.org/2001/XMLSchema#")
    tss = Namespace("https://w3id.org/tss#")

    grouped = defaultdict(list)

    # grouping (fast, no datetime parsing)
    for row in result:
        dt = str(row['fromTime'])
        key = (dt[:4], dt[5:7], dt[8:10])  # year, month, day
        grouped[key].append(row)

    template_cache = set()

    for (year, month, day), rows in grouped.items():
        g = Graph()
        g.bind("sosa", SOSA)
        g.bind("ex", example)
        g.bind("xsd", xsd)
        g.bind("tss", tss)

        for row in rows:
            snippet = row['snippet']
            from_time = str(row['fromTime'])
            to_time = str(row['toTime'])
            pointType = row['pointType']
            pointsJson = row['pointsJson']
            template = row['template']
            sensor = row['sensor']
            observedProperty = row['observedProperty']

            # snippet triples
            g.add((snippet, RDF.type, tss.Snippet))
            g.add((snippet, tss.about, template))
            g.add((snippet, tss.from_, Literal(from_time, datatype=xsd.dateTime)))
            g.add((snippet, tss.to, Literal(to_time, datatype=xsd.dateTime)))
            g.add((snippet, tss.pointType, Literal(pointType)))
            g.add((snippet, tss.points, Literal(pointsJson)))

            # template (only once)
            if template not in template_cache:
                template_cache.add(template)
                g.add((template, RDF.type, tss.PointTemplate))
                g.add((template, SOSA.madeBySensor, sensor))
                g.add((template, SOSA.observedProperty, observedProperty))

        # output
        file_path = os.path.join(base_path, year, month, day, "readings.ttl")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        g.serialize(destination=file_path, format="turtle")

    SOSA = Namespace("http://www.w3.org/ns/sosa/")
    example = Namespace("http://example.org/")
    xsd = Namespace("http://www.w3.org/2001/XMLSchema#")
    tss = Namespace("https://w3id.org/tss#")

    grouped = defaultdict(list)

    # First pass: group rows by date
    for row in result:
        dt = datetime.fromisoformat(str(row['fromTime'].toPython()))
        key = (dt.year, dt.month, dt.day)
        grouped[key].append(row)

    # Second pass: write one graph per date
    for (year, month, day), rows in grouped.items():
        g = Graph()
        g.bind("sosa", SOSA)
        g.bind("ex", example)
        g.bind("xsd", xsd)
        g.bind("tss", tss)

        for row in rows:
            snippet = row['snippet']
            from_time = row['fromTime']
            to_time = row['toTime']
            pointType = row['pointType']
            pointsJson = row['pointsJson']
            template = row['template']
            sensor = row['sensor']
            observedProperty = row['observedProperty']

            g.add((snippet, RDF.type, tss.Snippet))
            g.add((snippet, tss.about, template))
            g.add((snippet, tss.from_, Literal(from_time.toPython(), datatype=xsd.dateTime)))
            g.add((snippet, tss.to, Literal(to_time.toPython(), datatype=xsd.dateTime)))
            g.add((snippet, tss.pointType, Literal(pointType)))
            g.add((snippet, tss.points, Literal(pointsJson)))

            g.add((template, RDF.type, tss.PointTemplate))
            g.add((template, SOSA.madeBySensor, sensor))
            g.add((template, SOSA.observedProperty, observedProperty))

        # Output path
        year_str = f"{year:04d}"
        month_str = f"{month:02d}"
        day_str = f"{day:02d}"

        file_path = os.path.join(base_path, year_str, month_str, day_str, "readings.ttl")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # Write once → prefixes appear only once
        g.serialize(destination=file_path, format="turtle")



#RDF2LDES##############################################################################################
directory = "LDESTSS/"
AS = Namespace("https://www.w3.org/ns/activitystreams#")
LDES = Namespace("https://w3id.org/ldes#")
TREE = Namespace("https://w3id.org/tree#")
eventstream_uri = URIRef("https://shehabeldeenayman.github.io/Mol_sluis_Dessel_Usecase/") #change this everytime you change the base uri for hosting
#view_uri = URIRef("https://shehabeldeenayman.github.io/Mol_sluis_Dessel_Usecase/data/LDES/data.ttl")

def delete_ldes_files():
    for root, dirs, files in os.walk(directory):
        if(Path(os.path.join(root, f"{Path(root).parts[-1]}.ttl"))).exists():
            os.remove(os.path.join(root, f"{Path(root).parts[-1]}.ttl"))

# def create_ldes_files():
    # for root, dirs, files in os.walk(directory):

    #     root_path = Path(root)
    #     parts = root_path.parts
    #     depth = len(parts)

    #     ttl_path = root_path / f"{parts[-1]}.ttl"
    #     ttl_depth = len(ttl_path.parts)

    #     # Build graph first
    #     temp_graph = create_base_graph()

    #     # Precompute URIs
    #     root_uri = f"{eventstream_uri}{root_path.as_posix()}/"

    #     # Compute year/month/day once
    #     if ttl_depth == 2: 
    #         mode = "main"
    #     elif ttl_depth == 3:
    #         year = int(parts[1])
    #         mode = "year"
    #     elif ttl_depth == 4:
    #         year = int(parts[1])
    #         month = int(parts[2])
    #         mode = "month"
    #     else:
    #         mode = None

    #     for d in dirs:
    #         d_uri = f"{root_uri}{d}/"

    #         bn_ge = BNode(f"ge_{d}")
    #         bn_lt = BNode(f"lt_{d}")

    #         # add relations...

    #     # Write once
    #     if ttl_depth <= 4:
    #         temp_graph.serialize(destination=ttl_path, format="trig")

    # for root, dirs, files in os.walk(directory):
    #     root = Path(root).as_posix()
        
    #     #print("Current folder:", root)
    #     write_log(f"Current folder: {root} \n")
    #     write_log(f"Current folder length: {len(Path(root).parts)} \n")
    #     path = Path(root)
    #     #print("Last Folder", path.parts[-1]) #File name maybe
    #     write_log(f"Last part of directory {path.parts[-1]} \n")
    #     #print("Subfolders:", dirs)
    #     #print("Subfolders:\n " + "\n ".join(dirs))
    #     # with open(os.path.join(root,f"{path.parts[-1]}.ttl"),'w') as file:
    #     #     pass
    #     temp_graph = create_base_graph()
    #     direct_subfolders = [Path(root) / d for d in dirs]
    #     for folder in direct_subfolders:
    #         write_log(f"  Subfolder: {folder.as_posix()}\n")
    #         #print(len(Path(folder).parts),"\n")

    #     # for sub in direct_subfolders:
    #     #         p = Path(sub)
    #     #         if len(p.parts) == 2:
    #     #             print(p.parts[1])
    #     #         if len(p.parts) == 3:
    #     #             print(p.parts[2])
    #     #         if len(p.parts) == 4:
    #     #             print(p.parts[3])
        
    #     for d in dirs:
    #         #print(" Subfolder:", d)
    #         temp_graph.add((eventstream_uri, TREE.view, URIRef(f"{eventstream_uri}{root}.ttl")))
            
    #         write_log(f"Subfolder: {d} \n")
    #         bn_ge = BNode()
    #         bn_lt = BNode()
    #         temp_graph.add((bn_ge, RDF.type, TREE.GreaterThanOrEqualToRelation))
    #         # if len(Path(os.path.join(root, f"{path.parts[-1]}.ttl")).parts) == 2:
    #         #     print(os.path.join(root, f"{path.parts[-1]}.ttl"))
    #             #print(URIRef(f"{eventstream_uri}{root}/{d}/{d}.ttl"))
    #         if len(Path(os.path.join(root, f"{path.parts[-1]}.ttl")).parts) <= 3: #this is the main data.ttl file
    #             temp_graph.add((bn_ge, TREE.node, URIRef(f"{eventstream_uri}{root}/{d}/{d}.ttl")))
                
    #             #
    #         if len(Path(os.path.join(root, f"{path.parts[-1]}.ttl")).parts) > 3:
    #             temp_graph.add((bn_ge, TREE.node, URIRef(f"{eventstream_uri}{root}/{d}/readings.ttl")))

    #         temp_graph.add((bn_ge, TREE.path, AS.published))

    #         if len(Path(os.path.join(root, f"{path.parts[0]}.ttl")).parts) == 3:#writing in each year file. so we should be refrencing months.
    #             #print(Path(os.path.join(root, f"{path.parts[0]}.ttl"))) 
    #             #print(d) #this is the actual month.
    #             temp_graph.add((bn_ge,TREE.vaue,Literal(datetime(int(Path(os.path.join(root, f"{path.parts[0]}.ttl")).parts[1]),int(d),1,0,0,0, tzinfo=timezone.utc), datatype=XSD.dateTime)))
    #             temp_graph.add((bn_lt,TREE.vaue,Literal(datetime(int(Path(os.path.join(root, f"{path.parts[0]}.ttl")).parts[1]),int(d),1,0,0,0, tzinfo=timezone.utc), datatype=XSD.dateTime))) #this has a small bug that needs to be fixed

    #         if len(Path(os.path.join(root, f"{path.parts[0]}.ttl")).parts) == 2: #writing in the main data file. it should be ok
    #             #print(d)
    #             temp_graph.add((bn_ge,TREE.vaue,Literal(datetime(int(d),1,1,0,0,0, tzinfo=timezone.utc), datatype=XSD.dateTime)))
    #             temp_graph.add((bn_lt,TREE.vaue,Literal(datetime(int(d)+1,1,1,0,0,0, tzinfo=timezone.utc), datatype=XSD.dateTime)))

    #         if len(Path(os.path.join(root, f"{path.parts[0]}.ttl")).parts) == 4:#writing in the month file. so we should be refrencing days.
    #             #print(Path(os.path.join(root, f"{path.parts[0]}.ttl"))) 
    #             #print(Path(os.path.join(root, f"{path.parts[0]}.ttl")).parts[2])
    #             temp_graph.add((bn_ge,TREE.vaue,Literal(datetime(int(Path(os.path.join(root, f"{path.parts[0]}.ttl")).parts[1]),int(Path(os.path.join(root, f"{path.parts[0]}.ttl")).parts[2]),int(d),0,0,0, tzinfo=timezone.utc), datatype=XSD.dateTime)))
    #             temp_graph.add((bn_lt,TREE.vaue,Literal(datetime(int(Path(os.path.join(root, f"{path.parts[0]}.ttl")).parts[1]),int(Path(os.path.join(root, f"{path.parts[0]}.ttl")).parts[2]),int(d),0,0,0, tzinfo=timezone.utc), datatype=XSD.dateTime)))

    #         # if(len(Path(direct_subfolders).parts)==2):
    #         #     print(Path(direct_subfolders).parts[1]) #we should find a way to get the subfolders
    #             #temp_graph.add((bn_ge, TREE.value, Literal(timestamp, datatype=XSD.dateTime)))
    #         #still missing the date time value here

    #         temp_graph.add((bn_lt, RDF.type, TREE.LessThanRelation))
    #         if len(Path(os.path.join(root, f"{path.parts[-1]}.ttl")).parts) <= 3:
    #             temp_graph.add((bn_lt, TREE.node, URIRef(f"{eventstream_uri}{root}/{d}/{d}.ttl")))
    #             #print(URIRef(f"{eventstream_uri}{root}/{d}/{d}.ttl"))
    #         if len(Path(os.path.join(root, f"{path.parts[-1]}.ttl")).parts) > 3:
    #             #temp_graph.add((bn_lt, TREE.node, URIRef(f"{eventstream_uri}{root}/{d}?page=0")))
    #             temp_graph.add((bn_lt, TREE.node, URIRef(f"{eventstream_uri}{root}/{d}/readings.ttl")))

    #         temp_graph.add((bn_lt, TREE.path, AS.published))

    #         #still missing the date time value here

            
    #             #this is where we add actual metadata about the subfolder
    #             #file.write(f"{d}\n")
    #     if len(Path(os.path.join(root, f"{path.parts[-1]}.ttl")).parts) <= 4:
    #         with open(os.path.join(root,f"{path.parts[-1]}.ttl"),'a') as file: # we should move the with open with file write to after the for loop. the for loop will only creat the greater than less than relations. It will add them to the base graph initialized before the loop, then it will be added to the graph and written after the graph.
    #                 #print(f" Writing to file: {os.path.join(root,f'{path.parts[-1]}.ttl')}")
    #                 write_log(f"path parts: {len(Path(os.path.join(root, f"{path.parts[-1]}.ttl")).parts)} \n")
    #                 write_log(f" Writing to file: {Path(os.path.join(root,f'{path.parts[-1]}.ttl')).as_posix()} \n")
    #                 file.write(temp_graph.serialize(format="trig"))

    #     #print("Files:", files)
    #     write_log("-" * 40)
    #     write_log("\n")
    #     #print("-" * 40)

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

        # for sub in direct_subfolders:
        #         p = Path(sub)
        #         if len(p.parts) == 2:
        #             print(p.parts[1])
        #         if len(p.parts) == 3:
        #             print(p.parts[2])
        #         if len(p.parts) == 4:
        #             print(p.parts[3])
        
        for d in dirs:
            #print(" Subfolder:", d)                                                        #we only need last part of root.
            temp_graph.add((eventstream_uri, TREE.view, URIRef(f"{eventstream_uri}{root}/{path.parts[-1]}.ttl"))) #this needs to be fixed first thing in the morning.
            
            write_log(f"Subfolder: {d} \n")
            bn_ge = BNode()
            bn_lt = BNode()
            temp_graph.add((bn_ge, RDF.type, TREE.GreaterThanOrEqualToRelation))
            # if len(Path(os.path.join(root, f"{path.parts[-1]}.ttl")).parts) == 2:
            #     print(os.path.join(root, f"{path.parts[-1]}.ttl"))
                #print(URIRef(f"{eventstream_uri}{root}/{d}/{d}.ttl"))
            if len(Path(os.path.join(root, f"{path.parts[-1]}.ttl")).parts) <= 3: #this is the main data.ttl file
                temp_graph.add((bn_ge, TREE.node, URIRef(f"{eventstream_uri}{root}/{d}/{d}.ttl")))
                #
            if len(Path(os.path.join(root, f"{path.parts[-1]}.ttl")).parts) > 3:
                temp_graph.add((bn_ge, TREE.node, URIRef(f"{eventstream_uri}{root}/{d}/readings.ttl")))

            temp_graph.add((bn_ge, TREE.path, AS.published))

            if len(Path(os.path.join(root, f"{path.parts[0]}.ttl")).parts) == 3:#writing in each year file. so we should be refrencing months.
                #print(Path(os.path.join(root, f"{path.parts[0]}.ttl"))) 
                #print(d) #this is the actual month.
                temp_graph.add((bn_ge,TREE.vaue,Literal(datetime(int(Path(os.path.join(root, f"{path.parts[0]}.ttl")).parts[1]),int(d),1,0,0,0, tzinfo=timezone.utc), datatype=XSD.dateTime)))
                temp_graph.add((bn_lt,TREE.vaue,Literal(datetime(int(Path(os.path.join(root, f"{path.parts[0]}.ttl")).parts[1]),int(d),1,0,0,0, tzinfo=timezone.utc), datatype=XSD.dateTime))) #this has a small bug that needs to be fixed

            if len(Path(os.path.join(root, f"{path.parts[0]}.ttl")).parts) == 2: #writing in the main data file. it should be ok
                #print(d)
                temp_graph.add((bn_ge,TREE.vaue,Literal(datetime(int(d),1,1,0,0,0, tzinfo=timezone.utc), datatype=XSD.dateTime)))
                temp_graph.add((bn_lt,TREE.vaue,Literal(datetime(int(d)+1,1,1,0,0,0, tzinfo=timezone.utc), datatype=XSD.dateTime)))

            if len(Path(os.path.join(root, f"{path.parts[0]}.ttl")).parts) == 4:#writing in the month file. so we should be refrencing days.
                #print(Path(os.path.join(root, f"{path.parts[0]}.ttl"))) 
                #print(Path(os.path.join(root, f"{path.parts[0]}.ttl")).parts[2])
                temp_graph.add((bn_ge,TREE.vaue,Literal(datetime(int(Path(os.path.join(root, f"{path.parts[0]}.ttl")).parts[1]),int(Path(os.path.join(root, f"{path.parts[0]}.ttl")).parts[2]),int(d),0,0,0, tzinfo=timezone.utc), datatype=XSD.dateTime)))
                temp_graph.add((bn_lt,TREE.vaue,Literal(datetime(int(Path(os.path.join(root, f"{path.parts[0]}.ttl")).parts[1]),int(Path(os.path.join(root, f"{path.parts[0]}.ttl")).parts[2]),int(d),0,0,0, tzinfo=timezone.utc), datatype=XSD.dateTime)))

            # if(len(Path(direct_subfolders).parts)==2):
            #     print(Path(direct_subfolders).parts[1]) #we should find a way to get the subfolders
                #temp_graph.add((bn_ge, TREE.value, Literal(timestamp, datatype=XSD.dateTime)))
            #still missing the date time value here

            temp_graph.add((bn_lt, RDF.type, TREE.LessThanRelation))
            if len(Path(os.path.join(root, f"{path.parts[-1]}.ttl")).parts) <= 3:
                temp_graph.add((bn_lt, TREE.node, URIRef(f"{eventstream_uri}{root}/{d}/{d}.ttl")))
                #print(URIRef(f"{eventstream_uri}{root}/{d}/{d}.ttl"))
            if len(Path(os.path.join(root, f"{path.parts[-1]}.ttl")).parts) > 3:
                #temp_graph.add((bn_lt, TREE.node, URIRef(f"{eventstream_uri}{root}/{d}?page=0")))
                temp_graph.add((bn_lt, TREE.node, URIRef(f"{eventstream_uri}{root}/{d}/readings.ttl")))

            temp_graph.add((bn_lt, TREE.path, AS.published))

            #still missing the date time value here

            
                #this is where we add actual metadata about the subfolder
                #file.write(f"{d}\n")
        if len(Path(os.path.join(root, f"{path.parts[-1]}.ttl")).parts) <= 4:
            with open(os.path.join(root,f"{path.parts[-1]}.ttl"),'a') as file: # we should move the with open with file write to after the for loop. the for loop will only creat the greater than less than relations. It will add them to the base graph initialized before the loop, then it will be added to the graph and written after the graph.
                    #print(f" Writing to file: {os.path.join(root,f'{path.parts[-1]}.ttl')}")
                    write_log(f"path parts: {len(Path(os.path.join(root, f"{path.parts[-1]}.ttl")).parts)} \n")
                    write_log(f" Writing to file: {Path(os.path.join(root,f'{path.parts[-1]}.ttl')).as_posix()} \n")
                    file.write(temp_graph.serialize(format="trig"))

        #print("Files:", files)
        write_log("-" * 40)
        write_log("\n")
        #print("-" * 40)


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
    #g.add((eventstream_uri, TREE.view, view_uri)) # this needs to be moved and properly fixed

    g.add((retention_bn, RDF.type, LDES.LatestVersionSubset))
    g.add((retention_bn, LDES.amount, Literal(1, datatype=XSD.integer)))

    return g

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

