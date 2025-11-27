from rdflib import Graph, Namespace, Literal, URIRef, BNode,ConjunctiveGraph
from rdflib.namespace import XSD, RDF
from collections import defaultdict
from datetime import date, timedelta,timezone,datetime
import os
import time
from pathlib import Path

# --- Config ---
input_path = "./sources/Mol_Sluis_Dessel_data_prettified.ttl"
base_path = "./data"

# --- Namespaces ---
SOSA = Namespace("http://www.w3.org/ns/sosa/")
EX = Namespace("http://example.com/")
XSD = Namespace("http://www.w3.org/2001/XMLSchema#")

directory = "data/"
AS = Namespace("https://www.w3.org/ns/activitystreams#")
LDES = Namespace("https://w3id.org/ldes#")
TREE = Namespace("https://w3id.org/tree#")
eventstream_uri = URIRef("https://shehabeldeenayman.github.io/Mol_sluis_Dessel_Usecase/")
view_uri = URIRef("https://shehabeldeenayman.github.io/Mol_sluis_Dessel_Usecase/data/data.ttl")


#RDF2LDES##############################################################################################

def load_graph(input_path):
    """Load the RDF graph from a Turtle file."""
    g = Graph()
    g.parse(input_path, format="turtle", publicID="https://example.com/")
    return g


def extract_observations(g: Graph):
    """
    Extract all SOSA observations directly from the graph.
    Returns a list of tuples: (obs, id, result, property, time_as_datetime)
    """
    observations = []
    for obs in g.subjects(RDF.type, SOSA.Observation):
        id_node = g.value(obs, EX.id)
        result_node = g.value(obs, SOSA.hasSimpleResult)
        prop_node = g.value(obs, SOSA.observedProperty)
        time_node = g.value(obs, SOSA.resultTime)

        # skip incomplete observations
        if None in (id_node, result_node, prop_node, time_node):
            continue

        # convert values
        id_val = id_node.toPython() if hasattr(id_node, "toPython") else str(id_node)

        try:
            result_val = float(result_node.toPython())
        except Exception:
            try:
                result_val = float(str(result_node))
            except Exception:
                result_val = str(result_node)

        prop_val = prop_node.toPython() if hasattr(prop_node, "toPython") else str(prop_node)

        # ensure time is a datetime object
        try:
            time_val = time_node.toPython()
            if not isinstance(time_val, datetime):
                time_val = datetime.fromisoformat(str(time_val))
        except Exception:
            time_val = datetime.fromisoformat(str(time_node))

        observations.append((obs, id_val, result_val, prop_val, time_val))

    # sort by time for consistency
    #observations.sort(key=lambda t: t[4])
    return observations


def divide_data(observations):
    """Group observations by (year, month, day) and write one file per day."""
    grouped = defaultdict(list)

    # Group all observations by date
    for obs, id_, result_value, property_, time_ in observations:
        key = (time_.year, time_.month, time_.day)
        grouped[key].append((obs, id_, result_value, property_, time_))

    for (year, month, day), daily_obs in grouped.items():
        file_path = os.path.join(base_path, f"{year}/{month:02d}/{day:02d}/readings.ttl")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        temp_graph = Graph()
        temp_graph.bind("sosa", SOSA)
        temp_graph.bind("ex", EX)
        temp_graph.bind("xsd", XSD)
        #store = ConjunctiveGraph()

        for obs, id_, result_value, property_, time_ in daily_obs:
            temp_graph.add((obs, RDF.type, SOSA.Observation))
            temp_graph.add((obs, EX.id, Literal(id_, datatype=XSD.int)))
            temp_graph.add((obs, SOSA.hasSimpleResult, Literal(result_value, datatype=XSD.float)))
            temp_graph.add((obs, SOSA.observedProperty, Literal(property_)))
            temp_graph.add((obs, SOSA.resultTime, Literal(time_, datatype=XSD.dateTime)))


        temp_graph.serialize(destination=file_path, format="trig")
        #temp_graph.serialize(destination=file_path, format="turtle")
        # with open(file_path, "w", encoding="utf-8") as f:
        #     f.write(temp_graph.serialize(format="nt"))

#RDF2LDES##############################################################################################

def delete_ldes_files():
    for root, dirs, files in os.walk(directory):
        if(Path(os.path.join(root, f"{Path(root).parts[-1]}.ttl"))).exists():
            os.remove(os.path.join(root, f"{Path(root).parts[-1]}.ttl"))

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
            #print(" Subfolder:", d)
            temp_graph.add((eventstream_uri, TREE.view, URIRef(f"{eventstream_uri}{root}")))
            
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
    start_time = time.perf_counter()
    g = load_graph(input_path)
    observations = extract_observations(g)
    divide_data(observations)
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
