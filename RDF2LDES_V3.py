from rdflib import Graph, URIRef, Namespace, BNode, Literal, Dataset
from rdflib.namespace import XSD, RDF
import argparse
from collections import defaultdict
from datetime import datetime, timezone
import os
import time
from pathlib import Path
from dateutil.relativedelta import relativedelta

# --- Config ---
input_path = "./sources/Mol_Sluis_Dessel_data_prettified.ttl"
base_path = "./LDES"

# --- Namespaces ---
SOSA = Namespace("http://www.w3.org/ns/sosa/")
EX = Namespace("http://example.org/")
XSD = Namespace("http://www.w3.org/2001/XMLSchema#")
AS = Namespace("https://www.w3.org/ns/activitystreams#")
LDES = Namespace("https://w3id.org/ldes#")
TREE = Namespace("https://w3id.org/tree#")
TSS = Namespace("https://w3id.org/tss#")

directory = "LDES/"

# --- 1. URI FIX: Added '#Stream' to the end ---
eventstream_uri = URIRef("https://shehabeldeenayman.github.io/Mol_sluis_Dessel_Usecase/LDES/LDES.trig#Stream")
base_uri = URIRef("https://shehabeldeenayman.github.io/Mol_sluis_Dessel_Usecase/")

#RDF2LDES##############################################################################################

def load_graph(input_path):
    """Load the RDF graph from a Turtle file."""
    g = Graph()
    g.parse(input_path, format="turtle", publicID="https://example.org/")
    return g


def extract_observations(g: Graph):
    observations = []
    for obs in g.subjects(RDF.type, SOSA.Observation):
        id_node = g.value(obs, EX.id)
        result_node = g.value(obs, SOSA.hasSimpleResult)
        prop_node = g.value(obs, SOSA.observedProperty)
        time_node = g.value(obs, SOSA.resultTime)

        if None in (id_node, result_node, prop_node, time_node):
            continue

        id_val = id_node.toPython() if hasattr(id_node, "toPython") else str(id_node)

        try:
            result_val = float(result_node.toPython())
        except Exception:
            try:
                result_val = float(str(result_node))
            except Exception:
                result_val = str(result_node)

        prop_val = prop_node.toPython() if hasattr(prop_node, "toPython") else str(prop_node)

        try:
            time_val = time_node.toPython()
            if not isinstance(time_val, datetime):
                time_val = datetime.fromisoformat(str(time_val))
        except Exception:
            time_val = datetime.fromisoformat(str(time_node))

        observations.append((obs, id_val, result_val, prop_val, time_val))

    return observations


def divide_data(observations):
    """Group observations by (year, month, day) and write one file per day."""
    grouped = defaultdict(list)

    for obs, id_, result_value, property_, time_ in observations:
        key = (time_.year, time_.month, time_.day)
        grouped[key].append((obs, id_, result_value, property_, time_))

    for (year, month, day), daily_obs in grouped.items():
        file_path = os.path.join(base_path, f"{year}/{month:02d}/{day:02d}/readings.trig")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        ds = Dataset()
        ds.bind("sosa", SOSA)
        ds.bind("ex", EX)
        ds.bind("tss", TSS)
        ds.bind("xsd", XSD)
        ds.bind("ldes", LDES)
        ds.bind("tree", TREE)
        ds.bind("as", AS)
        metadata_graph = ds.default_context

        # --- 2. LOOP FIX: Removed EventStream definition and TREE.view from leaf nodes ---
        # The leaf nodes only need to contain the Members. 
        # The Stream definition stays in the Root file.
        
        for obs, id_, result_value, property_, time_ in daily_obs:
            # Create a named graph for the member (optional but standard in LDES) or just triples
            # Here we link the member to the stream
            
            member_uri = URIRef(f"{eventstream_uri}/{id_}") 
            
            # Add the triple saying "The Stream has this Member"
            metadata_graph.add((eventstream_uri, TREE.member, member_uri))

            # Add the actual data for the member
            # Note: Ideally, the data triples should be in the named graph `member_uri`, 
            # but adding to default graph is also common for simple LDES.
            # Using g_snip to put it in a named graph if preferred:
            g_snip = ds.graph(member_uri)
            
            g_snip.add((obs, RDF.type, SOSA.Observation))
            g_snip.add((obs, EX.id, Literal(id_, datatype=XSD.int)))
            g_snip.add((obs, SOSA.hasSimpleResult, Literal(result_value, datatype=XSD.float)))
            g_snip.add((obs, SOSA.observedProperty, Literal(property_)))
            g_snip.add((obs, SOSA.resultTime, Literal(time_, datatype=XSD.dateTime)))

            # Add extra timestamp metadata if needed
            time_temp_variable = time_.replace(hour=0, minute=0, second=0, microsecond=0)
            metadata_graph.add((member_uri, TSS["from"], Literal(time_temp_variable, datatype=XSD.dateTime)))
  
        ds.serialize(destination=file_path, format="trig")


def delete_ldes_files():
    for root, dirs, files in os.walk(directory):
        if(Path(os.path.join(root, f"{Path(root).parts[-1]}.trig"))).exists():
            os.remove(os.path.join(root, f"{Path(root).parts[-1]}.trig"))

def create_ldes_files():
    for root, dirs, files in os.walk(directory):
        # Normalize path separators for Windows/Linux compatibility
        root_posix = Path(root).as_posix()
        path = Path(root)
        
        write_log(f"Current folder: {root_posix} \n")
        
        # Determine the file name for this directory (e.g. "2021.trig" or "LDES.trig")
        filename = f"{path.parts[-1]}.trig"
        file_uri_path = f"{base_uri}{root_posix}/{filename}"
        
        # Ensure proper URL joining (avoid double slashes if base_uri has one)
        # Using string replacement to ensure we don't duplicate the protocol slashes
        current_page_uri = URIRef(file_uri_path)

        temp_graph = create_base_graph()
        
        # --- 3. LOOP FIX: Only add TREE.view to the ROOT file ---
        # "LDES" is the root folder name in your config
        if path.parts[-1] == "LDES": 
             temp_graph.add((eventstream_uri, TREE.view, current_page_uri))

        for d in dirs:
            # Create URI for the child node
            # If child is a leaf folder (has 'readings.trig'), point to readings.trig
            # Otherwise point to the child's structure file (e.g. 2021/2021.trig)
            
            child_path_obj = Path(root) / d
            # Calculate depth to decide if it's a leaf
            # LDES (1) / 2021 (2) / 01 (3) / 01 (4) -> readings.trig
            if len(child_path_obj.parts) > 4: 
                child_node_uri = URIRef(f"{base_uri}{root_posix}/{d}/readings.trig")
            else:
                child_node_uri = URIRef(f"{base_uri}{root_posix}/{d}/{d}.trig")

            # Create Relations (GreaterThan / LessThan)
            bn_ge = BNode()
            bn_lt = BNode()
            
            temp_graph.add((current_page_uri, TREE.relation, bn_ge))
            temp_graph.add((current_page_uri, TREE.relation, bn_lt))

            temp_graph.add((bn_ge, RDF.type, TREE.GreaterThanOrEqualToRelation))
            temp_graph.add((bn_ge, TREE.node, child_node_uri))
            temp_graph.add((bn_ge, TREE.path, SOSA.resultTime))

            temp_graph.add((bn_lt, RDF.type, TREE.LessThanRelation))
            temp_graph.add((bn_lt, TREE.node, child_node_uri))
            temp_graph.add((bn_lt, TREE.path, SOSA.resultTime))

            # Add Time Logic
            # Root (LDES) -> Years
            if len(path.parts) == 1: 
                year = int(d)
                t_start = datetime(year, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
                t_end = datetime(year + 1, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
                
            # Year (LDES/2021) -> Months
            elif len(path.parts) == 2: 
                year = int(path.parts[1])
                month = int(d)
                t_start = datetime(year, month, 1, 0, 0, 0, tzinfo=timezone.utc)
                t_end = t_start + relativedelta(months=1)

            # Month (LDES/2021/01) -> Days
            elif len(path.parts) == 3: 
                year = int(path.parts[1])
                month = int(path.parts[2])
                day = int(d)
                t_start = datetime(year, month, day, 0, 0, 0, tzinfo=timezone.utc)
                t_end = t_start + relativedelta(days=1)
            
            else:
                continue # Should not happen based on depth logic

            temp_graph.add((bn_ge, TREE.value, Literal(t_start, datatype=XSD.dateTime)))
            temp_graph.add((bn_lt, TREE.value, Literal(t_end, datatype=XSD.dateTime)))

        # Write file only if it is NOT a leaf node (leaves are handled by divide_data)
        if len(path.parts) <= 3:
            with open(os.path.join(root, filename), 'a') as file: 
                write_log(f" Writing to file: {filename} \n")
                file.write(temp_graph.serialize(format="trig"))

        write_log("-" * 40 + "\n")


def create_base_graph():
    g = Dataset()
    default = g.default_context

    default.bind("as", AS)
    default.bind("ldes", LDES)
    default.bind("tree", TREE)
    default.bind("xsd", XSD)
    default.bind("tss", TSS)

    # Only add basic stream info here. 
    # Do NOT add tree:view here, as it is handled conditionally in the loop.
    default.add((eventstream_uri, RDF.type, LDES.EventStream))
    default.add((eventstream_uri, LDES.timestampPath, SOSA.resultTime))

    return g 

def write_log(msg):
    with open("logs.txt",'a') as file:
        file.write(msg)
def delete_log():
    if(Path("logs.txt").exists()):
        os.remove("logs.txt")

def main():
    start_time = time.perf_counter()
    g = load_graph(input_path)
    observations = extract_observations(g)
    
    # 1. Clear old data
    delete_log()
    delete_ldes_files()
    
    # 2. Generate Leaf Nodes (Readings)
    divide_data(observations)
    
    # 3. Generate Tree Structure (Relations)
    create_ldes_files()
    
    end_time = time.perf_counter()
    print(f"Processing completed in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()