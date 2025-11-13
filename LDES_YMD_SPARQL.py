from rdflib import Graph,URIRef,Namespace,BNode,Literal
from rdflib.namespace import XSD,RDF
import pandas as pd
import argparse
from collections import defaultdict
from datetime import datetime
import json
import os
import calendar
import time


input_path = "Mol_Sluis_Dessel_data_prettified.ttl"
base_path = "./data" 

def load_graph(input_path):
    g = Graph()
    g.parse(input_path, format="turtle", publicID="https://example.com/")
    return g

def process_graph(graph):
    process_graph_query = """
    PREFIX ns1: <http://example.com/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    PREFIX sosa: <http://www.w3.org/ns/sosa/>

    SELECT ?obs ?id ?result ?property ?time
WHERE {
    ?obs a sosa:Observation ;
         ns1:id ?id ;
         sosa:hasSimpleResult ?result ;
         sosa:observedProperty ?property ;
         sosa:resultTime ?time .
}
ORDER BY ?time

"""
    result = graph.query(process_graph_query)
    return result

def divide_data(result):
    SOSA = Namespace("http://www.w3.org/ns/sosa/")
    example = Namespace("http://example.com/")
    xsd = Namespace("http://www.w3.org/2001/XMLSchema#")

    os.makedirs(base_path, exist_ok=True)

    for row in result:
        obs = row['obs']
        id_ = row['id']
        result_value = row['result']
        property_ = row['property']
        time_ = row['time']

        # Extract year, month, day from time_
        dt = datetime.fromisoformat(str(time_.toPython()))
        year_str = str(dt.year)
        month_str = f"{dt.month:02d}"
        day_str = f"{dt.day:02d}"
        timestamp_str = dt.strftime("%Y%m%dT%H%M%S")
        # Construct the file path
        #file_path = os.path.join(base_path, year_str, month_str, day_str, f"{id_}.nt")
        file_path = os.path.join(base_path, year_str, month_str, day_str, "readings.nt")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        temp_graph = Graph()
        temp_graph.bind("sosa", SOSA)
        temp_graph.bind("ex", example)
        temp_graph.bind("xsd", xsd)
        # Add the observation type
        temp_graph.add((obs, RDF.type, SOSA.Observation))

        # Add observation properties
        temp_graph.add((obs, example.id, Literal(id_, datatype=xsd.int)))
        temp_graph.add((obs, SOSA.hasSimpleResult, Literal(result_value, datatype=xsd.float)))
        temp_graph.add((obs, SOSA.observedProperty, Literal(property_)))
        temp_graph.add((obs, SOSA.resultTime, Literal(time_.toPython(), datatype=xsd.dateTime)))

        ttl_str = temp_graph.serialize(format='nt')
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(ttl_str)

        
def main():
    start_time = time.perf_counter()
    original_graph = load_graph(input_path)
    result = process_graph(original_graph)
    divide_data(result)
    end_time = time.perf_counter()
    print(f"Processing completed in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()

