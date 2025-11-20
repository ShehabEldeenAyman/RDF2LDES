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

input_path = "./sources/Mol_Sluis_Dessel_data_prettified.ttl"
base_path = "./data" 
SOSA = Namespace("http://www.w3.org/ns/sosa/")
EX = Namespace("http://example.com/")
XSD = Namespace("http://www.w3.org/2001/XMLSchema#")

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


"""
#ORDER BY ?time
    result = graph.query(process_graph_query)
    return result

def divide_data(result):
    # Prepare a dictionary: { (year, month, day): [triples] }
    grouped = defaultdict(list)

    for row in result:
        obs, id_, result_value, property_, time_ = row
        dt = datetime.fromisoformat(str(time_.toPython()))
        key = (dt.year, dt.month, dt.day)
        grouped[key].append((obs, id_, result_value, property_, time_))

    # Process one file per day
    for (year, month, day), observations in grouped.items():
        file_path = os.path.join(base_path, f"{year}/{month:02d}/{day:02d}/readings.nt")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        temp_graph = Graph()
        temp_graph.bind("sosa", SOSA)
        temp_graph.bind("ex", EX)
        temp_graph.bind("xsd", XSD)

        for obs, id_, result_value, property_, time_ in observations:
            temp_graph.add((obs, RDF.type, SOSA.Observation))
            temp_graph.add((obs, EX.id, Literal(id_, datatype=XSD.int)))
            temp_graph.add((obs, SOSA.hasSimpleResult, Literal(result_value, datatype=XSD.float)))
            temp_graph.add((obs, SOSA.observedProperty, Literal(property_)))
            temp_graph.add((obs, SOSA.resultTime, Literal(time_.toPython(), datatype=XSD.dateTime)))

        # Serialize all observations for that day at once
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(temp_graph.serialize(format='nt'))


def main():
    start_time = time.perf_counter()
    original_graph = load_graph(input_path)
    result = process_graph(original_graph)
    divide_data(result)
    end_time = time.perf_counter()
    print(f"Processing completed in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()
