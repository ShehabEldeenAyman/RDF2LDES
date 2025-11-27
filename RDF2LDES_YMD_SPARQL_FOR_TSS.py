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


input_path = "./sources/test_tss.ttl"
base_path = "./data" 

def load_graph(input_path):
    g = Graph()
    g.parse(input_path, format="turtle", publicID="https://example.com/")
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
    example = Namespace("http://example.com/")
    xsd = Namespace("http://www.w3.org/2001/XMLSchema#")
    tss = Namespace("https://w3id.org/tss#")

    os.makedirs(base_path, exist_ok=True)

    for row in result:
        # Extract variables from SPARQL row
        snippet = row['snippet']
        from_time = row['fromTime']
        to_time = row['toTime']
        pointType = row['pointType']
        pointsJson = row['pointsJson']
        template = row['template']
        sensor = row['sensor']
        observedProperty = row['observedProperty']

        # Extract year, month, day from ?from variable
        dt = datetime.fromisoformat(str(from_time.toPython()))
        year_str = str(dt.year)
        month_str = f"{dt.month:02d}"
        day_str = f"{dt.day:02d}"

        # Output path
        file_path = os.path.join(base_path, year_str, month_str, day_str, "readings.ttl")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # Create small graph for writing
        temp_graph = Graph()
        temp_graph.bind("sosa", SOSA)
        temp_graph.bind("ex", example)
        temp_graph.bind("xsd", xsd)
        temp_graph.bind("tss", tss)

        # Add triples (example model - adjust as needed)
        temp_graph.add((snippet, RDF.type, tss.Snippet))
        temp_graph.add((snippet, tss.about, template))
        temp_graph.add((snippet, tss.from_, Literal(from_time.toPython(), datatype=xsd.dateTime)))
        temp_graph.add((snippet, tss.to, Literal(to_time.toPython(), datatype=xsd.dateTime)))
        temp_graph.add((snippet, tss.pointType, Literal(pointType)))
        temp_graph.add((snippet, tss.points, Literal(pointsJson)))

        temp_graph.add((template, RDF.type, tss.PointTemplate))
        temp_graph.add((template, SOSA.madeBySensor, sensor))
        temp_graph.add((template, SOSA.observedProperty, observedProperty))

        # Serialize
        ttl_str = temp_graph.serialize(format='turtle')

        # Append to file
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(ttl_str)
            f.write("\n")


        
def main():
    print("Starting processing...")
    start_time = time.perf_counter()
    original_graph = load_graph(input_path)
    result = process_graph(original_graph)
    divide_data(result)
    end_time = time.perf_counter()
    print(f"Processing completed in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()

