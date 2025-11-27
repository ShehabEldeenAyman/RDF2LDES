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


input_path = "./sources/Mol_Sluis_Dessel_data_TSS_per_day.ttl"
base_path = "./data" 

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

