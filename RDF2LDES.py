from rdflib import Graph,URIRef,Namespace,BNode,Literal
from rdflib.namespace import XSD,RDF
import pandas as pd
import argparse
from collections import defaultdict
from datetime import datetime
import json

current_day = ((datetime.now()).replace(hour=0, minute=0, second=0)).strftime("%Y-%m-%dT%H:%M:%S")

def load_graph(input_path):
    g = Graph()
    g.parse(input_path, format="turtle", publicID="https://example.org/")
    return g
    
def query_graph_before(graph):
    q_before = f"""
PREFIX sosa: <http://www.w3.org/ns/sosa/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

CONSTRUCT {{
  ?obs a sosa:Observation ;
       ?p ?o .
}}
WHERE {{
  ?obs a sosa:Observation ;
       ?p ?o ;
       sosa:resultTime ?time .
  FILTER ( ?time <= "{current_day}"^^xsd:dateTime )
}}
"""
    g_before = graph.query(q_before).graph
    return g_before

def query_graph_after(graph):
    q_after = f"""
PREFIX sosa: <http://www.w3.org/ns/sosa/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

CONSTRUCT {{
  ?obs a sosa:Observation ;
       ?p ?o .
}}
WHERE {{
  ?obs a sosa:Observation ;
       ?p ?o ;
       sosa:resultTime ?time .
  FILTER ( ?time > "{current_day}"^^xsd:dateTime )
}}
"""
    g_after = graph.query(q_after).graph
    return g_after

def save_graph(graph, output_path):
    graph.serialize(destination=output_path, format="turtle")

def main():
    input_path = "rdf.ttl"
    #output_path = "rdf2ldes.ttl"
    g = load_graph(input_path)
    
    g_before = query_graph_before(g)
    g_after = query_graph_after(g)
    
    save_graph(g_before, "rdf2ldes_before.ttl")
    save_graph(g_after, "rdf2ldes_after.ttl")

if __name__ == "__main__":
    main()