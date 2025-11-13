from rdflib import Graph, Namespace, Literal
from rdflib.namespace import RDF, XSD
from datetime import datetime
from collections import defaultdict
import os
import time

input_path = "./sources/Mol_Sluis_Dessel_data_prettified.ttl"
base_path = "./data" 

SOSA = Namespace("http://www.w3.org/ns/sosa/")
EX = Namespace("http://example.com/")

def extract_observations(g: Graph):
    """
    Iterate the graph and extract observations without SPARQL.
    Returns a list of tuples: (obs, id, result, property, time_as_datetime)
    Skips observations missing required fields.
    """
    observations = []
    for obs in g.subjects(RDF.type, SOSA.Observation):
        # try to get expected properties (may be URIs or Literals)
        id_node = g.value(obs, EX.id) or g.value(obs, EX['id'])  # fallback
        result_node = g.value(obs, SOSA.hasSimpleResult)
        prop_node = g.value(obs, SOSA.observedProperty)
        time_node = g.value(obs, SOSA.resultTime)

        # skip incomplete observations
        if None in (id_node, result_node, prop_node, time_node):
            continue

        # convert id to python int if possible (keep as str otherwise)
        try:
            id_val = id_node.toPython()
        except Exception:
            id_val = str(id_node)

        # convert result to float if possible
        try:
            result_val = float(result_node.toPython())
        except Exception:
            # try str -> float fallback
            try:
                result_val = float(str(result_node))
            except Exception:
                result_val = str(result_node)

        # property: could be a URIRef or a Literal; keep as string/URI
        prop_val = prop_node.toPython() if hasattr(prop_node, "toPython") else str(prop_node)

        # time: convert to datetime
        try:
            time_val = time_node.toPython()
            if not isinstance(time_val, datetime):
                # try parsing ISO string
                time_val = datetime.fromisoformat(str(time_val))
        except Exception:
            # last resort: parse string
            time_val = datetime.fromisoformat(str(time_node))

        observations.append((obs, id_val, result_val, prop_val, time_val))

    # If you care about ordering by time (like your SPARQL ORDER BY), do it here:
    #observations.sort(key=lambda tup: tup[4])
    return observations


def divide_data_monthly(observations):
    """
    Group observations by (year, month) and write one file per month.
    observations: list of (obs, id, result, property, time(datetime))
    """
    grouped = defaultdict(list)
    for obs, id_, result_value, property_, time_ in observations:
        key = (time_.year, time_.month)
        grouped[key].append((obs, id_, result_value, property_, time_))

    for (year, month), items in grouped.items():
        file_path = os.path.join(base_path, f"{year}/{month:02d}/readings.nt")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        temp_graph = Graph()
        temp_graph.bind("sosa", SOSA)
        temp_graph.bind("ex", EX)
        temp_graph.bind("xsd", XSD)

        for obs, id_, result_value, property_, time_ in items:
            temp_graph.add((obs, RDF.type, SOSA.Observation))
            # put back id as an xsd:int if it's int-like
            try:
                temp_graph.add((obs, EX.id, Literal(int(id_), datatype=XSD.int)))
            except Exception:
                temp_graph.add((obs, EX.id, Literal(id_)))
            # result as xsd:float if possible
            try:
                temp_graph.add((obs, SOSA.hasSimpleResult, Literal(float(result_value), datatype=XSD.float)))
            except Exception:
                temp_graph.add((obs, SOSA.hasSimpleResult, Literal(result_value)))
            # observedProperty: if it looks like a URI, try to keep it as URIRef, otherwise Literal
            try:
                # if property_ is a URI string, create a URIRef; otherwise literal
                from rdflib import URIRef
                prop_obj = URIRef(property_) if isinstance(property_, str) and property_.startswith("http") else Literal(property_)
            except Exception:
                prop_obj = Literal(property_)
            temp_graph.add((obs, SOSA.observedProperty, prop_obj))
            temp_graph.add((obs, SOSA.resultTime, Literal(time_, datatype=XSD.dateTime)))

        # write the whole month's graph at once
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(temp_graph.serialize(format='nt'))


def main():
    start = time.perf_counter()
    g = Graph()
    g.parse(input_path, format="turtle", publicID="https://example.com/")

    observations = extract_observations(g)
    divide_data_monthly(observations)

    print(f"Processing completed in {time.perf_counter() - start:.2f} seconds.")


if __name__ == "__main__":
    main()
