from rdflib import Graph, Namespace, Literal
from rdflib.namespace import XSD, RDF
from collections import defaultdict
from datetime import datetime
import os
import time

# --- Config ---
input_path = "./sources/Mol_Sluis_Dessel_data_prettified.ttl"
base_path = "./data"

# --- Namespaces ---
SOSA = Namespace("http://www.w3.org/ns/sosa/")
EX = Namespace("http://example.com/")
XSD = Namespace("http://www.w3.org/2001/XMLSchema#")


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
        file_path = os.path.join(base_path, f"{year}/{month:02d}/{day:02d}/readings.nt")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        temp_graph = Graph()
        temp_graph.bind("sosa", SOSA)
        temp_graph.bind("ex", EX)
        temp_graph.bind("xsd", XSD)

        for obs, id_, result_value, property_, time_ in daily_obs:
            temp_graph.add((obs, RDF.type, SOSA.Observation))
            temp_graph.add((obs, EX.id, Literal(id_, datatype=XSD.int)))
            temp_graph.add((obs, SOSA.hasSimpleResult, Literal(result_value, datatype=XSD.float)))
            temp_graph.add((obs, SOSA.observedProperty, Literal(property_)))
            temp_graph.add((obs, SOSA.resultTime, Literal(time_, datatype=XSD.dateTime)))

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(temp_graph.serialize(format="nt"))


def main():
    start_time = time.perf_counter()
    g = load_graph(input_path)
    observations = extract_observations(g)
    divide_data(observations)
    end_time = time.perf_counter()
    print(f"Processing completed in {end_time - start_time:.2f} seconds.")


if __name__ == "__main__":
    main()
