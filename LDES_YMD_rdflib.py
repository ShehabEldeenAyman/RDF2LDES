from rdflib import Graph, Namespace, Literal
from rdflib.namespace import RDF, XSD
from datetime import datetime
import os
import time

input_path = "Mol_Sluis_Dessel_data_prettified.ttl"
base_path = "./data"

def process_graph(input_path):
    SOSA = Namespace("http://www.w3.org/ns/sosa/")
    EX = Namespace("http://example.com/")
    XSD_NS = Namespace("http://www.w3.org/2001/XMLSchema#")

    g = Graph()
    g.parse(input_path, format="turtle", publicID="https://example.com/")

    os.makedirs(base_path, exist_ok=True)

    for obs in g.subjects(RDF.type, SOSA.Observation):
        id_ = g.value(obs, EX.id)
        result_value = g.value(obs, SOSA.hasSimpleResult)
        property_ = g.value(obs, SOSA.observedProperty)
        time_ = g.value(obs, SOSA.resultTime)
        if not (id_ and result_value and property_ and time_):
            continue

        dt = datetime.fromisoformat(str(time_.toPython()))
        year_str = str(dt.year)
        month_str = f"{dt.month:02d}"
        day_str = f"{dt.day:02d}"
        file_path = os.path.join(base_path, year_str, month_str, day_str, "readings.nt")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        temp_graph = Graph()
        temp_graph.bind("sosa", SOSA)
        temp_graph.bind("ex", EX)
        temp_graph.add((obs, RDF.type, SOSA.Observation))
        temp_graph.add((obs, EX.id, Literal(id_, datatype=XSD.int)))
        temp_graph.add((obs, SOSA.hasSimpleResult, Literal(result_value, datatype=XSD.float)))
        temp_graph.add((obs, SOSA.observedProperty, property_))
        temp_graph.add((obs, SOSA.resultTime, Literal(time_.toPython(), datatype=XSD.dateTime)))

        with open(file_path, "a", encoding="utf-8") as f:
            f.write(temp_graph.serialize(format="nt"))

def main():
    start = time.perf_counter()
    process_graph(input_path)
    end = time.perf_counter()
    print(f"Processing completed in {end - start:.2f} seconds.")

if __name__ == "__main__":
    main()
