from rdflib import Graph, Namespace, Literal, URIRef, BNode
from rdflib.namespace import XSD, RDF
import os
import time
from datetime import date, timedelta

base_path = "./data"

AS = Namespace("https://www.w3.org/ns/activitystreams#")
LDES = Namespace("https://w3id.org/ldes#")
TREE = Namespace("https://w3id.org/tree#")

eventstream_uri = URIRef("https://shehabeldeenayman.github.io/Mol_sluis_Dessel_Usecase/")
view_uri = URIRef("https://shehabeldeenayman.github.io/Mol_sluis_Dessel_Usecase/ldes/")

# -----------------------------
# Base graph builder
# -----------------------------
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
    g.add((eventstream_uri, TREE.view, view_uri))

    g.add((retention_bn, RDF.type, LDES.LatestVersionSubset))
    g.add((retention_bn, LDES.amount, Literal(1, datatype=XSD.integer)))

    return g

# -----------------------------
# Year-level relations
# -----------------------------
def add_year_relations(g, year):
    bn_ge = BNode()
    bn_lt = BNode()

    g.add((bn_ge, RDF.type, TREE.GreaterThanOrEqualToRelation))
    g.add((bn_ge, TREE.node, URIRef(f"{view_uri}{year}/")))
    g.add((bn_ge, TREE.path, AS.published))
    g.add((bn_ge, TREE.value, Literal(year, datatype=XSD.gYear)))

    g.add((bn_lt, RDF.type, TREE.LessThanRelation))
    g.add((bn_lt, TREE.node, URIRef(f"{view_uri}{year}/")))
    g.add((bn_lt, TREE.path, AS.published))
    g.add((bn_lt, TREE.value, Literal(year + 1, datatype=XSD.gYear)))

# -----------------------------
# Month-level relations
# -----------------------------
def add_month_relations(g, year, month):
    bn_ge = BNode()
    bn_lt = BNode()

    month_str = f"{year}-{month:02d}"

    next_year = year if month < 12 else year + 1
    next_month = month + 1 if month < 12 else 1
    next_month_str = f"{next_year}-{next_month:02d}"

    month_uri = URIRef(f"{view_uri}{year}/{month:02d}/")

    g.add((bn_ge, RDF.type, TREE.GreaterThanOrEqualToRelation))
    g.add((bn_ge, TREE.node, month_uri))
    g.add((bn_ge, TREE.path, AS.published))
    g.add((bn_ge, TREE.value, Literal(month_str, datatype=XSD.gYearMonth)))

    g.add((bn_lt, RDF.type, TREE.LessThanRelation))
    g.add((bn_lt, TREE.node, month_uri))
    g.add((bn_lt, TREE.path, AS.published))
    g.add((bn_lt, TREE.value, Literal(next_month_str, datatype=XSD.gYearMonth)))

# -----------------------------
# Day-level relations
# -----------------------------
def add_day_relations(g, year, month, day):
    bn_ge = BNode()
    bn_lt = BNode()

    this_day = date(year, month, day)
    next_day = this_day + timedelta(days=1)

    day_str = this_day.isoformat()     # YYYY-MM-DD
    next_day_str = next_day.isoformat()

    day_uri = URIRef(f"{view_uri}{year}/{month:02d}/{day:02d}/")

    # >= YYYY-MM-DD
    g.add((bn_ge, RDF.type, TREE.GreaterThanOrEqualToRelation))
    g.add((bn_ge, TREE.node, day_uri))
    g.add((bn_ge, TREE.path, AS.published))
    g.add((bn_ge, TREE.value, Literal(day_str, datatype=XSD.date)))

    # < next day
    g.add((bn_lt, RDF.type, TREE.LessThanRelation))
    g.add((bn_lt, TREE.node, day_uri))
    g.add((bn_lt, TREE.path, AS.published))
    g.add((bn_lt, TREE.value, Literal(next_day_str, datatype=XSD.date)))

# -----------------------------
# Main logic
# -----------------------------
def main():
    start = time.perf_counter()

    # ===================================
    #   YEAR-LEVEL FILE
    # ===================================
    g_years = create_base_graph()
    years = sorted(
        int(name) for name in os.listdir(base_path)
        if os.path.isdir(os.path.join(base_path, name))
        and name.isdigit() and len(name) == 4
    )

    print("Detected years:", years)

    for year in years:
        add_year_relations(g_years, year)

    g_years.serialize(os.path.join(base_path, "all_years_metadata.trig"), format="trig")
    print("Saved: all_years_metadata.trig")

    # ===================================
    #   MONTH & DAY-LEVEL FILES
    # ===================================
    for year in years:
        year_path = os.path.join(base_path, str(year))

        # Detect month folders
        months = sorted(
            int(m) for m in os.listdir(year_path)
            if os.path.isdir(os.path.join(year_path, m))
            and m.isdigit() and len(m) == 2
            and 1 <= int(m) <= 12
        )
        print(f"{year}: months → {months}")

        # --------------------------
        # MONTH-LEVEL FILE
        # --------------------------
        g_months = create_base_graph()

        for m in months:
            add_month_relations(g_months, year, m)

        g_months.serialize(
            os.path.join(year_path, f"{year}_months_metadata.trig"),
            format="trig"
        )
        print(f"Saved: {year}_months_metadata.trig")

        # --------------------------
        # DAY-LEVEL FILES PER MONTH
        # --------------------------
        for m in months:
            month_path = os.path.join(year_path, f"{m:02d}")

            # Detect valid day folders (must contain readings.ttl)
            days = sorted(
                int(d) for d in os.listdir(month_path)
                if os.path.isdir(os.path.join(month_path, d))
                and d.isdigit() and len(d) == 2
                and os.path.isfile(os.path.join(month_path, d, "readings.ttl"))
            )

            print(f"  {year}-{m:02d}: days → {days}")

            g_days = create_base_graph()

            for d in days:
                add_day_relations(g_days, year, m, d)

            day_output_file = os.path.join(
                month_path,
                f"{year}-{m:02d}_days_metadata.trig"
            )

            g_days.serialize(day_output_file, format="trig")
            print(f"  Saved: {year}-{m:02d}_days_metadata.trig")

    print(f"Completed in {time.perf_counter() - start:.2f}s")


if __name__ == "__main__":
    main()
