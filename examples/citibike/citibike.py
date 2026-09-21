#!/usr/bin/env python3
"""NYC Citi Bike trips in Sail, analysed with Nutmeg.

Part 1 replicates Neo4j's "Aura Graph Analytics with Spark" tutorial
(https://neo4j.com/docs/graph-data-science-client/current/tutorials/graph-analytics-serverless-spark/)
step for step: the same Kaggle file, the same projection, PageRank, and the
same result shape. Part 2 goes further: trip duration as the edge weight,
Leiden station communities checked for geographic coherence, betweenness,
results written back to Delta, and A* between two named stations on the
stations' coordinates from Citi Bike's own trip archive.

Every result is checked against an independent reference computed in this
client with NumPy/SciPy and NetworkX, never with Nutmeg.

Usage (a nutmeg-server must be listening; see run.sh):

    python citibike.py --remote sc://127.0.0.1:50051 --work /abs/path/work

`--work` must be a path the server can read and write: the server reads the
CSV and writes the Delta tables there. The script downloads the data itself;
nothing from either dataset is committed to the repository.
"""
from __future__ import annotations

import argparse
import csv as csv_module
import json
import math
import shutil
import sys
import urllib.request
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

KAGGLE_URL = "https://www.kaggle.com/api/v1/datasets/download/gabrielramos87/bike-trips"
KAGGLE_CSV = "New York Citibike Trips.csv"
CITIBIKE_URL = "https://s3.amazonaws.com/tripdata/2018-citibike-tripdata.zip"
CITIBIKE_MAY = "2018-citibike-tripdata/201805-citibike-tripdata.csv"

# The Grust A* kernel's Earth radius (grust-algorithms/src/astar.rs), so the
# edge lengths computed here in SQL and the kernel's heuristic use one sphere.
EARTH_RADIUS_METRES = 6_371_008.8
DAMPING = 0.85
MAX_TRIP_MINUTES = 180  # Part 2 excludes trips longer than three hours
MIN_TRIPS_PER_LINK = 5  # Part 2 route graphs keep links ridden at least this often
LEIDEN_SEED = 42
PERMUTATIONS = 1000
# Stations whose coordinates lie outside this box are left out of the
# geographic analyses (the 2018 data has one mobile station located in Montreal).
NYC_LAT = (40.4, 41.0)
NYC_LON = (-74.3, -73.6)

# The Kaggle file's columns, in order.
TRIPS_SCHEMA = (
    "start_time TIMESTAMP, stop_time TIMESTAMP, start_station_id BIGINT, start_station_name STRING, "
    "end_station_id BIGINT, end_station_name STRING, user_type STRING, bike_id BIGINT, "
    "gender STRING, age INT, trip_duration DOUBLE")
# Citi Bike's 2018 legacy layout. Only the station columns are used; rider
# columns are read as strings and never selected.
CITIBIKE_SCHEMA = (
    "tripduration BIGINT, starttime STRING, stoptime STRING, "
    "start_station_id BIGINT, start_station_name STRING, start_latitude DOUBLE, start_longitude DOUBLE, "
    "end_station_id BIGINT, end_station_name STRING, end_latitude DOUBLE, end_longitude DOUBLE, "
    "bikeid STRING, usertype STRING, birth_year STRING, gender STRING")


# ---------------------------------------------------------------- reporting

class Report:
    """Collects the run's output as Markdown (output.md) and numbers (results.json)."""

    def __init__(self, directory: Path):
        self.directory = directory
        directory.mkdir(parents=True, exist_ok=True)
        self.lines: list[str] = []
        self.numbers: dict = {}

    def emit(self, text: str = "") -> None:
        print(text, flush=True)
        self.lines.append(text)

    def h(self, level: int, text: str) -> None:
        self.emit()
        self.emit("#" * level + " " + text)
        self.emit()

    def table(self, frame: pd.DataFrame, floatfmt: str = "{:.6g}") -> None:
        cols = list(frame.columns)
        self.emit("| " + " | ".join(cols) + " |")
        self.emit("|" + "|".join("---" for _ in cols) + "|")
        for row in frame.itertuples(index=False):
            cells = []
            for value in row:
                if isinstance(value, (float, np.floating)):
                    cells.append(floatfmt.format(value))
                else:
                    cells.append(str(value))
            self.emit("| " + " | ".join(cells) + " |")
        self.emit()

    def code(self, text: str) -> None:
        self.emit("```")
        for line in text.rstrip().splitlines():
            self.emit(line)
        self.emit("```")
        self.emit()

    def record(self, key: str, value) -> None:
        if isinstance(value, (np.integer,)):
            value = int(value)
        if isinstance(value, (np.floating,)):
            value = float(value)
        self.numbers[key] = value

    def save(self) -> None:
        (self.directory / "output.md").write_text("\n".join(self.lines) + "\n")
        (self.directory / "results.json").write_text(
            json.dumps(self.numbers, indent=2, sort_keys=True) + "\n")


# ---------------------------------------------------------------- data

def download(url: str, dest: Path) -> None:
    """GET (a HEAD on Kaggle's URL answers 404; a GET redirects and serves the zip)."""
    if dest.exists():
        return
    print(f"downloading {url}", flush=True)
    tmp = dest.with_suffix(".part")
    with urllib.request.urlopen(url) as response, open(tmp, "wb") as out:
        shutil.copyfileobj(response, out)
    tmp.rename(dest)


def prepare_data(work: Path) -> tuple[Path, Path]:
    data = work / "data"
    data.mkdir(parents=True, exist_ok=True)
    kaggle_zip = data / "bike-trips.zip"
    download(KAGGLE_URL, kaggle_zip)
    trips_dir = data / "bike_trips_data"
    if not (trips_dir / KAGGLE_CSV).exists():
        with zipfile.ZipFile(kaggle_zip) as z:
            z.extractall(trips_dir)
    citibike_zip = data / "2018-citibike-tripdata.zip"
    download(CITIBIKE_URL, citibike_zip)
    may = data / "201805-citibike-tripdata.csv"
    if not may.exists():
        with zipfile.ZipFile(citibike_zip) as z, z.open(CITIBIKE_MAY) as src, open(may, "wb") as dst:
            shutil.copyfileobj(src, dst)
    return trips_dir, may


# ---------------------------------------------------------------- references

def pagerank_reference(src, dst, weight, nodes, damping=DAMPING, tol=1e-14, max_iter=100_000):
    """Power iteration on the column-stochastic matrix built from the edge list.

    Parallel edges are summed (scipy adds duplicate coordinates), a dangling
    node's mass is spread uniformly over all nodes, teleportation is uniform,
    and iteration stops when the L1 change is below `tol`."""
    import scipy.sparse as sp

    index = {node: i for i, node in enumerate(nodes)}
    n = len(nodes)
    s = np.fromiter((index[x] for x in src), dtype=np.int64, count=len(src))
    t = np.fromiter((index[x] for x in dst), dtype=np.int64, count=len(dst))
    w = np.asarray(weight, dtype=np.float64)
    out = np.bincount(s, weights=w, minlength=n)
    matrix = sp.csr_matrix((w / out[s], (t, s)), shape=(n, n))
    dangling = out == 0
    x = np.full(n, 1.0 / n)
    residual = float("inf")
    for iteration in range(1, max_iter + 1):
        new = damping * (matrix @ x + x[dangling].sum() / n) + (1.0 - damping) / n
        residual = float(np.abs(new - x).sum())
        x = new
        if residual < tol:
            break
    return pd.Series(x, index=nodes), iteration, residual


def pagerank_networkx(src, dst, weight, nodes, damping=DAMPING):
    import networkx as nx

    g = nx.DiGraph()
    g.add_nodes_from(nodes)
    frame = pd.DataFrame({"s": src, "t": dst, "w": weight}).groupby(["s", "t"], as_index=False).w.sum()
    g.add_weighted_edges_from(frame.itertuples(index=False, name=None))
    # networkx stops when the L1 change is below N * tol; dangling mass goes
    # to the personalization vector, uniform by default.
    scores = nx.pagerank(g, alpha=damping, weight="weight", tol=1e-16, max_iter=100_000)
    return pd.Series(scores).reindex(nodes)


def compare(label: str, ours: pd.Series, reference: pd.Series, report: Report, key: str) -> dict:
    ours = ours.sort_index()
    reference = reference.reindex(ours.index)
    diff = (ours - reference).abs()
    result = {
        "nodes": int(len(ours)),
        "max_abs_diff": float(diff.max()),
        "l1_diff": float(diff.sum()),
        "max_rel_diff": float((diff / reference.abs()).max()),
    }
    for k in (10, 50):
        a = list(ours.sort_values(ascending=False, kind="mergesort").index[:k])
        b = list(reference.sort_values(ascending=False, kind="mergesort").index[:k])
        result[f"top{k}_same_order"] = a == b
        result[f"top{k}_same_set"] = set(a) == set(b)
    report.record(key, result)
    report.emit(
        f"- {label}: {result['nodes']} nodes; max |diff| = {result['max_abs_diff']:.3e}, "
        f"L1 diff = {result['l1_diff']:.3e}, max relative diff = {result['max_rel_diff']:.3e}; "
        f"top 10 same order: {result['top10_same_order']}; "
        f"top 50 same order: {result['top50_same_order']} (same set: {result['top50_same_set']})")
    return result


def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 2 * EARTH_RADIUS_METRES * np.arcsin(np.sqrt(a))


# ---------------------------------------------------------------- nutmeg helpers

def stage_edges(df, graph: str, source: str, target: str) -> None:
    """Stage `df`'s rows as `graph`'s edges; nodes are the edges' endpoints."""
    spark = df.sparkSession
    empty_nodes = spark.createDataFrame([], "node_id string")
    (empty_nodes.write.format("nutmeg").option("graph", graph)
        .option("part", "nodes").mode("overwrite").save())
    (df.write.format("nutmeg").option("graph", graph).option("part", "edges")
        .option("sourceColumn", source).option("targetColumn", target)
        .mode("overwrite").save())


def run(spark, graph: str, algorithm: str, **options):
    reader = spark.read.format("nutmeg").option("graph", graph).option("algorithm", algorithm)
    for key, value in options.items():
        reader = reader.option(key, value if isinstance(value, str) else json.dumps(value))
    return reader.load()


# ---------------------------------------------------------------- main

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--remote", default="sc://127.0.0.1:50051")
    parser.add_argument("--work", required=True, type=Path,
                        help="absolute directory for data and Delta tables, readable by the server")
    parser.add_argument("--results", type=Path, default=Path(__file__).parent / "results")
    parser.add_argument("--versions", type=Path, help="JSON file of component versions (run.sh writes it)")
    parser.add_argument("--from-station", default="W 106 St & Central Park West")
    parser.add_argument("--to-station", default="Atlantic Ave & Fort Greene Pl")
    args = parser.parse_args()
    work = args.work.resolve()

    from pyspark.sql import SparkSession
    import pyspark

    report = Report(args.results)
    trips_dir, may_csv = prepare_data(work)
    delta = lambda name: str(work / "delta" / name)  # noqa: E731

    spark = SparkSession.builder.remote(args.remote).getOrCreate()

    report.emit("# Citi Bike showcase: captured output")
    report.emit()
    report.emit("Generated by `citibike.py`; every number below is from this run.")
    versions = json.loads(args.versions.read_text()) if args.versions else {}
    versions["pyspark (client)"] = pyspark.__version__
    versions["python (client)"] = sys.version.split()[0]
    versions["server Spark version (spark.version)"] = spark.version
    report.h(2, "Versions")
    report.table(pd.DataFrame(sorted(versions.items()), columns=["component", "version"]))
    report.record("versions", versions)

    # ============================================================ PART 1
    report.h(2, "Part 1: the tutorial, step for step")

    # Step 1. Load the CSV, as the tutorial does, into a Delta table without
    # gender and age.
    report.h(3, "Step 1: the Kaggle CSV into a Delta table")
    # The tutorial infers the schema. Try that first; if this Sail build cannot
    # infer it, say so and give the same columns explicitly.
    try:
        raw = spark.read.csv(str(trips_dir), header=True, inferSchema=True)
        raw_columns = raw.columns
        report.emit("- Schema inferred, as in the tutorial (`inferSchema=True`).")
        report.record("part1.infer_schema", "ok")
    except Exception as error:  # noqa: BLE001 - reported verbatim below
        message = str(error).splitlines()[0]
        report.emit(f"- `inferSchema=True` failed in this Sail build: `{message}`. "
                    "The schema is given explicitly instead:")
        report.record("part1.infer_schema", message)
        raw = spark.read.csv(str(trips_dir), header=True, schema=TRIPS_SCHEMA)
        raw_columns = raw.columns
        report.emit()
        report.code(TRIPS_SCHEMA)
    trips = raw.drop("gender", "age")
    trips.write.format("delta").mode("overwrite").save(delta("bike_trips"))
    spark.read.format("delta").load(delta("bike_trips")).createOrReplaceTempView("bike_trips")
    stored = spark.table("bike_trips")
    row_count = stored.count()
    report.record("part1.csv_columns", raw_columns)
    report.record("part1.delta_columns", stored.columns)
    report.record("part1.rows", row_count)
    report.emit(f"- CSV columns: {', '.join(raw_columns)}")
    report.emit(f"- Delta table `bike_trips` at `delta/bike_trips`, columns: {', '.join(stored.columns)}")
    report.emit(f"- Rows: {row_count}")
    report.emit("- `gender` and `age` are dropped before the write and exist nowhere in Sail's tables.")
    report.emit()
    report.code("\n".join(f"{f.name}: {f.dataType.simpleString()}" for f in stored.schema.fields))

    # Step 2. Stage the tutorial's projection as the graph's edges.
    report.h(3, "Step 2: stage the projection as edges")
    source_target_pairs = spark.sql(
        "SELECT start_station_id AS sourceNode, end_station_id AS targetNode FROM bike_trips")
    stage_edges(source_target_pairs, "bike_trips", "sourceNode", "targetNode")
    stats = run(spark, "bike_trips", "projectionStats").toPandas()
    report.table(stats)
    report.record("part1.projection_stats", stats.to_dict(orient="records"))

    # Step 3. PageRank, read back as a DataFrame.
    report.h(3, "Step 3: PageRank")
    pagerank = run(spark, "bike_trips", "pagerank")
    pagerank.createOrReplaceTempView("pagerank")
    result = spark.sql("SELECT CAST(nodeId AS BIGINT) AS nodeId, score AS pagerank FROM pagerank")
    report.emit("The tutorial's result shape, `nodeId long, pagerank double`:")
    report.emit()
    report.code(result.schema.simpleString())
    meta = spark.sql(
        "SELECT MIN(iterations) AS iterations, MIN(CAST(converged AS INT)) AS converged, "
        "MAX(residual) AS residual, COUNT(*) AS nodes, SUM(score) AS score_sum FROM pagerank"
    ).toPandas()
    report.table(meta, floatfmt="{:.12g}")
    report.record("part1.pagerank_run", meta.to_dict(orient="records")[0])

    # Step 4. Names joined in one query, in the same engine.
    report.h(3, "Step 4: top stations, scores joined to names in one SQL query")
    stations_cte = """
        stations AS (
            SELECT station_id, MIN(station_name) AS station_name
            FROM (SELECT start_station_id AS station_id, start_station_name AS station_name FROM bike_trips
                  UNION ALL
                  SELECT end_station_id, end_station_name FROM bike_trips) AS s
            GROUP BY station_id)"""
    top_sql = f"""
        WITH {stations_cte},
        arrivals AS (SELECT end_station_id AS station_id, COUNT(*) AS trips_in FROM bike_trips GROUP BY end_station_id)
        SELECT s.station_name, CAST(pr.nodeId AS BIGINT) AS nodeId, pr.score AS pagerank, a.trips_in
        FROM nutmeg_pagerank('bike_trips') AS pr
        JOIN stations AS s ON s.station_id = CAST(pr.nodeId AS BIGINT)
        JOIN arrivals AS a ON a.station_id = s.station_id
        ORDER BY pagerank DESC
        LIMIT 10"""
    report.code(top_sql.strip())
    top = spark.sql(top_sql).toPandas()
    report.table(top)
    report.record("part1.top10", top.to_dict(orient="records"))

    dangling = spark.sql(f"""
        WITH {stations_cte}
        SELECT s.station_id, s.station_name,
               (SELECT COUNT(*) FROM bike_trips t WHERE t.end_station_id = s.station_id) AS trips_in
        FROM stations s
        WHERE s.station_id NOT IN (SELECT start_station_id FROM bike_trips)
        ORDER BY s.station_id""").toPandas()
    report.emit(f"Stations with arrivals but no departures (dangling in the directed graph): {len(dangling)}")
    report.emit()
    report.table(dangling)
    report.record("part1.dangling", dangling.to_dict(orient="records"))

    # Reference check, independent of Sail: pandas reads the CSV itself.
    report.h(3, "Reference check: PageRank computed outside Nutmeg")
    csv = pd.read_csv(trips_dir / KAGGLE_CSV,
                      usecols=["start_station_id", "end_station_id", "trip_duration"])
    report.emit(f"The reference reads the CSV with pandas, not through Sail: {len(csv)} rows.")
    report.emit()
    nodes = sorted(set(csv.start_station_id) | set(csv.end_station_id))
    ref, ref_iters, ref_res = pagerank_reference(
        csv.start_station_id.to_numpy(), csv.end_station_id.to_numpy(), np.ones(len(csv)), nodes)
    nx_ref = pagerank_networkx(
        csv.start_station_id.to_numpy(), csv.end_station_id.to_numpy(), np.ones(len(csv)), nodes)
    report.record("part1.reference", {"iterations": ref_iters, "final_l1_change": ref_res})
    report.emit(f"- NumPy/SciPy power iteration: {ref_iters} iterations, final L1 change {ref_res:.3e}; "
                "parallel edges summed, dangling mass spread uniformly, damping 0.85.")
    compare("NetworkX `pagerank` vs NumPy reference", nx_ref, ref, report, "part1.check.networkx_vs_numpy")

    ours_default = result.toPandas().set_index("nodeId").pagerank
    compare("Nutmeg, default tolerance (1e-8, L1) vs NumPy reference", ours_default, ref, report,
            "part1.check.default")
    tight = run(spark, "bike_trips", "pagerank", tolerance=1e-13, maxIterations=10000).toPandas()
    report.record("part1.pagerank_tight_run", {
        "iterations": int(tight.iterations.min()), "converged": bool(tight.converged.all()),
        "residual": float(tight.residual.max())})
    tight.nodeId = tight.nodeId.astype(np.int64)
    compare(f"Nutmeg, tolerance 1e-13 ({int(tight.iterations.min())} iterations, "
            f"converged {bool(tight.converged.all())}) vs NumPy reference",
            tight.set_index("nodeId").score, ref, report, "part1.check.tight")

    pairs = csv.groupby(["start_station_id", "end_station_id"]).size().reset_index()
    collapsed, _, _ = pagerank_reference(
        pairs.start_station_id.to_numpy(), pairs.end_station_id.to_numpy(), np.ones(len(pairs)), nodes)
    report.emit()
    report.emit(f"Parallel edges: {len(csv)} trips run between {len(pairs)} distinct ordered station pairs "
                f"({int((csv.start_station_id == csv.end_station_id).sum())} trips start and end at the same station).")
    compare("Nutmeg (default) vs a reference that collapses parallel edges to one", ours_default, collapsed,
            report, "part1.check.collapsed")
    report.emit()

    # ============================================================ PART 2
    report.h(2, "Part 2: further, in the same engine")

    # Station coordinates from Citi Bike's own May 2018 file.
    report.h(3, "Station coordinates")
    with open(may_csv, newline="") as fh:
        header = next(csv_module.reader(fh))
    coord_columns = [c for c in header if "latitude" in c or "longitude" in c]
    report.emit(f"- Citi Bike May 2018 header: {', '.join(header)}")
    report.emit(f"- Coordinate columns found: {', '.join(coord_columns) or 'none'}")
    report.emit()
    report.record("part2.citibike_header", header)
    report.record("part2.coordinate_columns", coord_columns)
    expected = ["start station id", "start station latitude", "start station longitude",
                "end station id", "end station latitude", "end station longitude"]
    if len(header) != 15 or header[3:7] != ["start station id", "start station name",
                                            "start station latitude", "start station longitude"] \
            or header[7:11] != ["end station id", "end station name",
                                "end station latitude", "end station longitude"]:
        report.emit(f"- The header is not the expected 2018 layout ({', '.join(expected)} at positions 4-11); "
                    "geographic checks and A* are skipped.")
        have_coordinates = False
    else:
        have_coordinates = True
        may = spark.read.csv(str(may_csv), header=True, schema=CITIBIKE_SCHEMA)
        may.createOrReplaceTempView("citibike_may")
        # Only station columns are selected; nothing about riders is kept.
        spark.sql("""
            SELECT station_id, MIN(latitude) AS latitude, MAX(latitude) AS max_latitude,
                   MIN(longitude) AS longitude, MAX(longitude) AS max_longitude
            FROM (SELECT start_station_id AS station_id, start_latitude AS latitude,
                         start_longitude AS longitude FROM citibike_may
                  UNION ALL
                  SELECT end_station_id, end_latitude, end_longitude FROM citibike_may) AS c
            GROUP BY station_id""").write.format("delta").mode("overwrite").save(delta("station_coordinates"))
        spark.read.format("delta").load(delta("station_coordinates")).createOrReplaceTempView("station_coordinates")
        coverage = spark.sql(f"""
            WITH {stations_cte}
            SELECT COUNT(*) AS stations,
                   COUNT(c.station_id) AS with_coordinates,
                   SUM(CASE WHEN c.latitude <> c.max_latitude OR c.longitude <> c.max_longitude THEN 1 ELSE 0 END)
                       AS with_moving_coordinates,
                   SUM(CASE WHEN c.latitude BETWEEN {NYC_LAT[0]} AND {NYC_LAT[1]}
                             AND c.longitude BETWEEN {NYC_LON[0]} AND {NYC_LON[1]} THEN 1 ELSE 0 END) AS in_nyc_box
            FROM stations s LEFT JOIN station_coordinates c ON c.station_id = s.station_id""").toPandas()
        report.table(coverage)
        report.record("part2.coordinate_coverage", coverage.to_dict(orient="records")[0])
        outside = spark.sql(f"""
            WITH {stations_cte}
            SELECT s.station_id, s.station_name, c.latitude, c.longitude
            FROM stations s JOIN station_coordinates c ON c.station_id = s.station_id
            WHERE NOT (c.latitude BETWEEN {NYC_LAT[0]} AND {NYC_LAT[1]}
                       AND c.longitude BETWEEN {NYC_LON[0]} AND {NYC_LON[1]})""").toPandas()
        report.emit("Left out of the geographic analyses (coordinates outside the NYC box):")
        report.emit()
        report.table(outside, floatfmt="{:.6f}")
        report.record("part2.outside_box", outside.to_dict(orient="records"))

    # ---------------------------------------------------- weighted PageRank
    report.h(3, "PageRank weighted by trip duration")
    spark.sql(f"SELECT * FROM bike_trips WHERE trip_duration <= {MAX_TRIP_MINUTES}") \
        .createOrReplaceTempView("trips_clean")
    excluded = spark.sql(
        f"SELECT COUNT(*) AS trips_over_{MAX_TRIP_MINUTES}_min, MAX(trip_duration) AS longest_minutes "
        f"FROM bike_trips WHERE trip_duration > {MAX_TRIP_MINUTES}").toPandas()
    report.emit(f"Trips longer than {MAX_TRIP_MINUTES} minutes are left out of every weighted analysis "
                "(a bike not docked for days is not a trip's travel time):")
    report.emit()
    report.table(excluded)
    report.record("part2.excluded_long_trips", excluded.to_dict(orient="records")[0])
    stage_edges(spark.sql("SELECT start_station_id AS source, end_station_id AS target, trip_duration "
                          "FROM trips_clean"), "trips_minutes", "source", "target")
    movers_sql = f"""
        WITH {stations_cte},
        by_count AS (SELECT nodeId, score, RANK() OVER (ORDER BY score DESC) AS rank_by_trips
                     FROM nutmeg_pagerank('trips_minutes')),
        by_minutes AS (SELECT nodeId, score, RANK() OVER (ORDER BY score DESC) AS rank_by_minutes
                       FROM nutmeg_pagerank('trips_minutes', '{{"weightProperty": "trip_duration"}}')),
        arriving AS (SELECT end_station_id AS station_id, COUNT(*) AS trips_in,
                            AVG(trip_duration) AS mean_minutes_in FROM trips_clean GROUP BY end_station_id)
        SELECT s.station_name, m.rank_by_minutes, c.rank_by_trips, m.score AS pagerank_minutes,
               c.score AS pagerank_trips, a.trips_in, a.mean_minutes_in
        FROM by_minutes m JOIN by_count c ON c.nodeId = m.nodeId
        JOIN stations s ON s.station_id = CAST(m.nodeId AS BIGINT)
        JOIN arriving a ON a.station_id = s.station_id"""
    ranks = spark.sql(movers_sql).toPandas()
    report.emit("Top 10 by duration-weighted PageRank, with their rank by trip count on the same trips:")
    report.emit()
    top_w = ranks.sort_values("rank_by_minutes").head(10)
    report.table(top_w)
    report.record("part2.weighted_top10", top_w.to_dict(orient="records"))
    ranks["rise"] = ranks.rank_by_trips - ranks.rank_by_minutes
    busy = ranks[ranks.trips_in >= 1000]
    report.emit("Largest rises in rank when minutes replace trip counts, among stations with at least 1000 arrivals:")
    report.emit()
    risers = busy.sort_values("rise", ascending=False).head(10)
    report.table(risers)
    report.record("part2.weighted_risers", risers.to_dict(orient="records"))
    fallers = busy.sort_values("rise").head(5)
    report.emit("Largest falls:")
    report.emit()
    report.table(fallers)
    report.record("part2.weighted_fallers", fallers.to_dict(orient="records"))
    corr = float(np.corrcoef(ranks.mean_minutes_in, ranks.rise)[0, 1])
    report.emit(f"- Correlation between a station's mean arriving trip length and its rise in rank: {corr:.3f}")
    report.record("part2.corr_mean_minutes_vs_rise", corr)

    clean = csv[csv.trip_duration <= MAX_TRIP_MINUTES]
    ref_w, it_w, res_w = pagerank_reference(
        clean.start_station_id.to_numpy(), clean.end_station_id.to_numpy(), clean.trip_duration.to_numpy(),
        sorted(set(clean.start_station_id) | set(clean.end_station_id)))
    weighted = run(spark, "trips_minutes", "pagerank", weightProperty="trip_duration").toPandas()
    weighted.nodeId = weighted.nodeId.astype(np.int64)
    report.emit(f"- Reference (pandas on the CSV, durations summed per pair): {it_w} iterations, "
                f"final L1 change {res_w:.3e}")
    compare("Nutmeg duration-weighted PageRank (default tolerance) vs NumPy reference",
            weighted.set_index("nodeId").score, ref_w, report, "part2.check.weighted")

    # ---------------------------------------------------- Leiden communities
    report.h(3, "Station communities (Leiden)")
    # Leiden visits nodes in projection row order (shuffled by `seed`), and the
    # row order follows the order edges are staged in. Sail's scan of the Delta
    # table does not fix that order from run to run, so the trips are staged
    # sorted: the same rows as the tutorial's graph, in a reproducible order.
    stage_edges(spark.sql("SELECT start_station_id AS source, end_station_id AS target FROM bike_trips "
                          "ORDER BY start_station_id, end_station_id"), "trips_sorted", "source", "target")
    (run(spark, "trips_sorted", "leiden", orientation="undirected", seed=LEIDEN_SEED)
        .write.format("delta").mode("overwrite").save(delta("station_communities")))
    spark.read.format("delta").load(delta("station_communities")).createOrReplaceTempView("leiden")
    leiden = spark.table("leiden")
    lmeta = spark.sql("SELECT COUNT(DISTINCT communityId) AS communities, MAX(modularity) AS modularity, "
                      "MAX(levels) AS levels, MIN(CAST(converged AS INT)) AS converged FROM leiden").toPandas()
    report.emit("Leiden on the tutorial's trips (every trip one undirected edge of weight 1; staged sorted by "
                f"station so the node order, and so the result, is reproducible), seed {LEIDEN_SEED}, "
                "written to `delta/station_communities`:")
    report.emit()
    report.table(lmeta, floatfmt="{:.9f}")
    report.record("part2.leiden", lmeta.to_dict(orient="records")[0])

    import networkx as nx
    ug = nx.Graph()
    ug.add_nodes_from(nodes)
    und = csv.groupby(["start_station_id", "end_station_id"]).size().reset_index(name="w")
    und[["a", "b"]] = np.sort(und[["start_station_id", "end_station_id"]].to_numpy(), axis=1)
    und = und.groupby(["a", "b"], as_index=False).w.sum()
    ug.add_weighted_edges_from(und.itertuples(index=False, name=None))
    lpd = leiden.toPandas()
    lpd.nodeId = lpd.nodeId.astype(np.int64)
    parts = [set(g.nodeId) for _, g in lpd.groupby("communityId")]
    nx_q = nx.community.modularity(ug, parts, weight="weight")
    reported_q = float(lmeta.modularity[0])
    report.emit(f"- Modularity of Nutmeg's partition recomputed by NetworkX on the CSV's trips: {nx_q:.12f}; "
                f"Nutmeg reports {reported_q:.12f}; |diff| = {abs(nx_q - reported_q):.3e}")
    report.record("part2.check.modularity", {"networkx": nx_q, "nutmeg": reported_q,
                                             "abs_diff": abs(nx_q - reported_q)})

    community_sql = f"""
        WITH {stations_cte}
        SELECT l.communityId, COUNT(*) AS stations,
               ROUND(AVG(c.latitude), 4) AS centroid_lat, ROUND(AVG(c.longitude), 4) AS centroid_lon,
               ROUND(MIN(c.latitude), 3) AS min_lat, ROUND(MAX(c.latitude), 3) AS max_lat,
               ROUND(MIN(c.longitude), 3) AS min_lon, ROUND(MAX(c.longitude), 3) AS max_lon,
               MAX_BY(s.station_name, p.score) AS highest_pagerank_station
        FROM leiden l
        JOIN stations s ON s.station_id = CAST(l.nodeId AS BIGINT)
        JOIN pagerank p ON p.nodeId = l.nodeId
        JOIN station_coordinates c ON c.station_id = s.station_id
        WHERE c.latitude BETWEEN {NYC_LAT[0]} AND {NYC_LAT[1]} AND c.longitude BETWEEN {NYC_LON[0]} AND {NYC_LON[1]}
        GROUP BY l.communityId
        ORDER BY stations DESC""" if have_coordinates else None
    if have_coordinates:
        report.emit()
        report.emit("Communities, with their extent and highest-PageRank station (one SQL query; "
                    "stations outside the NYC box are left out, so a community made only of them "
                    "has no row):")
        report.emit()
        comm = spark.sql(community_sql).toPandas()
        report.table(comm, floatfmt="{:.4f}")
        report.record("part2.communities", comm.to_dict(orient="records"))

        coords = spark.table("station_coordinates").toPandas().set_index("station_id")
        geo = lpd.join(coords, on="nodeId")
        geo = geo[geo.latitude.between(*NYC_LAT) & geo.longitude.between(*NYC_LON)].reset_index(drop=True)

        def mean_to_centroid(labels: np.ndarray) -> float:
            frame = pd.DataFrame({"c": labels, "lat": geo.latitude, "lon": geo.longitude})
            centroids = frame.groupby("c")[["lat", "lon"]].transform("mean")
            return float(haversine(frame.lat, frame.lon, centroids.lat, centroids.lon).mean())

        observed = mean_to_centroid(geo.communityId.to_numpy())
        rng = np.random.default_rng(LEIDEN_SEED)
        baseline = np.array([mean_to_centroid(rng.permutation(geo.communityId.to_numpy()))
                             for _ in range(PERMUTATIONS)])
        centroids = geo.groupby("communityId")[["latitude", "longitude"]].mean()
        dist = np.stack([haversine(geo.latitude, geo.longitude, lat, lon)
                         for lat, lon in centroids.itertuples(index=False)], axis=1)
        nearest_own = float((centroids.index.to_numpy()[dist.argmin(axis=1)] == geo.communityId.to_numpy()).mean())
        geo_result = {
            "stations": int(len(geo)),
            "mean_metres_to_own_centroid": observed,
            "shuffled_mean": float(baseline.mean()),
            "shuffled_min": float(baseline.min()),
            "permutations": PERMUTATIONS,
            "shuffles_at_or_below_observed": int((baseline <= observed).sum()),
            "share_nearest_centroid_is_own": nearest_own,
        }
        report.record("part2.check.geography", geo_result)
        report.emit("Geographic coherence (the sanity test: the graph knows no coordinates):")
        report.emit()
        report.emit(f"- Mean distance from a station to its own community's centroid: {observed:.0f} m "
                    f"over {len(geo)} stations.")
        report.emit(f"- The same with community labels shuffled among stations ({PERMUTATIONS} shuffles, "
                    f"sizes kept): mean {baseline.mean():.0f} m, smallest {baseline.min():.0f} m; "
                    f"{geo_result['shuffles_at_or_below_observed']} shuffles at or below the observed value.")
        report.emit(f"- Share of stations whose nearest community centroid is their own community's: "
                    f"{nearest_own:.3f}")
        report.emit()
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
            fig, ax = plt.subplots(figsize=(7, 9))
            palette = plt.get_cmap("tab20")
            for i, (cid, g) in enumerate(geo.groupby("communityId")):
                ax.scatter(g.longitude, g.latitude, s=9, color=palette(i % 20), label=f"{cid} ({len(g)})")
            ax.set_aspect(1 / math.cos(math.radians(40.73)))
            ax.set_xlabel("longitude")
            ax.set_ylabel("latitude")
            ax.set_title("Leiden communities of Citi Bike stations, May 2018\n"
                         "(computed from trips only; positions from Citi Bike's station coordinates)",
                         fontsize=9)
            ax.legend(fontsize=6, markerscale=1.5, loc="upper left", title="community (stations)",
                      title_fontsize=6)
            fig.tight_layout()
            fig.savefig(args.results / "communities.png", dpi=130)
            report.emit("![Leiden communities on the map](communities.png)")
            report.emit()
        except ImportError:
            report.emit("(matplotlib not installed: no map drawn)")

    # ---------------------------------------------------- betweenness
    report.h(3, "Betweenness: hub stations on the quickest routes")
    links_sql = f"""
        SELECT start_station_id AS source, end_station_id AS target, COUNT(*) AS trips,
               CAST(ROUND(PERCENTILE(trip_duration, 0.5) * 60) AS BIGINT) AS median_seconds
        FROM trips_clean
        WHERE start_station_id <> end_station_id
        GROUP BY start_station_id, end_station_id
        HAVING COUNT(*) >= {MIN_TRIPS_PER_LINK}"""
    spark.sql(links_sql).write.format("delta").mode("overwrite").save(delta("station_links"))
    spark.read.format("delta").load(delta("station_links")).createOrReplaceTempView("station_links")
    report.code(links_sql.strip())
    links_stats = spark.sql("SELECT COUNT(*) AS links, COUNT(DISTINCT source) AS sources, "
                            "MIN(median_seconds) AS min_seconds, MAX(median_seconds) AS max_seconds "
                            "FROM station_links").toPandas()
    report.table(links_stats)
    report.record("part2.links", links_stats.to_dict(orient="records")[0])
    stage_edges(spark.table("station_links"), "station_links", "source", "target")
    betweenness_sql = f"""
        WITH {stations_cte}
        SELECT s.station_name, b.score AS betweenness, l.communityId
        FROM nutmeg_betweenness('station_links', '{{"weightProperty": "median_seconds"}}') AS b
        JOIN stations s ON s.station_id = CAST(b.nodeId AS BIGINT)
        JOIN leiden l ON l.nodeId = b.nodeId
        ORDER BY betweenness DESC
        LIMIT 10"""
    report.emit("Directed links ridden at least "
                f"{MIN_TRIPS_PER_LINK} times, each weighted by its median trip time in whole seconds "
                "(integral weights make Dijkstra's ties exact):")
    report.emit()
    report.code(betweenness_sql.strip())
    btop = spark.sql(betweenness_sql).toPandas()
    report.table(btop)
    report.record("part2.betweenness_top10", btop.to_dict(orient="records"))

    links_pd = spark.table("station_links").toPandas()
    dg = nx.DiGraph()
    dg.add_nodes_from(set(links_pd.source) | set(links_pd.target))
    dg.add_weighted_edges_from(links_pd[["source", "target", "median_seconds"]].itertuples(index=False, name=None))
    report.emit("Reference: NetworkX `betweenness_centrality(weight=..., normalized=False)` on the same "
                "link table, read back from Delta into the client.")
    report.emit()
    nx_b = pd.Series(nx.betweenness_centrality(dg, weight="weight", normalized=False))
    ours_b = run(spark, "station_links", "betweenness", weightProperty="median_seconds").toPandas()
    ours_b.nodeId = ours_b.nodeId.astype(np.int64)
    ours_b = ours_b.set_index("nodeId").score
    diff = (ours_b.sort_index() - nx_b.reindex(ours_b.sort_index().index)).abs()
    report.emit(f"- NetworkX {nx.__version__}: max |diff| = {diff.max():.3e} over {len(diff)} nodes "
                f"(largest score {nx_b.max():.1f}); top 10 same order: "
                f"{list(ours_b.sort_values(ascending=False, kind='mergesort').index[:10]) == list(nx_b.sort_values(ascending=False, kind='mergesort').index[:10])}")
    report.record("part2.check.betweenness", {"max_abs_diff": float(diff.max()), "nodes": int(len(diff)),
                                              "max_score": float(nx_b.max())})

    # ---------------------------------------------------- results back to Delta
    report.h(3, "Results written back to Delta")
    metrics_sql = f"""
        WITH {stations_cte}
        SELECT s.station_id, s.station_name, c.latitude, c.longitude,
               p.score AS pagerank,
               pm.score AS pagerank_minutes,
               l.communityId AS community,
               b.score AS betweenness
        FROM stations s
        JOIN pagerank p ON CAST(p.nodeId AS BIGINT) = s.station_id
        LEFT JOIN nutmeg_pagerank('trips_minutes', '{{"weightProperty": "trip_duration"}}') pm ON pm.nodeId = p.nodeId
        JOIN leiden l ON l.nodeId = p.nodeId
        LEFT JOIN nutmeg_betweenness('station_links', '{{"weightProperty": "median_seconds"}}') b ON b.nodeId = p.nodeId
        LEFT JOIN station_coordinates c ON c.station_id = s.station_id"""
    report.code(metrics_sql.strip())
    spark.sql(metrics_sql).write.format("delta").mode("overwrite").save(delta("station_metrics"))
    spark.read.format("delta").load(delta("station_metrics")).createOrReplaceTempView("station_metrics")
    rows_written = spark.table("station_metrics").count()
    report.emit(f"`delta/station_metrics`: {rows_written} rows, columns "
                f"{', '.join(spark.table('station_metrics').columns)}.")
    report.record("part2.station_metrics_rows", rows_written)
    readback_sql = """
        SELECT station_name, community, betweenness,
               RANK() OVER (ORDER BY pagerank DESC) AS pagerank_rank,
               RANK() OVER (ORDER BY pagerank_minutes DESC) AS minutes_rank
        FROM station_metrics
        ORDER BY betweenness DESC
        LIMIT 5"""
    report.emit()
    report.emit("Read back from Delta in one query:")
    report.emit()
    report.code(readback_sql.strip())
    readback = spark.sql(readback_sql).toPandas()
    report.table(readback)
    report.record("part2.readback", readback.to_dict(orient="records"))

    # ---------------------------------------------------- A*
    report.h(3, "A* between two named stations")
    if not have_coordinates:
        report.emit("Skipped: no station coordinates.")
    else:
        def box(alias: str) -> str:
            return (f"{alias}.latitude BETWEEN {NYC_LAT[0]} AND {NYC_LAT[1]} "
                    f"AND {alias}.longitude BETWEEN {NYC_LON[0]} AND {NYC_LON[1]}")

        route_edges_sql = f"""
            SELECT l.source, l.target,
                   2 * {EARTH_RADIUS_METRES} * ASIN(SQRT(
                       POW(SIN(RADIANS(b.latitude - a.latitude) / 2), 2)
                       + COS(RADIANS(a.latitude)) * COS(RADIANS(b.latitude))
                         * POW(SIN(RADIANS(b.longitude - a.longitude) / 2), 2))) AS metres
            FROM station_links l
            JOIN station_coordinates a ON a.station_id = l.source
            JOIN station_coordinates b ON b.station_id = l.target
            WHERE {box('a')} AND {box('b')}"""
        route_nodes_sql = f"""
            SELECT CAST(c.station_id AS STRING) AS station_id, c.latitude, c.longitude
            FROM station_coordinates c
            WHERE {box('c')}
              AND c.station_id IN (SELECT source FROM route_edges UNION SELECT target FROM route_edges)"""
        report.code(route_edges_sql.strip())
        # Both tables are joins over the tables above, staged as they are.
        spark.sql(route_edges_sql).createOrReplaceTempView("route_edges")
        route_edges = spark.table("route_edges")
        (spark.sql(route_nodes_sql).write.format("nutmeg").option("graph", "routes").option("part", "nodes")
            .option("idColumn", "station_id").mode("overwrite").save())
        (route_edges.write.format("nutmeg").option("graph", "routes").option("part", "edges")
            .option("sourceColumn", "source").option("targetColumn", "target").mode("overwrite").save())
        report.emit()
        report.table(run(spark, "routes", "projectionStats").toPandas())
        ends = spark.sql(f"""
            WITH {stations_cte}
            SELECT station_name, station_id FROM stations
            WHERE station_name IN ('{args.from_station.replace("'", "''")}', '{args.to_station.replace("'", "''")}')
        """).toPandas().set_index("station_name").station_id
        if len(ends) != 2:
            report.emit(f"Skipped: station names not found ({args.from_station!r}, {args.to_station!r}).")
        else:
            src, dst = str(ends[args.from_station]), str(ends[args.to_station])
            report.emit(f"From **{args.from_station}** ({src}) to **{args.to_station}** ({dst}), over the "
                        f"links ridden at least {MIN_TRIPS_PER_LINK} times, each weighted by the great-circle "
                        "length between its stations in metres. The heuristic is the great-circle distance to "
                        "the target, which is admissible because it is in the same unit and no chain of links "
                        "is shorter than the straight line.")
            report.emit()
            astar_sql = f"""
                WITH {stations_cte}
                SELECT ROW_NUMBER() OVER (ORDER BY a.costFromSource) - 1 AS hop, s.station_name,
                       ROUND(a.costFromSource) AS metres_from_start,
                       a.totalCost, a.settled
                FROM nutmeg_astar('routes', '{{"source": "{src}", "target": "{dst}", "weightProperty": "metres"}}') a
                JOIN stations s ON s.station_id = CAST(a.nodeId AS BIGINT)
                ORDER BY a.costFromSource"""
            report.code(astar_sql.strip())
            path = spark.sql(astar_sql).toPandas()
            report.table(path.drop(columns=["totalCost", "settled"]), floatfmt="{:.0f}")
            total = float(path.totalCost.iloc[0])
            settled = int(path.settled.iloc[0])
            report.record("part2.astar", {"from": args.from_station, "to": args.to_station,
                                          "total_metres": total, "hops": int(len(path) - 1),
                                          "settled": settled, "path": path.station_name.tolist()})
            coords_pd = spark.table("station_coordinates").toPandas().set_index("station_id")
            straight = float(haversine(coords_pd.latitude[int(src)], coords_pd.longitude[int(src)],
                                       coords_pd.latitude[int(dst)], coords_pd.longitude[int(dst)]))
            report.emit(f"- Total {total:.1f} m over {len(path) - 1} links; straight line {straight:.1f} m; "
                        f"the search settled {settled} stations.")
            dij = run(spark, "routes", "dijkstra", source=src, weightProperty="metres").toPandas()
            dij_total = float(dij.set_index("nodeId").distance[dst])
            unreachable = int(dij.distance.isna().sum())
            report.record("part2.dijkstra_unreachable", unreachable)
            edges_pd = route_edges.toPandas()
            rg = nx.DiGraph()
            rg.add_weighted_edges_from(edges_pd[["source", "target", "metres"]].astype(
                {"source": str, "target": str}).itertuples(index=False, name=None))
            nx_total = nx.dijkstra_path_length(rg, src, dst, weight="weight")
            nx_path = nx.dijkstra_path(rg, src, dst, weight="weight")
            ours_path_ids = spark.sql(
                f"SELECT nodeId FROM nutmeg_astar('routes', '{{\"source\": \"{src}\", \"target\": \"{dst}\", "
                f"\"weightProperty\": \"metres\"}}') ORDER BY costFromSource").toPandas().nodeId.tolist()
            same_path = [str(x) for x in nx_path] == [str(x) for x in ours_path_ids]
            report.emit(f"- Nutmeg `dijkstra` to the same target: {dij_total:.6f} m ({unreachable} stations "
                        f"unreachable from the start, read as NULL); NetworkX Dijkstra on the "
                        f"same edges: {nx_total:.6f} m; A*: {total:.6f} m (largest pairwise |diff| "
                        f"{max(abs(total - dij_total), abs(total - nx_total), abs(dij_total - nx_total)):.3e} m). "
                        f"NetworkX's path is the same station sequence: {same_path}.")
            report.record("part2.check.astar", {"astar": total, "nutmeg_dijkstra": dij_total,
                                                "networkx_dijkstra": nx_total, "same_path": same_path})

    report.save()
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
