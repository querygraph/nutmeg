"""Live test against a running nutmeg-server: NUTMEG_REMOTE=sc://127.0.0.1:50051 pytest."""
import os

import pytest

remote = os.environ.get("NUTMEG_REMOTE")
pytestmark = pytest.mark.skipif(not remote, reason="NUTMEG_REMOTE is not set")


@pytest.fixture(scope="module")
def nm():
    from pyspark.sql import SparkSession

    from nutmeg import Nutmeg

    spark = SparkSession.builder.remote(remote).getOrCreate()
    yield Nutmeg(spark)
    spark.stop()


def test_project_and_run(nm):
    edges = nm.spark.createDataFrame(
        [("a", "b", 1.0), ("b", "c", 2.0), ("c", "a", 3.0), ("a", "c", 10.0)],
        "src string, dst string, w double",
    )
    g = nm.graph.project("py", edges, source="src", target="dst")
    assert g.stats().collect()[0]["nodes"] == 3
    ranks = {r["nodeId"]: r["score"] for r in nm.pagerank.stream(g, damping=0.9).collect()}
    assert abs(sum(ranks.values()) - 1.0) < 1e-6
    dist = {r["nodeId"]: r["distance"]
            for r in nm.dijkstra.stream(g, source="a", weightProperty="w").collect()}
    assert dist == {"a": 0.0, "b": 1.0, "c": 3.0}
    assert "py" in [r["name"] for r in nm.graph.list().collect()]
    # The same algorithms are reachable from Spark SQL, and compose with it.
    sql = nm.spark.sql(
        "SELECT `nodeId`, score FROM nutmeg_pagerank('py', '{\"damping\": 0.9}') "
        "ORDER BY score DESC LIMIT 1"
    ).collect()
    assert len(sql) == 1 and sql[0]["score"] > 0
    with pytest.raises(Exception, match="dampng"):
        nm.pagerank.stream(g, dampng=0.5).collect()
    g.drop()


def test_stage_a_joined_dataframe(nm):
    # The ordinary way to build a graph: trips joined to stations, then staged.
    # The sink used to run its input while the plan was still being built,
    # before the join's partition mode was chosen, and failed with
    # "unsupported PartitionMode Auto".
    spark = nm.spark
    trips = spark.createDataFrame([(1, 2), (2, 3), (3, 1)], "start int, stop int")
    stations = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], "id int, name string")
    edges = (trips.join(stations.withColumnRenamed("name", "src"), trips.start == stations.id)
             .drop("id")
             .join(stations.withColumnRenamed("name", "dst"), trips.stop == stations.id)
             .select("src", "dst"))
    g = nm.graph.project("joined", edges, source="src", target="dst")
    assert g.stats().collect()[0]["edges"] == 3
    g.drop()


def test_nullable_outputs_hold_nulls(nm):
    # d is staged with no edges, so it is unreachable from a: its distance is
    # NULL. The reported schema used to be observed on a probe where every node
    # is reachable, and declared `distance` non-nullable.
    spark = nm.spark
    edges = spark.createDataFrame([("a", "b", 1.0), ("b", "c", 2.0)], "src string, dst string, w double")
    nodes = spark.createDataFrame([("a",), ("b",), ("c",), ("d",)], "id string")
    g = nm.graph.project("unreachable", edges, nodes, source="src", target="dst", id="id")
    for algorithm, options in [
        ("dijkstra", {"source": "a", "weightProperty": "w"}),
        ("bfs", {"source": "a"}),
        ("bellmanFord", {"source": "a", "weightProperty": "w"}),
    ]:
        reader = spark.read.format("nutmeg").option("graph", "unreachable").option("algorithm", algorithm)
        for key, value in options.items():
            reader = reader.option(key, value)
        frame = reader.load()
        assert frame.schema["distance"].nullable, algorithm
        dist = {r["nodeId"]: r["distance"] for r in frame.collect()}
        assert dist["d"] is None and dist["a"] == 0.0, (algorithm, dist)
    g.drop()
