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
