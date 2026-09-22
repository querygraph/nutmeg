"""Live test against a running nutmeg-server: NUTMEG_REMOTE=sc://127.0.0.1:50051 pytest."""
import os
import threading
import time

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


# Streaming reads. A read's kernel runs when the query executes, on a thread of
# its own feeding a bounded channel, and dropping the execution's stream (which
# is what Sail's interrupt does) cancels it. `nutmeg_reads()` shows each read's
# state as the server sees it, so these tests observe the kernel, not only the
# client. Set NUTMEG_SERVER_PID to the server's process id to also check that
# its CPU goes idle once an interrupted kernel has stopped.


def _reads(spark, graph):
    return [r.asDict() for r in spark.sql("SELECT * FROM nutmeg_reads()").collect()
            if r["graph"] == graph]


def _wait_until(what, predicate, timeout=60.0, interval=0.05):
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if value:
            return value
        assert time.monotonic() < deadline, f"timed out waiting until {what}"
        time.sleep(interval)


def _ring(nm, name, n):
    """A ring of n nodes with one chord out of each: every node reaches every
    other, so any traversal visits all n nodes and 2n arcs."""
    from pyspark.sql import functions as F

    def node(column):
        return F.concat(F.lit("n"), F.lpad(column.cast("string"), 6, "0"))

    ids = nm.spark.range(n)
    edges = ids.select(node(F.col("id")).alias("src"),
                       node((F.col("id") + 1) % n).alias("dst")).union(
        ids.select(node(F.col("id")).alias("src"),
                   node((F.col("id") * 7919 + 13) % n).alias("dst")))
    return nm.graph.project(name, edges, source="src", target="dst")


def _cpu_seconds(pid):
    with open(f"/proc/{pid}/stat") as f:
        fields = f.read().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


# Exact betweenness on the ring runs one traversal per node, each visiting
# every node and arc: at least N * 3N = 1.2e11 visits for N = 200,000. That is
# over ten seconds even at an impossible 1e10 visits a second, while the
# interrupt is sent within a second or two of the kernel's first work charge,
# so the victim cannot finish before it is interrupted on any machine.
#
# The sibling reads all pairs shortest paths on the same graph and cached
# projection, and its consumer stops after SIBLING_ROWS rows until the victim
# has been dealt with. Its result has N * N = 4e10 rows, so, held back by the
# bounded channel and the transport, it is still running then on any machine.
# Its memoryLimitBytes stops it at once if the read is ever materialised.
VICTIM_NODES = 200_000
SIBLING_ROWS = 100_000


def test_an_interrupt_stops_the_kernel(nm):
    spark = nm.spark
    name = "interrupted"
    g = _ring(nm, name, VICTIM_NODES)
    known = {r["readId"] for r in _reads(spark, name)}
    outcome = {}
    sibling_ready, sibling_go_on = threading.Event(), threading.Event()
    sibling_read_all, sibling_release = threading.Event(), threading.Event()

    def victim():
        spark.addTag("nutmeg-victim")
        try:
            outcome["rows"] = nm.betweenness.stream(g).collect()
        except Exception as error:  # the interrupt surfaces here
            outcome["error"] = error
        finally:
            spark.clearTags()

    def sibling():
        spark.addTag("nutmeg-sibling")
        try:
            rows = nm.allPairsShortestPaths.stream(
                g, memoryLimitBytes=256 << 20).toLocalIterator()
            got = [tuple(next(rows)) for _ in range(SIBLING_ROWS)]
            sibling_ready.set()
            sibling_go_on.wait(120)
            got += [tuple(next(rows)) for _ in range(SIBLING_ROWS)]
            outcome["sibling"] = got
            # Hold the unfinished result open until it has been interrupted.
            sibling_read_all.set()
            sibling_release.wait(120)
        except Exception as error:
            outcome["sibling error"] = error
        finally:
            sibling_ready.set()
            sibling_read_all.set()
            spark.clearTags()

    def fresh(algorithm):
        return [r for r in _reads(spark, name)
                if r["readId"] not in known and r["algorithm"] == algorithm]

    def read(read_id):
        return [r for r in _reads(spark, name) if r["readId"] == read_id][0]

    victim_thread = threading.Thread(target=victim, daemon=True)
    victim_thread.start()
    victim_id = _wait_until(
        "the victim's kernel is charging work",
        lambda: [r for r in fresh("betweenness")
                 if r["state"] == "running" and r["workUnits"] > 0])[0]["readId"]
    sibling_thread = threading.Thread(target=sibling, daemon=True)
    sibling_thread.start()
    sibling_ready.wait(60)

    before = read(victim_id)
    sent = time.monotonic()
    interrupted = spark.interruptTag("nutmeg-victim")
    # The server's own record: the victim's kernel returned, cancelled, far
    # short of its work, and holds nothing.
    stopped = _wait_until("the interrupted kernel has stopped",
                          lambda: read(victim_id)["state"] != "running" and read(victim_id),
                          timeout=10)
    print(f"victim: {before['workUnits']} work units when interrupted; stopped within "
          f"{time.monotonic() - sent:.2f} s at {stopped['workUnits']} units, of at least "
          f"{VICTIM_NODES * 3 * VICTIM_NODES}; {stopped['message']!r}")
    assert stopped["state"] == "cancelled", stopped
    assert "cancelled" in (stopped["message"] or ""), stopped
    assert stopped["rows"] == 0 and stopped["liveBytes"] == 0, stopped
    assert stopped["workUnits"] < VICTIM_NODES * 3 * VICTIM_NODES, stopped
    assert len(interrupted) == 1, interrupted
    victim_thread.join(timeout=30)
    assert not victim_thread.is_alive()
    assert "error" in outcome and "rows" not in outcome, outcome

    # The sibling, on the same graph, was running throughout and goes on.
    assert "sibling error" not in outcome, outcome
    [sibling_read] = fresh("allPairsShortestPaths")
    assert sibling_read["state"] == "running", sibling_read
    sibling_go_on.set()
    sibling_read_all.wait(120)
    assert "sibling error" not in outcome, outcome
    lone = [tuple(r) for r in nm.allPairsShortestPaths.stream(g).limit(2 * SIBLING_ROWS).collect()]
    assert outcome["sibling"] == lone
    # Interrupting the sibling, blocked on its consumer, stops it too.
    assert read(sibling_read["readId"])["state"] == "running"
    assert len(spark.interruptTag("nutmeg-sibling")) == 1
    ended = _wait_until("the sibling has stopped",
                        lambda: read(sibling_read["readId"])["state"] != "running"
                        and read(sibling_read["readId"]), timeout=10)
    assert ended["state"] == "cancelled", ended
    sibling_release.set()
    sibling_thread.join(timeout=30)
    assert not sibling_thread.is_alive()

    pid = os.environ.get("NUTMEG_SERVER_PID")
    if pid:
        # Nothing is left running: the server's CPU is idle.
        before = _cpu_seconds(pid)
        time.sleep(2.0)
        busy = _cpu_seconds(pid) - before
        print(f"server CPU over 2 s after every read ended: {busy:.2f} s")
        assert busy < 0.5, f"the server used {busy:.2f} s of CPU in 2 s"
    g.drop()


def test_a_large_read_streams_under_a_slow_consumer(nm):
    # All pairs on n nodes is n * n rows, 4,000,000 here, and each row holds at
    # least two string offsets and a double: over 64 MB. Its cursor computes a
    # batch when one is pulled.
    spark = nm.spark
    name = "streamed"
    n = 2_000
    at_least = n * n * 16
    g = _ring(nm, name, n)
    known = {r["readId"] for r in _reads(spark, name)}

    def this_read():
        [read] = [r for r in _reads(spark, name) if r["readId"] not in known]
        return read

    started = time.monotonic()
    rows = nm.allPairsShortestPaths.stream(g).toLocalIterator()
    next(rows)
    first = time.monotonic() - started
    # The consumer pauses. Once the batches in flight have filled the
    # transport's buffers the kernel waits for it: the read is still running,
    # has stopped producing, and holds a few batches, not its result.
    time.sleep(1.5)
    paused = this_read()
    time.sleep(1.0)
    later = this_read()
    assert paused["state"] == later["state"] == "running", (paused, later)
    assert paused["rows"] == later["rows"] < n * n // 2, (paused, later)
    assert later["liveBytes"] * 10 < at_least, later
    count = 1 + sum(1 for _ in rows)
    total = time.monotonic() - started
    assert count == n * n
    read = this_read()
    assert read["state"] == "finished" and read["rows"] == n * n, read
    assert read["peakBytes"] * 10 < at_least, read
    print(f"all pairs on {n} nodes: first row after {first:.2f} s of {total:.2f} s; "
          f"{paused['rows']} rows produced while the consumer paused; "
          f"live {later['liveBytes']} bytes then; read peak {read['peakBytes']} bytes")
    g.drop()


def test_explain_runs_no_kernel(nm, capsys):
    spark = nm.spark
    name = "explained"
    g = _ring(nm, name, 1_000)
    known = {r["readId"] for r in _reads(spark, name)}
    plan = spark.sql(
        f"EXPLAIN SELECT * FROM nutmeg_betweenness('{name}')").collect()[0][0]
    nm.betweenness.stream(g).explain(True)
    assert [r for r in _reads(spark, name) if r["readId"] not in known] == []
    assert "NutmegAlgorithmExec" in plan, plan
    assert "NutmegAlgorithmExec" in capsys.readouterr().out
    g.drop()
