"""Graph algorithms inside Sail, from PySpark.

    from nutmeg import Nutmeg
    nm = Nutmeg(spark)                       # a Spark Connect session on a Nutmeg server
    g = nm.graph.project("g", edges=edges_df, source="src", target="dst")
    nm.pagerank.stream(g, damping=0.9, concurrency=8).orderBy("score", ascending=False).show()
    nm.dijkstra.stream(g, source="a", weightProperty="w")
    nm.graph.project_grust("social")         # from grust-sail's grust_nodes / grust_edges
    g.drop()

The verbs follow Neo4j's `graphdatascience` client so existing code reads
the same. The algorithms and their options are Grust's: every keyword is
passed to the server and validated there by Grust's procedure registry, so
this client knows no algorithm names and needs no change when Grust adds one.
Result columns carry Grust's names unless the client is made with
`Nutmeg(spark, column_names="gds")`, which renames the ones GDS names
differently. Results are ordinary lazy DataFrames; `mutate` and `write` are DataFrame
operations (join, `write.saveAsTable`), not separate modes.

Staged rows are kept in a canonical order by default (`order="canonical"`), so
a result does not depend on the order Spark happened to deliver the rows in;
`order="asStaged"` skips the sort. See `_GraphCatalog.project`.
"""
from __future__ import annotations

import json
from typing import Any, Optional

FORMAT = "nutmeg"

__all__ = ["Nutmeg", "Graph", "FORMAT"]


def _text(value: Any) -> str:
    return value if isinstance(value, str) else json.dumps(value)


class Graph:
    """A graph staged in the server under a name."""

    def __init__(self, nutmeg: "Nutmeg", name: str):
        self._nutmeg = nutmeg
        self.name = name

    def __repr__(self) -> str:
        return f"Graph({self.name!r})"

    def stats(self, **configuration: Any):
        """Node, edge and arc counts and CSR bytes of the projection."""
        return self._nutmeg.run("projectionStats", self, **configuration)

    def estimate(self, **configuration: Any):
        """Upper-bound CSR sizing from staged row counts, before building."""
        return self._nutmeg.run("estimateCsr", self, **configuration)

    def drop(self) -> None:
        """Replace the staged rows with nothing, releasing the server's memory."""
        spark = self._nutmeg.spark
        empty = spark.createDataFrame([], "source string, target string")
        (empty.write.format(FORMAT).option("graph", self.name)
            .option("part", "edges").mode("overwrite").save())
        empty_nodes = spark.createDataFrame([], "node_id string")
        (empty_nodes.write.format(FORMAT).option("graph", self.name)
            .option("part", "nodes").mode("overwrite").save())


class _Algorithm:
    def __init__(self, nutmeg: "Nutmeg", name: str):
        self._nutmeg = nutmeg
        self._name = name

    def stream(self, graph: "Graph | str", **configuration: Any):
        """Run the algorithm and return its rows as a DataFrame.

        Keywords are the algorithm's own options, validated by Grust, plus
        the read's own: `concurrency`, how many threads the kernel may use in
        the server (without it the kernel runs single-threaded), and
        `timeoutMs`, `workLimit` and `memoryLimitBytes`, which stop this read
        alone. Reads of one graph share its cached projection whatever they
        ask for.
        """
        return self._nutmeg.run(self._name, graph, **configuration)

    __call__ = stream


class _GraphCatalog:
    def __init__(self, nutmeg: "Nutmeg"):
        self._nutmeg = nutmeg

    def project(
        self,
        name: str,
        edges,
        nodes=None,
        *,
        source: Optional[str] = None,
        target: Optional[str] = None,
        type: Optional[str] = None,
        edge_id: Optional[str] = None,
        id: Optional[str] = None,
        label: Optional[str] = None,
        order: str = "canonical",
    ) -> Graph:
        """Stage a graph from DataFrames. Columns named as in grust-arrow
        (`source`, `target`, `label`, `node_id`) or grust-sail's tables
        (`src_id`, `dst_id`, `edge_type`, `id`) are found without naming them.
        Every other numeric edge column becomes selectable as `weightProperty`.
        Without `nodes`, the nodes are the edges' endpoints.

        `order="canonical"` (the default) has the server sort the staged rows:
        nodes by id, edges by source, target, edge id, type and then every
        other column. Ids are compared as text, so "10" sorts before "9".
        Some kernels depend on row order (Leiden, Louvain and label
        propagation visit nodes in it; others break ties by it), and a
        DataFrame's row order is not fixed from run to run, so this is what
        makes the same data give the same result. The cost is one in-memory
        sort of every staged row on the server. `order="asStaged"` keeps the
        rows in the order they arrive, for callers who already fix an order
        or want to skip the sort."""
        if order not in ("canonical", "asStaged"):
            raise ValueError(f"order is 'canonical' or 'asStaged', got {order!r}")

        def write(df, part, mapping):
            writer = df.write.format(FORMAT).option("graph", name).option("part", part)
            if order != "canonical":
                writer = writer.option("order", order)
            for key, column in mapping.items():
                if column is not None:
                    writer = writer.option(key, column)
            writer.mode("overwrite").save()

        if nodes is not None:
            write(nodes, "nodes", {"idColumn": id, "labelColumn": label})
        else:
            Graph(self._nutmeg, name).drop()
        write(edges, "edges", {
            "sourceColumn": source, "targetColumn": target,
            "typeColumn": type, "edgeIdColumn": edge_id,
        })
        return Graph(self._nutmeg, name)

    def project_grust(self, name: str, weight_properties=(), order: str = "canonical") -> Graph:
        """Stage the Grust graph stored in this Sail catalog by grust-sail
        (`grust_nodes`, `grust_edges`). Each name in `weight_properties` is
        lifted out of the edges' JSON `props` as a numeric column. `order` is
        as for `project`."""
        spark = self._nutmeg.spark
        for key in weight_properties:
            if not key.isidentifier():
                raise ValueError(f"weight property {key!r} must be an identifier")
        lifted = "".join(
            f", CAST(GET_JSON_OBJECT(props, '$.{k}') AS DOUBLE) AS {k}" for k in weight_properties
        )
        nodes = spark.sql("SELECT id AS node_id, label AS label FROM grust_nodes")
        edges = spark.sql(
            "SELECT id AS edge_id, src_id AS source, dst_id AS target, "
            f"edge_type AS label{lifted} FROM grust_edges"
        )
        return self.project(name, edges, nodes, order=order)

    def get(self, name: str) -> Graph:
        return Graph(self._nutmeg, name)

    def list(self):
        """The staged graphs, as a DataFrame."""
        return self._nutmeg.spark.read.format(FORMAT).load()


class Nutmeg:
    """Entry point, over a Spark Connect session on a Nutmeg-enabled Sail server."""

    def __init__(self, spark, column_names: str = "grust"):
        """`column_names="gds"` reports result columns under GDS's names where
        Grust's differ (`index` for Yen's `pathIndex`, `ranIterations` and
        `didConverge` for `iterations` and `converged`, ...) for every read
        from this client. The default keeps Grust's names, the ones its
        registry declares; one call can override with `columnNames=`."""
        if column_names not in ("grust", "gds"):
            raise ValueError(f"column_names is 'grust' or 'gds', got {column_names!r}")
        self.spark = spark
        self.column_names = column_names
        self.graph = _GraphCatalog(self)

    def run(self, algorithm: str, graph: "Graph | str", **configuration: Any):
        name = graph.name if isinstance(graph, Graph) else graph
        reader = (self.spark.read.format(FORMAT)
                  .option("graph", name).option("algorithm", algorithm))
        if self.column_names != "grust":
            reader = reader.option("columnNames", self.column_names)
        for key, value in configuration.items():
            if value is not None:
                reader = reader.option(key, _text(value))
        return reader.load()

    def reads(self):
        """The server's reads, running and recently ended, as a DataFrame:
        `readId`, `algorithm`, `graph`, `state` (`running`, `finished`,
        `cancelled` or `failed`), `message`, `batches`, `rows`, `liveBytes`,
        `peakBytes` and `workUnits`. A read's kernel runs when its DataFrame
        is executed, and an interrupt (`spark.interruptAll()`,
        `interruptTag`, `interruptOperation`) cancels it."""
        return self.spark.sql("SELECT * FROM nutmeg_reads()")

    def __getattr__(self, algorithm: str) -> _Algorithm:
        if algorithm.startswith("_"):
            raise AttributeError(algorithm)
        return _Algorithm(self, algorithm)
