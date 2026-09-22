# Nutmeg

Graph analytics inside Sail: Grust's graph kernels as a Spark data source
and as SQL table functions, so a Spark Connect client projects a graph from
any DataFrame Sail can read, runs algorithms in the engine's process, and
gets DataFrames back — no separate graph database, no copy into a billed
instance. The design and the Neo4j comparison it answers are in Grust's
`docs/goals/sail-graph-analytics.md`.

## Built on Grust's abstractions

Nutmeg adds a way in and a way out, not a second graph library.

- **The algorithm catalog is Grust's procedure registry.** Names,
  arguments, option names, defaults and declared outputs come from
  `grust-algorithm-procedures`, the registry behind
  `CALL grust.algorithms.pagerank(...)`; calls are validated by Grust's
  validator. An algorithm added to Grust appears in Nutmeg by name, and a
  test fails until it is served.
- **The graph layout is grust-arrow's**, and projections are built by
  Grust's `GraphProjection::from_arrow_batches` with Grust's projection
  options (`orientation`, `nodeLabels`, `relationshipTypes`,
  `weightProperty`, `defaultWeight`). Other tables are renamed into that
  layout when staged.
- **The stored graph and the Rust client are grust-sail's.** A Grust graph
  kept in Sail by `SailGraphStore` (`grust_nodes`, `grust_edges`) is staged
  with one call and never leaves the server;
  `SailGraphStore::stage_algorithm_graph` and `run_algorithm` are the Rust
  client, built on grust-sail's general `read_format_arrow_ipc` and
  `write_query_to_format`.

## Layout

- `crates/nutmeg-graph` — Sail-agnostic core over DataFusion 55: a named
  registry of staged graphs and every algorithm Grust registers as a
  DataFusion table function (`nutmeg_pagerank('g', '{"damping": 0.85}')`,
  …). Result schemas are observed from the kernels, not transcribed.
- `crates/nutmeg-sail` — the `nutmeg` Spark data source
  (`spark.read.format("nutmeg").option("graph", "g").option("algorithm", "pagerank")`
  to run an algorithm; `df.write.format("nutmeg").option("graph", "g")
  .option("part", "edges")` to stage a projection's rows) and a session
  mutator that registers the source and the table functions in a Sail
  session.
- `crates/nutmeg-server` — the Sail Spark Connect server with Nutmeg
  registered. Depends on one small hook in Sail (see below).
- `python/nutmeg` — a PySpark client shaped like the `graphdatascience`
  verbs: `project`, `page_rank.stream`, `wcc.stream`, …

## Column names

Results carry the column names Grust's registry declares, the ones
`CALL grust.algorithms.<name>(...) YIELD ...` uses. A few differ from Neo4j
GDS's for the same quantity (Yen's `pathIndex`, GDS `index`; `iterations`
and `converged`, GDS `ranIterations` and `didConverge`; ...). Opting in per
read with `columnNames` = `gds` (the `nutmeg` read option, a key in a table
function's JSON configuration, or `Nutmeg(spark, column_names="gds")` for a
whole client) renames those columns, from the cited table
`nutmeg_graph::GDS_COLUMN_ALIASES`. Only names change: where GDS's row shape
differs from Grust's, it still does.

## Row order

Grust's kernels are deterministic for a given input, but row order is part
of the input. Projection rows follow the order nodes are staged in, or, when
only edges are staged, the order ids first appear among the edges. Adjacency
follows edge order. Leiden and Louvain visit nodes in that order, and label
propagation breaks ties by it. The order also decides which of several
equal-cost paths is reported, how components and colours are labelled, and
the order of floating-point sums. A DataFrame's row order is not fixed, and
Sail's scan order changes from run to run. In the Citi Bike example, the
same query gave Leiden modularity 0.4398 in one run and 0.4389 in another.

So staging keeps each graph in a **canonical order** by default: the write
option `order` = `canonical`, or `project(..., order="canonical")` in the
Python client. After every write, the whole part is sorted, including rows
kept from earlier `append` writes, so neither the order of the rows nor the
order of the appends changes a result:

- nodes by `node_id`. Grust refuses duplicate ids, so this is a total order
  on every graph that can run. Nodes derived from edges alone are put in id
  order as well.
- edges by `source`, `target`, `edge_id` (nulls last), `label`, then every
  other staged column by name. Two edges that tie on all of these are equal
  in every column a kernel reads, so their relative order cannot change any
  result. Parallel edges without an `edge_id` are still ordered, for
  example by their weights.

Ids are Utf8 once staged, so the order is text order: `"10"` sorts before
`"9"`. Determinism only needs a total order, and no kernel reads the row
order as a numeric one. It does mean the canonical order is not the order an
`ORDER BY` on integer ids gives.

`order` = `asStaged` keeps rows in arrival order with no sort. It is for
callers who already guarantee an order or who want to skip the cost. An
`asStaged` append leaves the part unsorted until the next canonical write,
which sorts the whole part again. If appends of differently shaped
DataFrames disagree on a column's type (for example `w` as an integer in one
and a double in the next), there is no single sorted form, and a canonical
write is refused with a message that names the column. Cast the column, or
stage with `asStaged`.

**Cost.** Each canonical write does an in-memory sort of every staged row of
that part on the server, not only the new rows, while holding the graph's
write lock. Rows are compared in Arrow's row format. Time is O(n log n) in
the part's rows, so staging in k appends costs k such sorts. Peak memory is
about twice the part, plus the sort keys (the key columns encoded) and 16
bytes of permutation per row. For edges the keys are the ids, the type and
the numeric properties. For nodes they are only the ids. Like staging
itself, the sort is not charged against the projection's memory admission
(`NUTMEG_MEMORY_BYTES`) or its work limit. That admission starts when a
projection is built, and the staged rows, sorted or not, are held outside
it. Stage a large graph in one write rather than many appends, or use
`asStaged` if the order is already fixed.

## Sail

Nutmeg builds against a Sail checkout at `../sail` (commit recorded in
`SAIL_COMMIT`). Registering an external data source and table functions in
a Sail session needs one hook that Sail does not expose today — a way for
an embedder to choose the session factory, described in `docs/sail-prs.md`.
It is one isolated commit on its own branch of the Sail checkout, proposed
to Sail only after the full Spark compatibility suite passes on it, and
only on explicit go-ahead. Nothing graph-specific goes into Sail. No second
change is needed: Sail's SQL resolver already finds table functions
registered in the session, so `SELECT * FROM nutmeg_pagerank('g')` works in
Spark SQL as well as `spark.read.format("nutmeg")`.

## The twelve

`bfs`, `dfs`, `multiSourceBfs`, `dijkstra`, `shortestPaths`, `wcc`, `scc`,
`pagerank`, `degree`, `topologicalSort`, `projectionStats`, `estimateCsr` —
each with Grust's oracle-checked kernel, per-unit work charging, exact
memory admission and cancellation. What Nutmeg needs next, and in what
order, is recorded in Grust's `codex-to-codex.md` (2026-09-19 entry).
