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
the numeric properties. For nodes they are only the ids. That peak is
admitted from the memory budget before the sort starts (see below), so a
write whose rows fit but whose sort does not is refused under `canonical`
and accepted under `asStaged`. Stage a large graph in one write rather than
many appends, or use `asStaged` if the order is already fixed.

## Memory

One budget bounds everything Nutmeg holds in the Sail process: the staged
rows of every graph, each write's transient copies and its sort, every
cached projection, and the kernels running on them. It is set in bytes by
the `NUTMEG_MEMORY_BYTES` environment variable when the server starts, and
defaults to 8 GiB (`8589934592`). For example:

```sh
NUTMEG_MEMORY_BYTES=34359738368 nutmeg-server   # 32 GiB
```

The budget is fixed once the first graph is staged. Before this change the
variable limited each projection separately and staging was not counted at
all. It now limits the process total.

The budget is Grust's own admission: one `ExecutionContext` whose
reservations are taken before memory is allocated and returned when the
memory is freed. A write is admitted a batch at a time as Sail streams it:

- Each incoming batch is refused before it is copied if its rows cannot fit
  in what is free. What the renamed batch keeps alive is then admitted.
- A canonical write admits the sort's permutation, the larger of its keys and
  its sorted copy, before sorting.
- The new part is built aside and swapped in. A refused write leaves the
  graph exactly as it was: the same rows, revision, cached projections and
  bytes. A refused write to a new graph leaves no graph behind.
- The error is `ResourcesExhausted`. It names the graph, the part, what the
  write needed, what is in use and the limit.

Bytes are measured as the Arrow allocations the rows keep alive, each counted
once at its capacity. A batch sliced from a larger one is charged for the
whole buffer when it is staged `asStaged`. A canonical write copies it
compactly. Its sorted copy is admitted at an upper bound before it is made
and shrunk to its real size once it is, so a sorted part is charged exactly
what it keeps alive; the peak keeps the bound. Replacing a part, restaging
(which evicts the graph's cached projections) and `Registry::drop` return
their bytes. A read still running
on an evicted projection holds its bytes until it finishes.

To see what is using memory, `nutmeg_graphs()` and
`spark.read.format("nutmeg").load()` report `stagedBytes` per graph.
`SELECT * FROM nutmeg_memory()` reports:

- `limitBytes`
- `usedBytes`: staged rows, writes in progress, projections and kernels
- `stagedBytes`: the total over graphs
- `peakBytes`

### Reads

Every read (a `spark.read.format("nutmeg")` scan, a `nutmeg_<algorithm>`
table function, or `nutmeg_graph::Query` from Rust) runs on its own Grust
child of the budget's execution:

- **Memory** is shared. Every byte a read takes counts against the child
  and against `NUTMEG_MEMORY_BYTES` in one admission, so any number of
  concurrent reads cannot together exceed the budget. A read that does not
  fit is refused with `ResourcesExhausted`; the others go on.
- **What outlives a read stays on the budget.** Staged rows, their sort, and
  cached projections are built on the budget's execution and owned by it.
  So is the transpose an in-arc kernel (PageRank, SCC, label propagation,
  Yen's, spectral) builds the first time it needs one. It is cached with the
  projection. What only the read uses is charged to the read and returned
  when it finishes: nodes derived from edges, node properties, kernel
  scratch, result batches.
- **Stopping is per read.** Each read has its own cancellation, work counter,
  work budget and deadline. The kernel runs through a view of the cached
  projection (`GraphProjection::with_execution`) on the read's child. When a
  read is cancelled or hits a limit, it fails and nothing else is affected:
  not the budget, not the cached projection, not any other read on it.
  Cancelling the budget's execution would stop every read.

A read has no limits of its own by default. It runs until it finishes,
fails, or is cancelled, bounded only by the shared budget. A read opts in
with read options (or keys in a table function's JSON configuration). Nutmeg
takes them out before Grust validates the rest, as it does `columnNames`:

| option | meaning |
|---|---|
| `timeoutMs` | deadline, in milliseconds from when the read starts running |
| `workLimit` | Grust work units the read may charge |
| `memoryLimitBytes` | a memory ceiling of the read's own, within the budget |

There is no default deadline or work limit. Grust's work units are not
comparable across kernels, and wall time depends on load. A default would
fail large, legitimate reads on a busy machine and not on an idle one, and
Spark sets no query timeout by default either.

**Cancellation.** `Query::cancel` stops a read. Its kernel fails with
`cancelled` at its next check. Sail's interrupt does not reach a read today.
The kernel runs inside the table's `scan`, which DataFusion calls while it
plans the query. That is before Sail creates the executor whose stream an
interrupt drops, and the call is synchronous, so dropping its future cannot
stop it. Sail's interrupt could reach it if the kernel ran in the scan's
execution stream and dropping the stream cancelled its read.

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
