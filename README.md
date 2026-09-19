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
