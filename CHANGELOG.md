# Changelog

Nutmeg is not released yet; nothing here has a version number. Entries are
grouped by what changed for someone using Nutmeg from PySpark, Spark SQL or
Rust, newest group first within each section. The commits are on
`integration/nutmeg-0.1`.

Nutmeg builds against sibling checkouts of Grust and Sail as unpinned path
dependencies. This head was built and tested against Grust
`ca6890053fba0e1bb1b7581d876dd0a1d1ad7285` (`GRUST_COMMIT`) and Sail
`f1cf1729b1d083f2b97f1ce6e68a0d92c5ccee8f` (`SAIL_COMMIT`).

## Not supported: Sail's cluster modes

**Nutmeg does not work in Sail's cluster modes** (`local-cluster`,
`kubernetes-cluster`). It is a local-mode extension. In a cluster mode
staging fails with `unsupported data sink node` and reads fail with
`unsupported physical plan node` or `no graph named ...`.

Why, in short: a cluster-mode driver serialises every stage's physical plan
for its workers through a hard-wired codec, which refuses a node it does not
know — Nutmeg's `NutmegAlgorithmExec` and the stage writer's `DataSinkExec`
included — and places stages on workers rather than the driver. Nutmeg's
staged graphs live in one process, and a Kubernetes worker runs the stock
`sail worker` binary, which has neither Nutmeg's code nor its graphs, so
letting an embedder serialise its own nodes would move the failure from the
codec to the worker rather than remove it. A correct fix needs two seams
Sail does not offer today: driver placement for an embedder's node, and a
driver-side codec for it. No Sail change is proposed for this; the reasoning,
the code references and the maintainer's stated direction are in Grust's
`GRUST-SAIL.md` §7.

`nutmeg-server` therefore selects materialised reads (`NUTMEG_READS`) in the
cluster modes, which is what can be encoded; it does not make staging work
there.

## Reads

### Streaming reads, and an interrupt that reaches the kernel

A read's kernel now runs when the query executes, not while it is planned.
A scan plans a `NutmegAlgorithmExec` of one partition; nothing runs until
its stream is first polled, and then the kernel runs on a thread of its own,
sending each batch through a bounded channel of two batches.

- **An interrupt stops the kernel.** `spark.interruptAll()`, `interruptTag`
  and `interruptOperation` drop the execution's stream, which cancels the
  read: the kernel stops at its next check, or at its next send. A `LIMIT`
  that has its rows stops it the same way, as does `Query::cancel` in Rust.
- **A slow consumer holds a read to a few batches** instead of its whole
  result, so a result far larger than memory can be streamed out.
- **`EXPLAIN` runs no kernel**, and `timeoutMs` no longer counts planning.
- **`SELECT * FROM nutmeg_reads()`** (`Nutmeg.reads()` in Python) lists the
  reads running now and the last 256 that ended, with `state`, `message`,
  `batches`, `rows`, `liveBytes`, `peakBytes` and `workUnits`.
- `NUTMEG_READS=materialized` restores the old behaviour, where the kernel
  runs during planning and the whole result is collected first.

### Per-read executions: cancellation, limits and threads

Each read runs on its own Grust child of the process memory budget, instead
of every kernel running on the one process-wide execution.

- Cancelling, timing out or exhausting one read stops that read alone. The
  budget, the cached projection and every other read are untouched.
- New read options, taken out before Grust validates the rest (like
  `columnNames`): `timeoutMs`, `workLimit`, `memoryLimitBytes` and
  `concurrency`. None is set by default.
- `concurrency` says how many threads this read's kernel may use. It is the
  read's, not the graph's: reads of one graph share its cached projection
  whatever they ask for, and a read that does not ask runs the
  single-threaded code that predates threads. (It was previously a key of
  the projection cache; that is gone.)
- Memory stays shared: every byte a read takes is admitted against both its
  own ceiling and the process budget, so concurrent reads cannot together
  exceed it.

### Column names

Result columns carry the names Grust's registry declares. `columnNames=gds`,
per read or per client (`Nutmeg(spark, column_names="gds")`), renames the
few columns Neo4j GDS names differently — Yen's `pathIndex` → `index`,
`iterations`/`converged` → `ranIterations`/`didConverge`, `levels` →
`ranLevels`, the triangle and clustering-coefficient columns — from
`nutmeg_graph::GDS_COLUMN_ALIASES`, a table that cites the GDS page each
entry comes from. Only names change, and no Grust name becomes unreachable.

### Kernels that read node properties

Every Grust kernel that reads node properties (A\*, `linkPrediction`'s
`sameCommunity` path, the community-seeded kernels, …) is served. Staging
keeps node columns as properties instead of dropping everything but the id
and the label, so properties staged from an ordinary DataFrame reach the
kernel, and the schema probe carries a column of the declared kind for every
property option any registered kernel declares.

## Staging

### Canonical order, so results do not follow Sail's scan order

Row order is part of a kernel's input, and Sail's scan order is not stable:
the same Citi Bike query gave Leiden modularity 0.4398 in one run and 0.4389
in the next. Staging now keeps each part sorted after every write, over the
whole part including rows from earlier appends:

- nodes by id; edges by source, target, edge id (nulls last), label, then
  every other staged column by name. Ids are Utf8 once staged, so this is
  text order: `"10"` sorts before `"9"`.
- The write option `order` (Python `project(..., order=...)`) is `canonical`
  by default, or `asStaged` to keep arrival order and skip the sort.
- Appends of differently shaped rows are unified, or refused on a type
  conflict without losing the rows already staged.

Each canonical write sorts the whole part in memory on the server, so
staging in k appends costs k sorts; stage in one write, or use `asStaged`,
when the order is already fixed.

### Joined DataFrames, nullability and signed integers

- **A joined DataFrame can be staged.** The sink ran its input while the
  physical plan was still being built, so a join was executed in
  `PartitionMode::Auto` and failed with `unsupported PartitionMode Auto in
  execute()`. The sink now stages the rows when the optimised plan runs.
- **Nullable columns are reported nullable.** Result schemas were observed
  on a probe graph where every node is reachable, so seven kernels that
  declare a nullable output (`bfs`, `dijkstra`, `multiSourceBfs`,
  `bellmanFord`, `longestPath`, `localClusteringCoefficient`, `modularity`)
  were reported non-nullable and failed on any read with a null, and
  `degree`, probed unweighted, failed the other way on a weighted read.
  Every batch now carries the nullability Grust declares.
- **No column is unsigned.** The Spark Connect Python client refuses a
  `uint64` column, which broke `pagerank`, `degree`, `dfs`, `yens` and the
  edge-ordinal lists. Grust declares those `Integer`, and its Arrow output
  now matches the declaration.

### One memory budget for everything Nutmeg holds

`NUTMEG_MEMORY_BYTES` (8 GiB by default) now bounds the staged rows of every
graph, each write's transient copies and its sort, every cached projection
and the kernels running on them — one Grust `ExecutionContext`, not a second
accounting scheme. Previously the variable limited each projection
separately and staging was not counted at all, so a large enough write could
exhaust the server before Grust's admission saw it.

- A write is admitted a batch at a time as Sail streams it, and a canonical
  write admits its sort's working space before sorting.
- A refused write leaves the graph exactly as it was — same rows, revision,
  cached projections and bytes — and a refused write to a new graph leaves
  no graph. The error is `ResourcesExhausted` and names the graph, the part,
  what the write needed, what is in use and the limit.
- `SELECT * FROM nutmeg_memory()` reports `limitBytes`, `usedBytes`,
  `stagedBytes` and `peakBytes`; `nutmeg_graphs()` reports `stagedBytes` per
  graph.

## Examples

`examples/citibike` follows Neo4j's *Aura Graph Analytics with Spark*
tutorial step by step on the same bike-trip data, in Sail, and then goes
past its single algorithm. `run.sh` starts a built server, records the exact
component versions and runs the script; `results/` holds the captured output,
the numbers as JSON, the community map and a three-run rerun diff. The
results committed here are the output of a run on this integrated head.

## Building and testing

`cargo test -p nutmeg-graph` now runs every `nutmeg-graph` test, the five
that go through `ctx.sql` included; there is no `sql` feature to turn on.
DataFusion's `sql` feature is on `nutmeg-graph`'s `datafusion` dev-dependency
only, which resolver 2 unifies into a build only when that crate's own test
targets are built, so `nutmeg-sail` and `nutmeg-server` still build DataFusion
without the `DataFusionError::SQL` variant Sail's exhaustive match does not
cover. The gate's clippy is two invocations for that reason; the README's
"Building and testing" section has the chain.
