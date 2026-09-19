# Changes Nutmeg needs in Sail

Nothing graph-specific goes into Sail. Each change below is a general
embedding hook, kept as one isolated commit on its own branch of the Sail
checkout at `../sail`, small enough to review in one sitting.

**Gate for every one of them, before it is proposed upstream:** the full
Spark compatibility suite (`scripts/spark-tests`, the patched PySpark
built from `opt/spark`) is run against a server built from that branch and
compared with the same run on the base commit; the pull request is sent
only when the branch passes what the base passes, and only after an
explicit go-ahead. Results are recorded in `docs/spark-suite/`.

Sail's suite targets Spark 3.5.9 and 4.2.0 (the versions it carries
patches for in `scripts/spark-tests`), not 3.5.1. The gate is run on
3.5.9, the 3.5 line Sail supports.

## A. Session factory chosen by the embedder — branch `session-factory-hook`

Sail already has the right abstraction: `ServerSessionMutator`,
`ServerSessionFactory::new(config, runtime, mutator)` and
`create_session_manager(.., session_factory_fn, ..)` are public in
`sail-session`. What is missing is reach: `sail-spark-connect` keeps
`SparkSessionMutator` in a private module and `entrypoint::serve` always
uses Sail's own factory, so an embedder cannot add a data source, a table
function or a catalog to the sessions of a Spark Connect server without
forking the crate.

The change: make `session_manager` public, give `SparkSessionMutator` a
constructor, and add `entrypoint::serve_with_session_factory(.., factory)`
with `serve` delegating to it. No behavior changes for Sail's own binary.

Nutmeg uses it in `crates/nutmeg-server`: its factory wraps Sail's mutator
in `NutmegSessionMutator`, which registers the `nutmeg` data source in the
session's `DataSourceRegistry` and Nutmeg's table functions in the session
state.

## B. Table functions in Spark SQL — not needed

Earlier reading of Sail's SQL resolver suggested a second change would be
required, because the table-function lookup appeared to consult only Sail's
built-in list and Python UDTFs. That is wrong for this version: the
resolver asks the DataFusion session for the function first
(`self.ctx.table_function(&canonical_function_name)` in
`sail-plan/src/resolver/query/read.rs`) and falls back to the built-ins.
A function registered through the session mutator therefore resolves
already. Verified against a running Nutmeg server:

```sql
SELECT * FROM nutmeg_degree('sqltest')
```

returns the kernel's rows. Names are lowercased before lookup, so Nutmeg
registers `nutmeg_shortest_paths`, not `nutmeg_shortestPaths`. Only
change A is needed.
