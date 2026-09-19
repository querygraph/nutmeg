# Draft pull request description (not sent)

**Title:** feat(spark-connect): let an embedder choose the session factory

## What

`sail-session` already has the extension point: `ServerSessionMutator`,
`ServerSessionFactory::new(config, runtime, mutator)` and
`create_session_manager(.., session_factory_fn, ..)` are public. What is
missing is reach from `sail-spark-connect`, which keeps `SparkSessionMutator`
in a private module and whose `entrypoint::serve` always installs Sail's own
factory. An embedder that wants to add a data source, a table function or a
catalog to the sessions of a Spark Connect server has to fork the crate.

This change:

- makes the `session_manager` module public, and gives `SparkSessionMutator`
  a `new(config)` constructor so it can be wrapped;
- exposes `create_spark_session_factory`, the factory used by default;
- adds `create_spark_session_manager_with_factory` and
  `entrypoint::serve_with_session_factory`, taking a
  `ServerSessionFactoryFn`.

`serve` and `create_spark_session_manager` delegate to the new functions
with the default factory, so Sail's own binary behaves exactly as before.
Three files, no new dependencies, no behavior change.

## Why

We are building a graph analytics data source that runs in the Sail process
(Grust's algorithm kernels over Arrow). It needs one thing from Sail: its
data source and table functions registered in each session. Wrapping the
Spark mutator does that in about twenty lines on our side, and keeps
everything graph-specific out of Sail.

## Testing

The full Spark compatibility suite (`scripts/spark-tests/run-tests.sh`,
default suites, patched PySpark 3.5.9) was run against a release server
built from this branch and against one built from the base commit, and the
results compared with `scripts/spark-tests/generate-test-report.sh`. The
report is in the comment below.
