# Spark compatibility runs

Every Sail change Nutmeg needs is gated on Sail's own Spark test suite,
run twice: once against a server built from the base commit, once against
one built from the branch. The pull request is proposed only when the
branch passes what the base passes, and only with an explicit go-ahead.

Run it with:

```bash
scripts/sail-spark-gate.sh ~/opt/sail-base-<base> ~/opt/sail-hook-<branch> <report-name>
```

The script mirrors Sail's `scripts/spark-tests/run-server.sh` environment,
runs `scripts/spark-tests/run-tests.sh` (the default suites:
`pyspark.sql.tests.connect` plus the catalog, column, dataframe and
functions doctests) in Sail's `test-spark.spark-3.5.9` hatch environment,
and finishes with Sail's `generate-test-report.sh` comparing branch to
base. Results land in `sail/tmp/spark-tests/`; the report for each branch
is copied here.

Prerequisites, installed on quegee: OpenJDK 17 (`~/opt/jdk17`), hatch via
uv, a Spark 3.5.9 checkout at `sail/opt/spark`, and the patched PySpark
package built by `env SPARK_VERSION=3.5.9 scripts/spark-tests/build-pyspark.sh`.

Sail carries test patches for Spark 3.5.9 and 4.2.0 only, so 3.5.9 is the
3.5 line the gate uses.

## Why the script checks the port

The first three attempts produced reports that meant nothing. A server
leaked by an aborted run kept the port, every later server failed to bind
with `Address already in use`, and the tests silently talked to the leaked
one, so the branch was compared against itself. The script now refuses to
start a suite when the port is already open, verifies that the server it
started is alive before and after the suite, records which binary served
each run in `<run>/binary`, and stops the server with escalation before the
next one. A report is only trustworthy when both `binary` files name the
binaries you intended.

## Runs

- `session-factory-hook.md` — the `session-factory-hook` branch against
  base `20f4de4f`, both built with
  `PYO3_PYTHON=<test venv>/bin/python` so the Python user-defined function
  tests run on the same interpreter as the tests.
