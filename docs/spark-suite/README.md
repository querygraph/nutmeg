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

## Runs

- `session-factory-hook-py313.md` — the first run of the
  `session-factory-hook` branch against base `20f4de4f`. Empty passed-test
  diff and identical counts in all five suites. Caveat: the server binaries
  embedded Python 3.13 while the tests ran on 3.11, so 394 user-defined
  function tests errored on a version mismatch on both sides. Superseded by
  the run below, which builds both binaries with
  `PYO3_PYTHON=<test venv>/bin/python`.
