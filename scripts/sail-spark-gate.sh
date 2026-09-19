#!/usr/bin/env bash
# Gate for a Sail change: run Sail's default Spark test suite against a
# server built from the base commit and one built from the branch, then
# write Sail's own before/after report.
#
#   scripts/sail-spark-gate.sh <base-sail-binary> <branch-sail-binary> <report-name>
#
# Both binaries are `sail` CLI release builds. The server environment
# mirrors Sail's scripts/spark-tests/run-server.sh; the tests run in Sail's
# hatch `test-spark.spark-3.5.9` environment with the patched PySpark.
set -euo pipefail

base_bin="$1"; branch_bin="$2"; report="$3"
sail="${SAIL_DIR:-$HOME/src/sail}"
spark_version="${SPARK_VERSION:-3.5.9}"
env_name="test-spark.spark-${spark_version}"
venv="${sail}/.venvs/${env_name}"
port="${SPARK_TESTING_REMOTE_PORT:-50051}"
# The Spark tests start a local JVM Spark alongside the remote session, so
# Java must be on the path for the tests as well as for the build.
export JAVA_HOME="${JAVA_HOME:-$HOME/opt/jdk17}"
export PATH="$HOME/.local/bin:${JAVA_HOME}/bin:$PATH"

python_version="$("${venv}/bin/python" -c 'import sys; print("%s.%s" % sys.version_info[:2])')"
# The server binary embeds the test environment's interpreter, so its shared
# library must be findable at run time; Python UDF tests fail otherwise with
# a version mismatch.
python_libdir="$("${venv}/bin/python" -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR"))')"
export LD_LIBRARY_PATH="${python_libdir}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
work_dir="$("${venv}/bin/python" -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')"

run_suite() {
  local bin="$1" name="$2" commit="$3"
  (
    export PYARROW_IGNORE_TIMEZONE=1
    export SAIL_EXECUTION__DEFAULT_PARALLELISM=4
    export SAIL_RUNTIME__STACK_SIZE=16777216
    export SAIL_CATALOG__DEFAULT_CATALOG='"spark_catalog"'
    export SAIL_CATALOG__DEFAULT_DATABASE='["default"]'
    export SAIL_CATALOG__LIST='[{name="spark_catalog", type="memory", initial_database=["default"], initial_database_comment="default database"}]'
    export PYO3_PYTHON="${venv}/bin/python"
    export PYTHONPATH="${venv}/lib/python${python_version}/site-packages"
    export RUST_LOG=warn
    exec "${bin}" spark server --port "${port}" -C "${work_dir}"
  ) > "${sail}/tmp/sail-server-${name}.log" 2>&1 &
  local server=$!
  trap 'kill ${server} 2>/dev/null || true' RETURN
  for _ in $(seq 1 60); do
    (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null && break
    sleep 1
  done
  (cd "${sail}" && TEST_RUN_NAME="${name}" TEST_RUN_GIT_COMMIT="${commit}" \
     TEST_RUN_GIT_REF="${name}" SPARK_TESTING_REMOTE_PORT="${port}" \
     hatch run "${env_name}:scripts/spark-tests/run-tests.sh")
  kill "${server}" 2>/dev/null || true
  wait "${server}" 2>/dev/null || true
}

mkdir -p "${sail}/tmp"
base_commit="$(basename "${base_bin}" | sed 's/.*-//')"
branch_commit="$(basename "${branch_bin}" | sed 's/.*-//')"
run_suite "${base_bin}" "${report}-base" "${base_commit}"
run_suite "${branch_bin}" "${report}-branch" "${branch_commit}"
(cd "${sail}" && TEST_REPORT_NAME="${report}" scripts/spark-tests/generate-test-report.sh \
   "tmp/spark-tests/${report}-branch" "tmp/spark-tests/${report}-base") \
   > "${sail}/tmp/spark-tests/${report}.md"
echo "report: ${sail}/tmp/spark-tests/${report}.md"
