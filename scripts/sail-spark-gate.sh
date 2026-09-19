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
#
# The script refuses to start a suite unless the server it just started is
# the one listening on the port, and it stops that server before the next
# one. A server leaked by an earlier run would otherwise answer both suites
# and make the comparison meaningless, which is what happened the first time
# this ran: the branch servers failed to bind and both suites measured the
# base.
set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "usage: $0 <base-sail-binary> <branch-sail-binary> <report-name>" >&2
  exit 2
fi

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

for f in "${base_bin}" "${branch_bin}" "${venv}/bin/python"; do
  [ -x "${f}" ] || { echo "not executable: ${f}" >&2; exit 2; }
done

python_version="$("${venv}/bin/python" -c 'import sys; print("%s.%s" % sys.version_info[:2])')"
work_dir="$("${venv}/bin/python" -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')"
# The server binary embeds the test environment's interpreter, so its shared
# library must be findable at run time; the Python UDF tests fail with a
# version mismatch otherwise.
python_libdir="$("${venv}/bin/python" -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR"))')"
export LD_LIBRARY_PATH="${python_libdir}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"

server_pid=""

port_open() {
  (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null
}

stop_server() {
  [ -n "${server_pid}" ] || return 0
  kill "${server_pid}" 2>/dev/null || true
  for _ in $(seq 1 20); do
    kill -0 "${server_pid}" 2>/dev/null || break
    sleep 1
  done
  if kill -0 "${server_pid}" 2>/dev/null; then
    kill -9 "${server_pid}" 2>/dev/null || true
    sleep 1
  fi
  wait "${server_pid}" 2>/dev/null || true
  server_pid=""
  for _ in $(seq 1 20); do
    port_open || return 0
    sleep 1
  done
  echo "port ${port} is still open after stopping the server" >&2
  return 1
}

trap 'stop_server || true' EXIT

start_server() {
  local bin="$1" name="$2"
  local log="${sail}/tmp/sail-server-${name}.log"
  if port_open; then
    echo "port ${port} is already in use; refusing to run, a leaked server would answer the tests" >&2
    return 1
  fi
  (
    export PYARROW_IGNORE_TIMEZONE=1
    export SAIL_EXECUTION__DEFAULT_PARALLELISM=4
    export SAIL_RUNTIME__STACK_SIZE=16777216
    export SAIL_CATALOG__DEFAULT_CATALOG='"spark_catalog"'
    export SAIL_CATALOG__DEFAULT_DATABASE='["default"]'
    export SAIL_CATALOG__LIST='[{name="spark_catalog", type="memory", initial_database=["default"], initial_database_comment="default database"}]'
    export PYO3_PYTHON="${venv}/bin/python"
    export PYTHONPATH="${venv}/lib/python${python_version}/site-packages"
    export RUST_LOG="${RUST_LOG:-warn}"
    exec "${bin}" spark server --port "${port}" -C "${work_dir}"
  ) > "${log}" 2>&1 &
  server_pid=$!
  for _ in $(seq 1 60); do
    port_open && break
    if ! kill -0 "${server_pid}" 2>/dev/null; then
      echo "server exited before listening; see ${log}" >&2
      sed -n 1,20p "${log}" >&2
      return 1
    fi
    sleep 1
  done
  port_open || { echo "server never listened on ${port}; see ${log}" >&2; return 1; }
  kill -0 "${server_pid}" 2>/dev/null || { echo "server died after the port opened; see ${log}" >&2; return 1; }
  echo "started ${bin} as pid ${server_pid} on port ${port}"
}

run_suite() {
  local bin="$1" name="$2" commit="$3" status=0
  start_server "${bin}" "${name}"
  (cd "${sail}" && TEST_RUN_NAME="${name}" TEST_RUN_GIT_COMMIT="${commit}" \
     TEST_RUN_GIT_REF="${name}" SPARK_TESTING_REMOTE_PORT="${port}" \
     hatch run "${env_name}:scripts/spark-tests/run-tests.sh") || status=$?
  # The suite means nothing unless our server served all of it.
  if ! kill -0 "${server_pid}" 2>/dev/null; then
    echo "server died during ${name}" >&2
    stop_server || true
    return 1
  fi
  echo "${bin}" > "${sail}/tmp/spark-tests/${name}/binary"
  stop_server
  return "${status}"
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
