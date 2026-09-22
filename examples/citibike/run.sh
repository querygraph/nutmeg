#!/usr/bin/env bash
# Build-free runner: start a built nutmeg-server, run citibike.py against it,
# record the exact component versions, stop the server.
#
#   NUTMEG_SERVER=/path/to/target/release/nutmeg-server \
#   WORK=/abs/dir/for/data-and-delta \
#   PYTHON=/path/to/venv/bin/python \
#   examples/citibike/run.sh
#
# Nutmeg builds against sibling checkouts ../grust and ../sail (see the
# repository README); their commits are recorded from there.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
nutmeg_root="$(cd "$here/../.." && pwd)"
: "${NUTMEG_SERVER:?set NUTMEG_SERVER to a built nutmeg-server binary}"
: "${WORK:?set WORK to an absolute directory for data and Delta tables}"
PYTHON="${PYTHON:-python3}"
PORT="${PORT:-50051}"
mkdir -p "$WORK" "$here/results"
# Every run writes its Delta tables afresh; downloads in $WORK/data are kept.
rm -rf "$WORK/delta"

rev() { git -C "$1" rev-parse HEAD 2>/dev/null || echo unknown; }
"$PYTHON" - "$here/results/versions.json" <<EOF
import json, sys
json.dump({
    "nutmeg commit": "$(rev "$nutmeg_root")",
    "grust commit": "$(rev "$nutmeg_root/../grust")",
    "sail commit": "$(rev "$nutmeg_root/../sail")",
    "rustc": "$(rustc --version 2>/dev/null || echo unknown)",
    "host": "$(uname -srm)",
}, open(sys.argv[1], "w"), indent=2)
EOF

# Sail embeds Python and imports pyspark in the server process; give it the
# client environment's packages.
site_packages="$("$PYTHON" -c 'import site; print(site.getsitepackages()[0])')"
PYTHONPATH="$site_packages${PYTHONPATH:+:$PYTHONPATH}" \
  "$NUTMEG_SERVER" --port "$PORT" > "$WORK/nutmeg-server.log" 2>&1 &
server=$!
trap 'kill $server 2>/dev/null || true; wait $server 2>/dev/null || true' EXIT
for _ in $(seq 60); do
  grep -q "Spark Connect on" "$WORK/nutmeg-server.log" && break
  sleep 1
done

"$PYTHON" "$here/citibike.py" --remote "sc://127.0.0.1:$PORT" --work "$WORK" \
  --results "$here/results" --versions "$here/results/versions.json"
