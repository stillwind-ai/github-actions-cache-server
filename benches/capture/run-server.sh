#!/usr/bin/env bash
# Starts a cache server behind the recording proxy for one capture session.
#
#   benches/capture/run-server.sh <name> <proxy port> [server binary]
#
# Creates database cache_capture_<name> on TEST_DATABASE_URL's server, storage
# in $CAPTURE_DIR/<name>/storage, and writes the trace to
# $CAPTURE_DIR/<name>/trace.jsonl. Prints the variables a client needs.
# Stop both processes with: kill $(cat $CAPTURE_DIR/<name>/pids)
set -euo pipefail
name=$1
proxy_port=$2
here=$(cd "$(dirname "$0")" && pwd)
binary=${3:-$here/../../target/release/github-actions-cache-server}
capture_dir=${CAPTURE_DIR:-$here/../../target/capture}
admin_url=${TEST_DATABASE_URL:-postgres://postgres:postgres@localhost:5432/postgres}
dir=$capture_dir/$name
mkdir -p "$dir/storage"
db=cache_capture_${name//[^a-zA-Z0-9]/_}
psql "$admin_url" -qc "DROP DATABASE IF EXISTS \"$db\" WITH (FORCE)" -c "CREATE DATABASE \"$db\""
server_port=$((proxy_port + 1000))
proxy_url=http://127.0.0.1:$proxy_port
HOST=127.0.0.1 PORT=$server_port API_BASE_URL=$proxy_url \
  DB_POSTGRES_URL="${admin_url%/*}/$db" STORAGE_FILESYSTEM_PATH="$dir/storage" \
  SKIP_TOKEN_VALIDATION=true CACHE_FILESYSTEM_MAX_USAGE_PERCENT=100 RUST_LOG=${RUST_LOG:-info} \
  "$binary" >"$dir/server.log" 2>&1 </dev/null &
server_pid=$!
node "$here/record-proxy.mjs" "$proxy_port" "http://127.0.0.1:$server_port" "$dir/trace.jsonl" >"$dir/proxy.log" 2>&1 </dev/null &
proxy_pid=$!
echo "$server_pid $proxy_pid" >"$dir/pids"
until curl -sf "http://127.0.0.1:$server_port/health" >/dev/null; do sleep 0.2; done
# An unsigned runtime token (the server skips signature checks) with write
# access to refs/heads/main of repository 1.
payload=$(printf '{"ac":"[{\\"Scope\\":\\"refs/heads/main\\",\\"Permission\\":3}]","repository_id":"1"}' | base64 -w0 | tr '+/' '-_' | tr -d '=')
token="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.$payload.signature"
cat <<VARS
# Cache server for capture '$name' (trace: $dir/trace.jsonl)
export ACTIONS_RESULTS_URL=$proxy_url/
export ACTIONS_CACHE_URL=$proxy_url/
export ACTIONS_CACHE_SERVICE_V2=True
export ACTIONS_RUNTIME_TOKEN=$token
export GITHUB_REF=refs/heads/main
export GITHUB_SERVER_URL=https://github.com
VARS
