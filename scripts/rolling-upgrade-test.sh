#!/usr/bin/env bash
set -euo pipefail

baseline_binary=${1:?baseline binary is required}
current_binary=${2:?current binary is required}
log_dir=${RUNNER_TEMP:-/tmp}/agentmesh-rolling-upgrade
mkdir -p "$log_dir"

declare -A active_pids=()
cleanup() {
	for pid in "${!active_pids[@]}"; do kill "$pid" 2>/dev/null || true; done
	for pid in "${!active_pids[@]}"; do wait "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT

start_process() {
  local binary=$1 role=$2 port=$3 instance=$4
  AGENTMESH_MODE=distributed AGENTMESH_ROLE="$role" AGENTMESH_ADDR=":$port" \
  AGENTMESH_INSTANCE_ID="$instance" \
  AGENTMESH_DATABASE_URL='postgres://agentmesh:agentmesh@localhost:5432/agentmesh?sslmode=disable' \
  AGENTMESH_NATS_URL='nats://localhost:4222' AGENTMESH_REDIS_URL='redis://localhost:6379/0' \
  AGENTMESH_EXECUTION_DELAY=5ms "$binary" >"$log_dir/$instance.log" 2>&1 &
  LAST_PID=$!
	active_pids["$LAST_PID"]=1
}

stop_process() {
	kill "$1"
	wait "$1" || true
	unset 'active_pids['"$1"']'
}

assert_running() {
	sleep 1
	if ! kill -0 "$1" 2>/dev/null; then
		echo "process $1 stopped unexpectedly" >&2
		find "$log_dir" -maxdepth 1 -type f -print -exec tail -100 {} \; >&2
		return 1
	fi
}

wait_ready() {
  local port=$1
  for _ in $(seq 1 100); do
    if curl -fsS "http://127.0.0.1:$port/readyz" >/dev/null; then return; fi
    sleep 0.1
  done
  echo "API on port $port did not become ready" >&2
  find "$log_dir" -maxdepth 1 -type f -print -exec tail -100 {} \; >&2
  return 1
}

create_agent() {
  curl -fsS -X POST "http://127.0.0.1:$1/api/v1/agents" -H 'Content-Type: application/json' \
    -d '{"name":"rolling-upgrade-agent"}' | jq -er .id
}

create_run() {
  local port=$1 agent_id=$2 key=$3 input=$4
  curl -fsS -X POST "http://127.0.0.1:$port/api/v1/runs" -H 'Content-Type: application/json' \
    -H "Idempotency-Key: $key" -d "{\"agent_id\":\"$agent_id\",\"input\":\"$input\"}" | jq -er .id
}

wait_run() {
  local port=$1 run_id=$2 status
  for _ in $(seq 1 150); do
    status=$(curl -fsS "http://127.0.0.1:$port/api/v1/runs/$run_id" | jq -er .status)
    if [[ "$status" == succeeded ]]; then return; fi
    if [[ "$status" == failed || "$status" == canceled ]]; then echo "Run $run_id became $status" >&2; return 1; fi
    sleep 0.1
  done
  echo "Run $run_id did not complete" >&2
  return 1
}

# Baseline initializes schema 016 and proves pre-upgrade state.
start_process "$baseline_binary" all 18080 baseline-all
baseline_all_pid=$LAST_PID
wait_ready 18080
agent_id=$(create_agent 18080)
before_run=$(create_run 18080 "$agent_id" compat-before before-upgrade)
wait_run 18080 "$before_run"
stop_process "$baseline_all_pid"

# Current API migrates to schema 017 while a baseline Worker consumes its work.
start_process "$current_binary" api 18081 current-api
wait_ready 18081
curl -fsS "http://127.0.0.1:18081/api/v1/runs/$before_run" | jq -e '.status == "succeeded"' >/dev/null
start_process "$baseline_binary" worker 0 baseline-worker
baseline_worker_pid=$LAST_PID
assert_running "$baseline_worker_pid"
overlap_run=$(create_run 18081 "$agent_id" compat-new-api-old-worker new-api-old-worker)
wait_run 18081 "$overlap_run"
stop_process "$baseline_worker_pid"

# Current Worker consumes work submitted by a restarted baseline API.
start_process "$current_binary" worker 0 current-worker
current_worker_pid=$LAST_PID
assert_running "$current_worker_pid"
start_process "$baseline_binary" api 18082 baseline-api
wait_ready 18082
curl -fsS "http://127.0.0.1:18082/api/v1/runs/$overlap_run" | jq -e '.status == "succeeded"' >/dev/null
reverse_run=$(create_run 18082 "$agent_id" compat-old-api-new-worker old-api-new-worker)
wait_run 18082 "$reverse_run"
curl -fsS "http://127.0.0.1:18081/api/v1/runs/$reverse_run" | jq -e '.status == "succeeded"' >/dev/null

# Idempotency is stable when replayed through the other API version.
replayed=$(create_run 18081 "$agent_id" compat-old-api-new-worker old-api-new-worker)
test "$replayed" = "$reverse_run"
echo "rolling upgrade compatibility passed"
