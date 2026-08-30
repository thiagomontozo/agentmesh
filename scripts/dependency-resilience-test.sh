#!/usr/bin/env bash
set -euo pipefail

binary=${1:?AgentMesh binary is required}
log_dir=${RUNNER_TEMP:-/tmp}/agentmesh-dependency-resilience
mkdir -p "$log_dir"

declare -A active_pids=()
cleanup() {
  docker compose unpause postgres redis nats >/dev/null 2>&1 || true
  for pid in "${!active_pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  for pid in "${!active_pids[@]}"; do wait "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT

start_process() {
  local role=$1 port=$2 instance=$3
  AGENTMESH_MODE=distributed AGENTMESH_ROLE="$role" AGENTMESH_ADDR="127.0.0.1:$port" \
  AGENTMESH_INSTANCE_ID="$instance" AGENTMESH_WORKERS=4 \
  AGENTMESH_DATABASE_URL='postgres://agentmesh:agentmesh@localhost:5432/agentmesh?sslmode=disable' \
  AGENTMESH_NATS_URL='nats://localhost:4222' AGENTMESH_REDIS_URL='redis://localhost:6379/0' \
  AGENTMESH_EXECUTION_DELAY=10ms AGENTMESH_AGENT_HEALTH_INTERVAL=1h \
  "$binary" >"$log_dir/$instance.log" 2>&1 &
  LAST_PID=$!
  active_pids["$LAST_PID"]=1
}

dump_logs() {
  find "$log_dir" -maxdepth 1 -type f -print -exec tail -100 {} \; >&2
}

assert_running() {
  if ! kill -0 "$1" 2>/dev/null; then
    echo "AgentMesh process $1 stopped unexpectedly" >&2
    dump_logs
    return 1
  fi
}

wait_ready() {
  for _ in $(seq 1 100); do
    if curl --max-time 3 -fsS 'http://127.0.0.1:18090/readyz' >/dev/null; then return; fi
    sleep 0.1
  done
  echo 'API did not become ready' >&2
  dump_logs
  return 1
}

wait_unready() {
  local dependency=$1 status
  for _ in $(seq 1 20); do
    status=$(curl --max-time 4 -sS -o /dev/null -w '%{http_code}' 'http://127.0.0.1:18090/readyz' || true)
    if [[ "$status" == 503 ]]; then return; fi
    sleep 0.1
  done
  echo "readiness did not report 503 while $dependency was unavailable" >&2
  dump_logs
  return 1
}

wait_run() {
  local run_id=$1 status
  for _ in $(seq 1 150); do
    status=$(curl -fsS "http://127.0.0.1:18090/api/v1/runs/$run_id" | jq -er .status)
    if [[ "$status" == succeeded ]]; then return; fi
    if [[ "$status" == failed || "$status" == canceled ]]; then
      echo "Run $run_id became $status after dependency recovery" >&2
      return 1
    fi
    sleep 0.1
  done
  echo "Run $run_id did not complete after dependency recovery" >&2
  return 1
}

start_process api 18090 resilience-api
api_pid=$LAST_PID
wait_ready

# A paused database models a dependency that accepts connections but stops
# responding. The readiness deadline must bound the stall and preserve liveness.
docker compose pause postgres
wait_unready 'PostgreSQL was paused'
assert_running "$api_pid"
docker compose unpause postgres
wait_ready

# Full Redis and NATS outages must make the replica unready without terminating
# it. Existing clients are then expected to reconnect after service recovery.
docker compose stop redis
wait_unready 'Redis was stopped'
assert_running "$api_pid"
docker compose up -d --wait redis
wait_ready

docker compose stop nats
wait_unready 'NATS was stopped'
assert_running "$api_pid"
docker compose up -d --wait nats
wait_ready

start_process worker 0 resilience-worker
worker_pid=$LAST_PID
sleep 1
assert_running "$worker_pid"

agent_id=$(curl -fsS -X POST 'http://127.0.0.1:18090/api/v1/agents' \
  -H 'Content-Type: application/json' -d '{"name":"post-recovery-agent"}' | jq -er .id)
run_id=$(curl -fsS -X POST 'http://127.0.0.1:18090/api/v1/runs' \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: post-dependency-recovery' \
  -d "{\"agent_id\":\"$agent_id\",\"input\":\"dependencies recovered\"}" | jq -er .id)
wait_run "$run_id"

assert_running "$api_pid"
assert_running "$worker_pid"
echo 'dependency outage, bounded stall, readiness degradation, and recovery passed'
