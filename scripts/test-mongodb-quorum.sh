#!/usr/bin/env bash
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
suite_dir="$(cd "${script_dir}/.." && pwd)"
server_dir="${SINK_SERVER_DIR:-${suite_dir}/../sink}"
artifacts="${SINK_QUORUM_ARTIFACTS:-$(mktemp -d "${TMPDIR:-/tmp}/sink-quorum.XXXXXXXX")}"
mkdir -p "${artifacts}"
project="sink-quorum-$(date +%s)-$$"
export SINK_QUORUM_IMAGE="${SINK_QUORUM_IMAGE:-${project}:local}"
compose=(docker compose --env-file /dev/null --project-name "${project}" --file "${suite_dir}/deploy/quorum/compose.yaml")
paused=()
load_pid=""
cleanup() {
  result="$?"
  trap - EXIT
  if [[ "${#paused[@]}" -gt 0 ]]; then
    "${compose[@]}" unpause "${paused[@]}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${load_pid}" ]]; then
    kill "${load_pid}" 2>/dev/null || true
    wait "${load_pid}" 2>/dev/null || true
  fi
  "${compose[@]}" logs --no-color > "${artifacts}/containers.log" 2>&1 || true
  if ! "${compose[@]}" down --volumes --remove-orphans > "${artifacts}/cleanup.log" 2>&1; then
    result=1
  fi
  echo "MongoDB quorum evidence: ${artifacts} (exit ${result})"
  exit "${result}"
}
trap cleanup EXIT
git -C "${server_dir}" rev-parse HEAD > "${artifacts}/server-revision.txt"
git -C "${suite_dir}" rev-parse HEAD > "${artifacts}/suite-revision.txt"
"${compose[@]}" config > "${artifacts}/compose.yaml"
if [[ "${SINK_QUORUM_BUILD:-1}" == 1 ]]; then
  docker build --tag "${SINK_QUORUM_IMAGE}" "${server_dir}" > "${artifacts}/build.log" 2>&1
fi
go -C "${server_dir}" build -o "${artifacts}/sink-perf" ./cmd/sink-perf
"${compose[@]}" up --detach --wait --wait-timeout 180
health="$("${compose[@]}" port engine 8081)"
ready=0
for _ in $(seq 1 60); do
  if curl --fail --silent --max-time 3 "http://${health}/readyz?service=sink.storage.mongo" >/dev/null; then
    ready=1
    break
  fi
  sleep 1
done
[[ "${ready}" == 1 ]] || { echo 'replica set did not become ready' >&2; exit 1; }
primary="$("${compose[@]}" exec -T mongo1 mongosh --quiet --eval 'db.hello().primary')"
primary="${primary%:27017}"
case "${primary}" in mongo1|mongo2|mongo3) ;; *) echo "invalid primary: ${primary}" >&2; exit 1 ;; esac
address="$("${compose[@]}" port engine 8080)"
"${artifacts}/sink-perf" -address "${address}" -dataset perf-quorum \
  -workload merge -keys 128 -concurrency 8 -duration 75s -timeout 1s \
  > "${artifacts}/load.json" 2> "${artifacts}/load.log" &
load_pid="$!"
for _ in $(seq 1 60); do
  if grep -q 'measured workload begins' "${artifacts}/load.log"; then break; fi
  kill -0 "${load_pid}"
  sleep 1
done
grep -q 'measured workload begins' "${artifacts}/load.log"
sleep 5
"${compose[@]}" exec -T "${primary}" mongosh --quiet --eval 'db.adminCommand({replSetStepDown:20})' > "${artifacts}/stepdown.txt" 2>&1 || true
replacement=""
for _ in $(seq 1 20); do
  replacement="$("${compose[@]}" exec -T "${primary}" mongosh --quiet --eval 'db.hello().primary' 2>/dev/null || true)"
  replacement="${replacement%:27017}"
  if [[ "${replacement}" != "${primary}" && "${replacement}" =~ ^mongo[123]$ ]]; then break; fi
  sleep 1
done
[[ "${replacement}" != "${primary}" && "${replacement}" =~ ^mongo[123]$ ]] || { echo 'primary did not change' >&2; exit 1; }
printf '%s -> %s\n' "${primary}" "${replacement}" > "${artifacts}/election.txt"
sleep 5
for node in mongo1 mongo2 mongo3; do
  if [[ "${node}" != "${replacement}" ]]; then paused+=("${node}"); fi
done
"${compose[@]}" pause "${paused[@]}"
python3 -c 'import time; print(time.time_ns())' > "${artifacts}/pause-ns.txt"
sleep 12
python3 -c 'import time; print(time.time_ns())' > "${artifacts}/resume-ns.txt"
"${compose[@]}" unpause "${paused[@]}"
paused=()
wait "${load_pid}"
load_pid=""
python3 "${script_dir}/check-mongodb-quorum.py" "${artifacts}"
