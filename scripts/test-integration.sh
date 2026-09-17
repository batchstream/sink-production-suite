#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
suite_dir="$(cd "${script_dir}/.." && pwd)"
export SINK_SERVER_DIR="${SINK_SERVER_DIR:-${suite_dir}/../sink}"
artifacts="$(mktemp -d "${TMPDIR:-/tmp}/sink-qualification.XXXXXXXX")"
suite_go_flags=(-mod=readonly)
if [[ -n "${SINK_GO_DIR:-}" ]]; then
  cp "${suite_dir}/go.mod" "${artifacts}/suite.go.mod"
  cp "${suite_dir}/go.sum" "${artifacts}/suite.go.sum"
  go mod edit -modfile="${artifacts}/suite.go.mod" -replace="github.com/liran/sink-go=${SINK_GO_DIR}"
  suite_go_flags+=("-modfile=${artifacts}/suite.go.mod")
fi
project="sink-qualification-$(date +%s)-$$"
export SINK_SUITE_IMAGE="${project}:local"
compose=(docker compose --env-file /dev/null --profile conflict --project-name "${project}" --project-directory "${suite_dir}" --file "${suite_dir}/deploy/compose.yaml" --file "${artifacts}/ports.yaml")
# Expose only this disposable replica set for the candidate's storage tests.
cat > "${artifacts}/ports.yaml" <<'YAML'
services:
  mongodb:
    ports:
      - "127.0.0.1::27017"
YAML
resilience_pid=""
sampler_pid=""
broker_paused=0
scaling_workers=()
fault_cycles="${SINK_FAULT_CYCLES:-1}"
fault_interval="${SINK_FAULT_INTERVAL_SECONDS:-0}"
if [[ ! "${fault_cycles}" =~ ^[0-9]+$ || "${fault_cycles}" -lt 1 || "${fault_cycles}" -gt 24 || ! "${fault_interval}" =~ ^[0-9]+$ || "${fault_interval}" -gt 3600 ]]; then
  echo "fault cycles must be 1..24 and interval must be 0..3600 seconds" >&2
  exit 1
fi
exec > >(tee "${artifacts}/test.log") 2>&1

backend_stores="primary:async,secondary:async,sync-only:sync,elasticsearch-sync:sync,elasticsearch-async:async,mongodb-sync:sync,mongodb-async:async"

cleanup() {
	exit_code="$?"
	trap - EXIT
	for process in "${resilience_pid}" "${sampler_pid}"; do
		if [[ -n "${process}" ]]; then
			kill "${process}" >/dev/null 2>&1 || true
			wait "${process}" >/dev/null 2>&1 || true
		fi
	done
	if [[ "${broker_paused}" == 1 ]]; then
		"${compose[@]}" unpause kafka >/dev/null 2>&1 || true
	fi
	if [[ "${#scaling_workers[@]}" -gt 0 ]]; then
		for container in "${scaling_workers[@]}"; do
			docker logs "${container}" > "${artifacts}/${container}.log" 2>&1 || true
			docker rm --force "${container}" >/dev/null 2>&1 || true
		done
	fi
	if [[ "${exit_code}" != 0 && -f "${artifacts}/soak.jsonl" ]]; then
		tail -30 "${artifacts}/soak.jsonl"
	fi
	"${compose[@]}" ps --all > "${artifacts}/containers.txt" 2>&1 || true
	"${compose[@]}" logs --no-color > "${artifacts}/containers.log" 2>&1 || true
	if ! "${compose[@]}" down --volumes --remove-orphans > "${artifacts}/cleanup.log" 2>&1; then
		exit_code=1
	fi
	echo "Qualification evidence: ${artifacts}"
	exit "${exit_code}"
}
trap cleanup EXIT

wait_for_readiness() {
	for port in 20090 20091 20093 20094 20100 20101 20092 20102 20103 20121 20104 20105 20106 20107 20108 20109 20124 20110 20111 20112 20113 20126; do
		local ready=0
		for _ in $(seq 1 60); do
			if curl --max-time 3 --fail --silent "http://127.0.0.1:${port}/readyz" >/dev/null; then
				ready=1
				break
			fi
			sleep 2
		done
		if [[ "${ready}" != 1 ]]; then
			echo "Sink dependency readiness on port ${port} did not recover" >&2
			return 1
		fi
	done
	local endpoint
	for endpoint in 20100/readyz?service=sink.storage.primary 20100/readyz?service=sink.kafka.primary 20101/readyz?service=sink.storage.primary 20101/readyz?service=sink.kafka.primary 20092/readyz 20102/readyz?service=sink.storage.secondary 20102/readyz?service=sink.kafka.secondary 20103/readyz?service=sink.storage.secondary 20103/readyz?service=sink.kafka.secondary 20121/readyz 20104/readyz?service=sink.storage.sync-only 20105/readyz?service=sink.storage.sync-only 20106/readyz?service=sink.storage.elasticsearch-sync 20107/readyz?service=sink.storage.elasticsearch-sync 20108/readyz?service=sink.storage.elasticsearch-async 20108/readyz?service=sink.kafka.elasticsearch-async 20109/readyz?service=sink.storage.elasticsearch-async 20109/readyz?service=sink.kafka.elasticsearch-async 20124/readyz 20110/readyz?service=sink.storage.mongodb-sync 20111/readyz?service=sink.storage.mongodb-sync 20112/readyz?service=sink.storage.mongodb-async 20112/readyz?service=sink.kafka.mongodb-async 20113/readyz?service=sink.storage.mongodb-async 20113/readyz?service=sink.kafka.mongodb-async 20126/readyz; do
		local ready=0
		for _ in $(seq 1 60); do
			if curl --max-time 3 --fail --silent "http://127.0.0.1:${endpoint}" >/dev/null; then
				ready=1
				break
			fi
			sleep 2
		done
		if [[ "${ready}" != 1 ]]; then
			echo "Sink capability ${endpoint} did not recover" >&2
			return 1
		fi
	done
}

run_checked_tests() {
  local phase="$1"
  local required="$2"
  shift 2
  go test "${suite_go_flags[@]}" -tags=integration ./integration -json -count=1 "$@" | tee "${artifacts}/${phase}.jsonl"
  go run ./cmd/check-test-events --file "${artifacts}/${phase}.jsonl" --require "${required}"
}

record_fault() {
	printf '%s %s\n' "$(date -u +%FT%TZ)" "$1" >> "${artifacts}/faults.log"
}

wait_for_worker_members() {
	local expected="$1"
	local observation=""
	for _ in $(seq 1 30); do
		"${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
			--bootstrap-server localhost:19092 --group sink-production-workers \
			--describe --members > "${artifacts}/worker-members-${expected}.txt"
		observation="$(awk '$1 == "sink-production-workers" && $NF ~ /^[0-9]+$/ {members++; if ($NF > 0) assigned++} END {printf "%d:%d", members, assigned}' "${artifacts}/worker-members-${expected}.txt")"
		if [[ "${observation}" == "${expected}:${expected}" ]]; then
			return 0
		fi
		sleep 2
	done
	echo "expected ${expected} active assigned Workers, got ${observation}" >&2
	return 1
}

wait_for_zero_group_lag() {
	local service="$1"
	local group="$2"
	local observation=""
	for _ in $(seq 1 60); do
		observation="$(
			"${compose[@]}" exec -T "${service}" \
				/opt/kafka/bin/kafka-consumer-groups.sh \
				--bootstrap-server localhost:19092 \
				--describe \
				--group "${group}" 2>/dev/null | \
				awk -v group="${group}" '
					$1 == group && $3 ~ /^[0-9]+$/ {
						count++
						if ($6 ~ /^[0-9]+$/) {
							lag += $6
						} else if (!($4 == "-" && $5 == "0" && $6 == "-")) {
							invalid++
						}
					}
					END {printf "%d:%d:%d", count, lag, invalid}
				'
		)"
		if [[ "${observation}" == "8:0:0" ]]; then
			return
		fi
		sleep 2
	done
	echo "consumer group ${group} on ${service} did not drain: ${observation}" >&2
	return 1
}

assert_empty_dlq() {
	local service="$1"
	local topic="$2"
	local observation=""
	observation="$(
		"${compose[@]}" exec -T "${service}" \
			/opt/kafka/bin/kafka-get-offsets.sh \
			--bootstrap-server localhost:19092 \
			--topic "${topic}" | \
			awk -F: '$3 ~ /^[0-9]+$/ {count++; offsets += $3} END {printf "%d:%d", count, offsets}'
	)"
	if [[ "${observation}" != "8:0" ]]; then
		echo "dead-letter topic ${topic} on ${service} is not empty: ${observation}" >&2
		return 1
	fi
}

git -C "${SINK_SERVER_DIR}" rev-parse HEAD > "${artifacts}/server-revision.txt"
git -C "${suite_dir}" rev-parse HEAD > "${artifacts}/suite-revision.txt"
git -C "${SINK_SERVER_DIR}" diff HEAD > "${artifacts}/server.patch"
git -C "${suite_dir}" diff HEAD > "${artifacts}/suite.patch"
"${compose[@]}" config > "${artifacts}/compose.yaml"
go test "${suite_go_flags[@]}" ./contract ./internal/... -count=1
# Build the shared local tag before image-only services try to pull it.
"${compose[@]}" build
"${compose[@]}" up --detach --wait --wait-timeout 180
wait_for_readiness
SINK_CANDIDATE_ARTIFACTS="${artifacts}" \
SINK_MONGODB_TEST_URI="mongodb://$("${compose[@]}" port mongodb 27017)/?directConnection=true" \
  bash "${suite_dir}/scripts/test-candidate.sh" mongodb
(
	while true; do
		"${compose[@]}" stats --no-stream --format json >> "${artifacts}/resources.jsonl" || true
		for port in 19090 19091 19093 19094 19100 19101 19092 19102 19103 19121 19104 19105 19106 19107 19108 19109 19124 19110 19111 19112 19113 19126; do
			date -u +%FT%TZ >> "${artifacts}/metrics-${port}.txt"
			curl --max-time 3 --silent "http://127.0.0.1:${port}/metrics" >> "${artifacts}/metrics-${port}.txt" || true
		done
		sleep 30
	done
) &
sampler_pid="$!"

SINK_ADDRESS=127.0.0.1:18080 \
SINK_SECONDARY_ADDRESS=127.0.0.1:18081 \
SINK_SEARCH_ENDPOINT=http://127.0.0.1:19200 \
	run_checked_tests business-contract TestProductMergeMatchesReferenceThroughSinkAndOpenSearch,TestOfferMergeMatchesReferenceThroughSinkAndOpenSearch,TestStoreKafkaRoutingAndSyncOnlyBehavior,TestReliabilityRejectsOversizedAsyncMutation,TestReliabilityReadBudgetCountsRepeatedKeysAcrossStores,TestReliabilityLuaAliasExpansionIsRejectedWithoutWriting -run '^Test(Product.*|Offer.*|StoreKafka.*|Reliability(Rejects.*|ReadBudget.*|LuaAlias.*))$' -timeout=10m

# Stable full membership gives both ordinary Gateways the same key owner.
# These two test-only Gateways have deliberately disjoint Engine views so the
# revision-conflict workload still exercises simultaneous writers on two Engines.
SINK_ADDRESS=127.0.0.1:18082 \
SINK_SECONDARY_ADDRESS=127.0.0.1:18083 \
SINK_SEARCH_ENDPOINT=http://127.0.0.1:19200 \
	run_checked_tests cross-engine TestConcurrentProductMergesAcrossSinkReplicasLoseNoSuccessfulUpdates -run '^TestConcurrentProductMergesAcrossSinkReplicasLoseNoSuccessfulUpdates$' -timeout=3m

backend_required='TestConfiguredStorageBackendsThroughSink,TestBackendOperationStateMachine,TestNativeBackendQueryCountScan,TestNativeBackendExecute,TestNativeBackendReturnedWrites,TestNativeBackendScanCheckpointsDuringBusinessChanges,TestMongoBSONFidelityAcrossMergeAndKafka'
IFS=',' read -r -a required_stores <<< "${backend_stores}"
for backend_spec in "${required_stores[@]}"; do
  backend_required+=",TestNativeBackendQueryCountScan/${backend_spec%%:*}/scan-projection-pages"
done
SINK_ADDRESS=127.0.0.1:18080 \
SINK_SECONDARY_ADDRESS=127.0.0.1:18081 \
SINK_SEARCH_ENDPOINT=http://127.0.0.1:19200 \
SINK_BACKEND_STORES="${backend_stores}" \
	run_checked_tests backend-tests "${backend_required}" -run '^Test(ConfiguredStorageBackendsThroughSink|BackendOperationStateMachine|NativeBackend.*|MongoBSONFidelityAcrossMergeAndKafka)$' -timeout=10m

"${compose[@]}" stop worker-primary
recovery_suffix="$(date +%s)-$$"
recovery_index="sink-qualification-recovery-${recovery_suffix}"
recovery_key="shopify:recovery.example:${recovery_suffix}"

SINK_ADDRESS=127.0.0.1:18080 \
SINK_SECONDARY_ADDRESS=127.0.0.1:18081 \
SINK_SEARCH_ENDPOINT=http://127.0.0.1:19200 \
SINK_RECOVERY_PHASE=publish \
SINK_RECOVERY_INDEX="${recovery_index}" \
SINK_RECOVERY_KEY="${recovery_key}" \
	run_checked_tests recovery-publish TestKafkaBacklogSurvivesWorkerRestart -run '^TestKafkaBacklogSurvivesWorkerRestart$' -timeout=5m

"${compose[@]}" restart kafka
"${compose[@]}" up --detach --wait kafka
"${compose[@]}" start worker-primary
SINK_ADDRESS=127.0.0.1:18080 \
SINK_SECONDARY_ADDRESS=127.0.0.1:18081 \
SINK_SEARCH_ENDPOINT=http://127.0.0.1:19200 \
SINK_RECOVERY_PHASE=verify \
SINK_RECOVERY_INDEX="${recovery_index}" \
SINK_RECOVERY_KEY="${recovery_key}" \
	run_checked_tests recovery-verify TestKafkaBacklogSurvivesWorkerRestart -run '^TestKafkaBacklogSurvivesWorkerRestart$' -timeout=5m

if [[ "${SINK_RUN_LOAD:-0}" == "1" ]]; then
	SINK_ADDRESS=127.0.0.1:18080 \
	SINK_SECONDARY_ADDRESS=127.0.0.1:18081 \
	SINK_SEARCH_ENDPOINT=http://127.0.0.1:19200 \
	SINK_RUN_LOAD=1 \
		run_checked_tests load TestRepresentativeProductMergeLoad -run '^TestRepresentativeProductMergeLoad$' -timeout=10m
fi

if [[ "${SINK_RUN_RESILIENCE:-0}" == "1" ]]; then
	SINK_ADDRESS=127.0.0.1:18080 \
	SINK_SECONDARY_ADDRESS=127.0.0.1:18081 \
	SINK_SEARCH_ENDPOINT=http://127.0.0.1:19200 \
	SINK_BACKEND_STORES="${backend_stores}" \
	SINK_RUN_SOAK=1 \
	SINK_SOAK_DURATION="${SINK_SOAK_DURATION:-3m}" \
	SINK_SOAK_CONCURRENCY="${SINK_SOAK_CONCURRENCY:-8}" \
	SINK_SOAK_MIN_CYCLES="${SINK_SOAK_MIN_CYCLES:-100}" \
		go test "${suite_go_flags[@]}" -tags=integration ./integration -json -count=1 -run '^TestStorageBackendSoak$' -timeout="${SINK_SOAK_TEST_TIMEOUT:-10m}" > "${artifacts}/soak.jsonl" &
	resilience_pid="$!"
	if [[ "${SINK_RUN_SCALING:-0}" == 1 ]]; then
		# Compose run does not publish the service's fixed host ports. These
		# disposable replicas use the same Store, topic and consumer group.
		sleep 15
		record_fault worker-scale-1-to-3
		for replica in 2 3; do
			container="${project}-worker-scaling-${replica}"
			scaling_workers+=("${container}")
			"${compose[@]}" run --detach --no-deps --name "${container}" worker-primary
		done
		wait_for_worker_members 3
		sleep 10
		kill -0 "${resilience_pid}"
		record_fault worker-scale-3-to-0
		"${compose[@]}" stop --timeout 30 worker-primary
		for container in "${scaling_workers[@]}"; do
			docker stop --time 30 "${container}"
		done
		wait_for_worker_members 0
		# Engine still accepts durable writes while all Workers are absent.
		sleep 15
		kill -0 "${resilience_pid}"
		record_fault worker-scale-0-to-1
		"${compose[@]}" start worker-primary
		wait_for_worker_members 1
		sleep 10
		kill -0 "${resilience_pid}"
	fi
	for cycle in $(seq 1 "${fault_cycles}"); do
		record_fault "cycle-${cycle}-start"
		sleep 15
		kill -0 "${resilience_pid}"
		record_fault worker-sigkill
		"${compose[@]}" kill --signal SIGKILL worker-primary
		"${compose[@]}" up --detach worker-primary
		sleep 15
		kill -0 "${resilience_pid}"
		record_fault opensearch-unavailable
		"${compose[@]}" stop opensearch
		if (( cycle % 3 == 0 )); then
			record_fault compound-kafka-unavailable
			"${compose[@]}" pause kafka
			broker_paused=1
		fi
		# This exceeds the product's 20-second processing window and default
		# retry round, while healthy stores must keep serving.
		sleep 45
		curl --max-time 5 --fail --silent http://127.0.0.1:20090/livez >/dev/null
		curl --max-time 5 --fail --silent http://127.0.0.1:20090/readyz >/dev/null
		curl --max-time 5 --fail --silent http://127.0.0.1:20100/readyz >/dev/null
		curl --max-time 5 --fail --silent 'http://127.0.0.1:20110/readyz?service=sink.storage.mongodb-sync' >/dev/null
		readiness_status="$(curl --max-time 5 --silent --output /dev/null --write-out '%{http_code}' 'http://127.0.0.1:20100/readyz?service=sink.storage.primary')"
		if [[ "${readiness_status}" != 503 ]]; then
			echo "unavailable OpenSearch must fail dependency readiness: ${readiness_status}" >&2
			exit 1
		fi
		SINK_ADDRESS=127.0.0.1:18080 \
		SINK_SECONDARY_ADDRESS=127.0.0.1:18081 \
		SINK_BACKEND_STORES='secondary:async,mongodb-sync:sync' \
			run_checked_tests "fault-isolation-${cycle}" \
				'TestConfiguredStorageBackendsThroughSink/secondary,TestConfiguredStorageBackendsThroughSink/mongodb-sync' \
				-run '^TestConfiguredStorageBackendsThroughSink$' -timeout=1m
		record_fault healthy-store-writes-confirmed
		if (( cycle % 3 == 0 )); then
			"${compose[@]}" unpause kafka
			broker_paused=0
			record_fault compound-kafka-recovery
		fi
		record_fault opensearch-recovery
		"${compose[@]}" start opensearch
		"${compose[@]}" up --detach --wait --wait-timeout 180 opensearch
		sleep 15
		kill -0 "${resilience_pid}"
		record_fault kafka-restart
		"${compose[@]}" restart kafka
		"${compose[@]}" up --detach --wait --wait-timeout 180 kafka
		record_fault "cycle-${cycle}-complete"
		if [[ "${cycle}" -lt "${fault_cycles}" ]]; then
			sleep "${fault_interval}"
		fi
	done
	wait "${resilience_pid}"
	resilience_pid=""
	go run ./cmd/check-test-events --file "${artifacts}/soak.jsonl" --require TestStorageBackendSoak
	cat "${artifacts}/soak.jsonl"
	wait_for_readiness
	record_fault all-dependencies-ready
fi

wait_for_zero_group_lag kafka sink-production-workers
wait_for_zero_group_lag kafka sink-production-elasticsearch-workers
wait_for_zero_group_lag kafka-secondary sink-production-secondary-workers
wait_for_zero_group_lag kafka-secondary sink-production-mongodb-workers

assert_empty_dlq kafka sink-production-mutations.dlq
assert_empty_dlq kafka sink-production-elasticsearch-mutations.dlq
assert_empty_dlq kafka-secondary sink-production-secondary-mutations.dlq
assert_empty_dlq kafka-secondary sink-production-mongodb-mutations.dlq

total_conflicts=0
total_exhausted=0
for metrics_port in 19100 19101; do
	metrics="$(curl --fail --silent --show-error "http://127.0.0.1:${metrics_port}/metrics")"
	grep -q '^sink_grpc_server_requests_total' <<<"${metrics}"
	conflicts="$(awk -v metric_name=sink_merge_conflicts_total -f "${script_dir}/metric-total.awk" <<<"${metrics}")"
	exhausted="$(awk -v metric_name=sink_merge_exhausted_total -f "${script_dir}/metric-total.awk" <<<"${metrics}")"
	total_conflicts=$((total_conflicts + conflicts))
	total_exhausted=$((total_exhausted + exhausted))
done
if [[ "${total_conflicts}" -lt 1 ]]; then
	echo "cross-replica test did not exercise a revision conflict" >&2
	exit 1
fi
if [[ "${total_exhausted}" -ne 0 ]]; then
	echo "${total_exhausted} merge operations exhausted their conflict budget" >&2
	exit 1
fi

# Quarantine is expected only for this final, explicit permanent-error case.
# The earlier empty-DLQ checks still protect all normal and outage workloads.
export SINK_ADDRESS=127.0.0.1:18080
export SINK_SECONDARY_ADDRESS=127.0.0.1:18081
export SINK_SEARCH_ENDPOINT=http://127.0.0.1:19200
export SINK_DLQ_INDEX="sink-dlq-$(date +%s)-$$"
SINK_DLQ_PHASE=publish run_checked_tests dlq-publish TestReliabilityDeadLetterRecovery -run '^TestReliabilityDeadLetterRecovery$' -timeout=3m
wait_for_zero_group_lag kafka sink-production-workers
"${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
	--bootstrap-server localhost:19092 --topic sink-production-mutations.dlq > "${artifacts}/dlq-offsets-before.txt"
dlq_summary="$(awk -F: '{count++; total += $3; if ($3 == 1) partition = $2} END {printf "%d:%d:%d", count, total, partition}' "${artifacts}/dlq-offsets-before.txt")"
if [[ "${dlq_summary}" != 8:1:* ]]; then
	echo "expected exactly one permanent failure in DLQ, got ${dlq_summary}" >&2
	exit 1
fi
dlq_partition="${dlq_summary##*:}"
export SINK_DLQ_INSPECT_REPORT="${artifacts}/dlq-inspect.jsonl"
export SINK_DLQ_REPLAY_REPORT="${artifacts}/dlq-replay.jsonl"
"${compose[@]}" exec -T worker-primary /usr/local/bin/sink dlq inspect \
	--config /etc/sink/config.yaml --store primary --partition "${dlq_partition}" --offset 0 --count 1 > "${SINK_DLQ_INSPECT_REPORT}"
SINK_DLQ_PHASE=repair run_checked_tests dlq-repair TestReliabilityDeadLetterRecovery -run '^TestReliabilityDeadLetterRecovery$' -timeout=3m
"${compose[@]}" exec -T worker-primary /usr/local/bin/sink dlq replay \
	--config /etc/sink/config.yaml --store primary --partition "${dlq_partition}" --offset 0 --count 1 > "${SINK_DLQ_REPLAY_REPORT}"
SINK_DLQ_PHASE=verify run_checked_tests dlq-verify TestReliabilityDeadLetterRecovery -run '^TestReliabilityDeadLetterRecovery$' -timeout=3m
wait_for_zero_group_lag kafka sink-production-workers
"${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
	--bootstrap-server localhost:19092 --topic sink-production-mutations.dlq > "${artifacts}/dlq-offsets-after.txt"
diff -u <(sort "${artifacts}/dlq-offsets-before.txt") <(sort "${artifacts}/dlq-offsets-after.txt")
wait_for_readiness
echo 'PASS permanent-failure continuation, DLQ inspection, repair, replay, and preserved DLQ offsets'
