#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
suite_dir="$(cd "${script_dir}/.." && pwd)"
export SINK_SERVER_DIR="${SINK_SERVER_DIR:-${suite_dir}/../sink}"
export SINK_CONFORMANCE_ARTIFACTS="$(mktemp -d "${TMPDIR:-/tmp}/sink-conformance.XXXXXXXX")"
export SINK_SERVER_BINARY="${SINK_CONFORMANCE_ARTIFACTS}/sink"
# Validate the current protocol and configuration together.
project="sink-conformance-$(date +%s)-$$"
compose=(docker compose --env-file /dev/null --project-name "${project}" --project-directory "${suite_dir}" --file "${suite_dir}/deploy/compose.yaml" --file "${SINK_CONFORMANCE_ARTIFACTS}/ports.yaml")
exec > >(tee "${SINK_CONFORMANCE_ARTIFACTS}/run.log") 2>&1

cleanup() {
  result="$?"
  trap - EXIT
  "${compose[@]}" logs --no-color > "${SINK_CONFORMANCE_ARTIFACTS}/backends.log" 2>&1 || true
  "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  echo "Conformance evidence: ${SINK_CONFORMANCE_ARTIFACTS}"
  exit "${result}"
}
trap cleanup EXIT

# Dynamic loopback ports isolate this runner from other qualification projects.
cat > "${SINK_CONFORMANCE_ARTIFACTS}/ports.yaml" <<'YAML'
services:
  elasticsearch:
    ports: !override
      - "127.0.0.1::9200"
  opensearch:
    image: ${SINK_CONFORMANCE_OPENSEARCH_IMAGE:-opensearchproject/opensearch:3.8.0@sha256:bcc1797519726ceb6d651d4a3e60b7c30da91793914a8dfe75fd441d4f641509}
    ports: !override
      - "127.0.0.1::9200"
YAML

git -C "${SINK_SERVER_DIR}" rev-parse HEAD > "${SINK_CONFORMANCE_ARTIFACTS}/server-revision.txt"
git -C "${suite_dir}" rev-parse HEAD > "${SINK_CONFORMANCE_ARTIFACTS}/suite-revision.txt"
# Retain changes to tracked source as well as the base revision for local runs.
git -C "${SINK_SERVER_DIR}" diff HEAD > "${SINK_CONFORMANCE_ARTIFACTS}/server.patch"
git -C "${suite_dir}" diff HEAD > "${SINK_CONFORMANCE_ARTIFACTS}/suite.patch"
"${compose[@]}" config > "${SINK_CONFORMANCE_ARTIFACTS}/compose.yaml"
go -C "${SINK_SERVER_DIR}" build -race -o "${SINK_SERVER_BINARY}" ./cmd/sink
# Reject stale release fixtures before starting backends or long-running tests.
for config in "${suite_dir}"/deploy/gateway*.yaml; do
  "${SINK_SERVER_BINARY}" config check --config "${config}"
done
for role in engines workers; do
  for config in "${suite_dir}/deploy/${role}"/*.yaml; do
    "${SINK_SERVER_BINARY}" config check --config "${config}" \
      --store-config "${suite_dir}/deploy/stores/${config##*/}"
  done
done
"${compose[@]}" up --detach --wait --wait-timeout 180 elasticsearch opensearch
export SINK_CONFORMANCE_ELASTICSEARCH="http://$("${compose[@]}" port elasticsearch 9200)"
export SINK_CONFORMANCE_OPENSEARCH="http://$("${compose[@]}" port opensearch 9200)"
SINK_CANDIDATE_ARTIFACTS="${SINK_CONFORMANCE_ARTIFACTS}" SINK_SEARCH_TEST_ENDPOINT="${SINK_CONFORMANCE_ELASTICSEARCH}" \
  bash "${suite_dir}/scripts/test-candidate.sh" elasticsearch
SINK_CANDIDATE_ARTIFACTS="${SINK_CONFORMANCE_ARTIFACTS}" SINK_SEARCH_TEST_ENDPOINT="${SINK_CONFORMANCE_OPENSEARCH}" \
  bash "${suite_dir}/scripts/test-candidate.sh" opensearch
cd "${suite_dir}"
suite_go_flags=(-mod=readonly)
if [[ -n "${SINK_GO_DIR:-}" ]]; then
  cp go.mod "${SINK_CONFORMANCE_ARTIFACTS}/suite.go.mod"
  cp go.sum "${SINK_CONFORMANCE_ARTIFACTS}/suite.go.sum"
  go mod edit -modfile="${SINK_CONFORMANCE_ARTIFACTS}/suite.go.mod" -replace="github.com/batchstream/sink-go=${SINK_GO_DIR}"
  suite_go_flags+=("-modfile=${SINK_CONFORMANCE_ARTIFACTS}/suite.go.mod")
fi
# Run every ordinary SDK test and require the latest Scan retry and projection
# regressions. A stale SDK with missing tests must not silently pass the gate.
go test "${suite_go_flags[@]}" -race github.com/batchstream/sink-go \
  -run '^(Test|Example)' -count=1 -timeout=3m -json > "${SINK_CONFORMANCE_ARTIFACTS}/client-unit-tests.jsonl"
go run ./cmd/check-test-events --file "${SINK_CONFORMANCE_ARTIFACTS}/client-unit-tests.jsonl" \
  --require 'TestScanRetriesAdmissionWithIdenticalPage,TestScanAdmissionRetriesAreBoundedOrDisabled,TestScanDoesNotRetryUnmarkedOrOtherFailures,TestScanRetryBackoffHonorsCancellationAndTotalTimeout,TestScanProjectionWirePresenceAndValidation'
# Exercise real loopback DNS with healthy scale-out, scale-in, SERVFAIL and
# default/custom refresh intervals; these opt-in tests need no storage backend.
SINK_CANDIDATE_ARTIFACTS="${SINK_CONFORMANCE_ARTIFACTS}" bash "${script_dir}/server-go.sh" test -race -tags=integration ./internal/gateway \
  -run '^TestGateway(DiscoversDNSScaleChanges|DNSWithdrawalDrainBoundary)$' \
  -count=1 -timeout=2m -json > "${SINK_CONFORMANCE_ARTIFACTS}/gateway-dns-tests.jsonl"
go run ./cmd/check-test-events --file "${SINK_CONFORMANCE_ARTIFACTS}/gateway-dns-tests.jsonl" \
  --require 'TestGatewayDiscoversDNSScaleChanges,TestGatewayDNSWithdrawalDrainBoundary/withdraw-before-stop,TestGatewayDNSWithdrawalDrainBoundary/stop-before-refresh,TestGatewayDNSWithdrawalDrainBoundary/dns-outage-during-stop,TestGatewayDNSWithdrawalDrainBoundary/stale-cache-during-stop,TestGatewayDNSWithdrawalDrainBoundary/scale-to-zero'
go test "${suite_go_flags[@]}" -race -tags=integration github.com/batchstream/sink-go \
  -run '^TestDial(BalancesWritesAndFollowsEndpointChanges|DiscoversDNSScaleChangesWithHealthyConnections)$' \
  -count=1 -timeout=3m -json | tee "${SINK_CONFORMANCE_ARTIFACTS}/client-tests.jsonl"
go run ./cmd/check-test-events --file "${SINK_CONFORMANCE_ARTIFACTS}/client-tests.jsonl" \
  --require 'TestDialBalancesWritesAndFollowsEndpointChanges,TestDialDiscoversDNSScaleChangesWithHealthyConnections/default,TestDialDiscoversDNSScaleChangesWithHealthyConnections/one-second'
# Each public test now starts both Gateway and Engine; reserve time for both
# process lifecycles while preserving the per-request fault deadlines.
go test "${suite_go_flags[@]}" -race -tags=integration ./conformance -count=1 -timeout="${SINK_CONFORMANCE_TEST_TIMEOUT:-40m}" -json | tee "${SINK_CONFORMANCE_ARTIFACTS}/tests.jsonl"
required_tests='TestStartupRejectsInsufficientMemoryBeforeDependencies,TestProcessLoggingSurvivesCollectorOutage/elasticsearch,TestProcessLoggingSurvivesCollectorOutage/opensearch,TestHotKeyMergeAmplification,TestAppliedDoesNotInheritVisibleRefresh,TestCompletedDocumentReleasedBeforeSiblingRead,TestSuccessfulSiblingNotReplayedDuringConflict,TestVisibleDatasetsCompleteIndependently,TestReadStreamsUsePerResultLimits,TestFormattedJSONBulkFraming,TestReplaceRechecksExistenceAfterConflict,TestQueuedCancellationDoesNotPoisonFollowingWrites,TestOperationStateMachine,TestSyncCrashBoundaries,TestLostBackendResponseDoesNotReplayMutation,TestCancellationAfterCommitRetainsState,TestAcceptedMutationCrashBoundaries,TestConcurrentHistories,TestSlowStoreDoesNotBlockIndependentWork,TestWorkerRetainsStorageFailures'
required_tests+=',TestNativeRejectsIncompleteBackendResults,TestNativeScanCancellationReleasesCursorAndAdmission,TestNativeExecuteLostResponseDoesNotReplay,TestReturnedWriteCommitAndConflictBoundaries,TestReturnedWriteStreamsUsePerResultLimits,TestNativeResponseLimitsFailWithoutTruncation,TestNativeWireValidationBeforeExecution'
required_tests+=',TestNativeScanDeadlinesReleaseResources,TestNativeScanResumesAfterServerExit'
required_tests+=',TestReturnedChainReleasesIndependentPut'
required_tests+=',TestReplaceConflictExhaustionIsRetryable'
required_tests+=',TestStoreIsolatedGatewayPublicContract,TestStoreIsolatedWorkerColdStartWithoutEngine'
required_tests+=',TestRequestGateDiscardPreventsLateForwarding'
required_tests+=',TestPublishingSurvivesSynchronousSaturation,TestSynchronousWritesSurvivePublisherSaturation,TestSynchronousMergesProcessCollectedWorkingSets'
required_tests+=',TestLuaBudgetFailuresPreserveStateAndSiblings,TestManagedQueriesCannotMutateDocuments,TestManagedQueryEndpointRecovery,TestQueryLookaheadDoesNotConsumeDocumentBudget'
required_tests+=',TestDirectBurstsProgressAcrossStores,TestDirectCancellationReleasesBackend,TestReturnedPutsKeepCollectedBatch,TestScanProjectionSurvivesConcurrentCount'
required_tests+=',TestStoreColdStartQueuesMixedBurstWithoutRejection'
required_tests+=',TestStoreBackpressureKeepsSharedAdmissionBounded,TestStoreBackpressureReplicasConvergeAndRecover,TestStoreBackpressureWorkerRetainsBacklogAndRecovers'
required_tests+=',TestReadyStoreAdmitsColdQueryBurst,TestQueryAndCountAdmissionQueueBoundedAndCancelable'
for backend in elasticsearch opensearch; do
  required_tests+=",TestReadyStoreAdmitsColdQueryBurst/${backend},TestQueryAndCountAdmissionQueueBoundedAndCancelable/${backend}/Query,TestQueryAndCountAdmissionQueueBoundedAndCancelable/${backend}/Count"
  required_tests+=",TestStoreColdStartQueuesMixedBurstWithoutRejection/${backend}/1,TestStoreColdStartQueuesMixedBurstWithoutRejection/${backend}/4"
  required_tests+=",TestStoreBackpressureKeepsSharedAdmissionBounded/${backend},TestStoreBackpressureWorkerRetainsBacklogAndRecovers/${backend},TestStoreBackpressureReplicasConvergeAndRecover/${backend}/1,TestStoreBackpressureReplicasConvergeAndRecover/${backend}/4"
  for operation in insert remove sort unpack move concat pack packsize string-unpack pattern unicode cumulative-helper; do
    required_tests+=",TestLuaBudgetFailuresPreserveStateAndSiblings/${backend}/${operation}"
  done
  for method in Query Count Scan; do
    required_tests+=",TestManagedQueriesCannotMutateDocuments/${backend}/${method},TestManagedQueryEndpointRecovery/${backend}/${method}"
  done
  required_tests+=",TestManagedQueryEndpointRecovery/${backend}/Execute,TestQueryLookaheadDoesNotConsumeDocumentBudget/${backend}"
done
go run ./cmd/check-test-events --file "${SINK_CONFORMANCE_ARTIFACTS}/tests.jsonl" --require "${required_tests}"
