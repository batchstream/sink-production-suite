#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
suite_dir="$(cd "${script_dir}/.." && pwd)"
export SINK_SERVER_DIR="${SINK_SERVER_DIR:-${suite_dir}/../sink}"
phase="${1:-local}"
artifacts="${SINK_CANDIDATE_ARTIFACTS:-$(mktemp -d "${TMPDIR:-/tmp}/sink-candidate.XXXXXXXX")}"
mkdir -p "${artifacts}"
export SINK_CANDIDATE_ARTIFACTS="$(cd "${artifacts}" && pwd)"
artifacts="${SINK_CANDIDATE_ARTIFACTS}"
trap 'echo "Candidate regression evidence: ${artifacts}"' EXIT
git -C "${SINK_SERVER_DIR}" rev-parse HEAD > "${artifacts}/candidate-revision.txt"
git -C "${SINK_SERVER_DIR}" diff HEAD > "${artifacts}/candidate.patch"

case "${phase}" in
  local)
    # Include the candidate unit tests and the suite-owned loopback/component
    # regressions. Input fuzzing has its own gate; skip only fuzz seeds here.
    packages=()
    while IFS= read -r package; do
      [[ -z "${package}" ]] || packages+=("${package}")
    done < <(bash "${script_dir}/server-go.sh" list -f '{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}}{{end}}' ./...)
    if [[ "${#packages[@]}" == 0 ]]; then
      echo "candidate contains no ordinary test packages" >&2
      exit 1
    fi
    flags=(-run '^(Test|Example)')
    required='TestApplicationAlwaysBatchesGRPCRequests,TestEngineOnlyAcceptsForwardingAndHonorsGatewayContext,TestBatchWithoutCallerDeadlineHasNoSyntheticDeadline,TestTypedStreamRejectsInvalidFramesAndRejectionMarkers,TestForwardRejectionTrailerDistinguishesUnknownMutation,TestGatewayMessageLimitDoesNotAdvertiseWriteNonExecution,TestNativeStreamCompletionAndLateErrors,TestLuaTestNumberComparisonIsExact,TestDeadLetterReplayPreservesPublisherRouting,TestLuaMergePreservesBSONScalarTypesAndLiteralObjects,TestLuaMergeRejectsJSONIntegerPrecisionLoss,TestLuaNativeBudgetFailureDoesNotCommitPartialMutation,TestLuaConversionDeadlineDoesNotCommitMutation,TestSinkV1CumulativeBudgetFailureDoesNotCommitMutation,TestLuaPatternCaptureStackFailureDoesNotCommitMutation,TestWriteCancellationInterruptsLuaCompilation,TestInvalidDocumentFailsBeforeWriteOrPublish,TestVTRecordRequestsRejectInvalidUTF8Identities,TestNativeRPCKeepsBackendDiagnosticsBounded,TestTinyFailureBudgetsKeepNonemptyMessages,TestClientBulkDiscoveryCannotUndoCommandRejection,TestManagedQueriesFailOverToHealthyEndpoint,TestReadSplitsOversizedResponsesWithoutEndpointFailover,TestCanceledReadDoesNotFailOverOrCoolDownEndpoints,TestMultiGetRejectsUnidentifiedAndMisorderedResults,TestBulkValidatesAllResultsBeforeAcknowledging'
    required+=',TestReturnedPutsKeepCollectedBatch,TestCrossStoreReturnBudgetAppliesPerResult,TestMisroutedEngineCannotWrite,TestSharedStoreWithIndependentRoleSettings,TestComponentDefaults,TestHealthEndpointsDoNotRequirePrometheus,TestRoleBoundariesAndRemovedFields,TestStrictDocumentsAndRequiredPaths,TestHealthAndMetricsIndependent,TestRoutesRejectDuplicateStoreNames,TestGatewayRoutesChangeOnlyAfterRestart,TestEngineRejectsStaleProtocolAndMismatchedStoreBeforeWrites,TestScanProjectionSurvivesWireAndRejectsInvalidFields'
    required+=',TestStartupMemoryMinimumForEveryRole,TestStartupMemoryEstimateCannotOverflow,TestWatermarksOnlyGateNewWork,TestWatermarkWaitRecoveryAndCancellation,TestConcurrentAdmissionAndMetrics,TestNativeBoundariesRetainWireLimitsAndCallerContext,TestNativeValidationStopsBeforeBackend,TestIncompleteOrCanceledResultsRemainRetryable,TestProcessorHandlesCollectedWritesAndDeletesWithoutReplayingSuccess,TestWorkerPausesForMemoryPressureAndCommitsCollectedPoll,TestReadMicrobatchRetainsCallerResponseLimitsAndOrder,TestCollectedMergesUseProcessMemoryInsteadOfDocumentQuotas,TestSlowForwardingDoesNotBlockHealthyStore,TestMemoryPressureRejectsBeforeForwarding,TestNativeMemoryPressurePreservesScanRetryAndCancellation'
    required+=',TestRecordAffinityAcrossGatewaysAndRPCBoundaries,TestAffinityMembershipChangesMoveOnlyAffectedOwners,TestReplicaReturnBudgetIsScopedToEachResult,TestDiscoverySnapshotsSurviveRefreshAndErrors,TestRecordAddressContainsOnlyCanonicalURI,TestNativeCommandUsesResourceURI,TestNativeURITargetsResourceAndKeepsOperationSeparate,TestNativeURIQueryUsesResourceWithoutOperationSuffix,TestMongoNativeURISelectsDatabase'
    required+=',TestNativeTransportSharesStoreAdmission'
    required+=',TestHealthyStartupOpensBoundedWindow,TestReadAdmissionFIFOAndCancellationReleaseEveryQueuePosition,TestReadAdmissionByteLimitAndCapacityReuse,TestReadAdmissionPreservesContextAndOnlyCallerEndsWaiting,TestQueuedHealthyReadsGrowWithoutRejectingRequests,TestReadAdmissionWaitsThroughCooldownWithoutRevokingActiveWork,TestReadAdmissionCancelReleaseContentionDoesNotLeak,TestQueryAndCountWaitForStoreCapacityWithoutNewDeadline,TestEngineCompletesStoreStartupBeforeServingReadiness,TestEngineStartupHonorsProcessCancellation'
    required+=',TestHTTPReadinessRejectsClosedRoles,TestReadinessProbeCannotRestoreClosedRole,TestShutdownWithdrawsReadinessAndDrainsAcceptedRPC,TestMembershipWithdrawalRetainsInFlightRequestSnapshot,TestAffinityRoutingKeepsPublishedHashMapping'
    required+=',TestConsoleDefaultsToText,TestConsoleJSONLabelsAndLevel,TestGlobalLevelAppliesToAllComponents,TestFailureBodyRequiresOptInErrorAndRemainsBounded,TestOTLPWireSurvivesIngestionProjection,TestCollectorOutageDoesNotBlockLoggingAndReportsQueueLoss,TestShutdownDeadlineWhileCollectorIsUnavailable,TestTLSNeverFallsBackToPlaintext,TestOTLPRecoversWithoutRestart,TestRPCDiagnosticsCaptureApplicationFailuresWithoutPayload,TestRPCDiagnosticsCaptureApplicationFailuresWithoutPayload/managed_failure,TestStreamDiagnosticsPreserveOutcomeAndExcludeHealth,TestSuppressedFailureBodiesAreNotEvaluated'
    ;;
  elasticsearch|opensearch)
    : "${SINK_SEARCH_TEST_ENDPOINT:?disposable backend endpoint is required}"
    export SINK_SEARCH_TEST_DRIVER="${phase}"
    packages=(./internal/storage/search)
    flags=(-tags=integration)
    required='TestSearchInvalidUTF8CannotOverwriteUnicodeKey,TestSearchScanShrinksLargeResponses,TestSearchScanProjectionRetainsSortOutsideSource,TestQueryDoesNotFetchLookaheadSourceOrExceedWindow,TestManagedQueriesCannotOverwriteSearchNamedDocument,TestSearchStorageLifecycleThroughIndexAlias,TestSearchReplaceLargeOldDocumentsUsesMetadataOnly'
    ;;
  mongodb)
    : "${SINK_MONGODB_TEST_URI:?disposable replica-set URI is required}"
    packages=(./internal/storage/mongodb)
    flags=(-tags=integration)
    required='TestMongoDBNumericIDRoundTrip,TestRecordOperationsKeepByteIdentityWithCollectionCollation,TestNativeMongoQueryPreservesBusinessFieldsAndLiteralStageNames,TestMongoScanProjectionAndContinuation,TestMongoQueryFetchesLookaheadInFirstBatch,TestMongoDBUniqueConstraintsPreservePublicWriteStatus,TestMongoDBQueuedConstraintFailureAllowsFollowingCorrection,TestMongoDBLargeConditionalReplacementPreservesLiteralBSONAndUsesDeltaOplog,TestMongoDBConditionalWritesSelectCapabilityAndPreserveDurabilityErrors'
    ;;
  *) echo "usage: $0 [local|elasticsearch|opensearch|mongodb]" >&2; exit 1 ;;
esac

bash "${script_dir}/server-go.sh" test -race -covermode=atomic -coverpkg=./... -coverprofile="${artifacts}/${phase}.out" "${flags[@]}" "${packages[@]}" \
  -count=1 -timeout=10m -json > "${artifacts}/${phase}.jsonl"
go -C "${suite_dir}" run ./cmd/check-test-events --file "${artifacts}/${phase}.jsonl" --require "${required}"

if [[ "${phase}" == mongodb || "${phase}" == opensearch ]]; then
  bash "${script_dir}/server-go.sh" test -race -covermode=atomic -coverpkg=./... -coverprofile="${artifacts}/${phase}-service.out" -tags=integration ./internal/service \
    -run "^(TestSynchronousStorageProcessesCollectedRecords|TestReadMicrobatchStorageWorkingSet)$/^${phase}$" \
    -count=1 -timeout=5m -json > "${artifacts}/${phase}-service.jsonl"
  go -C "${suite_dir}" run ./cmd/check-test-events --file "${artifacts}/${phase}-service.jsonl" \
    --require "TestSynchronousStorageProcessesCollectedRecords/${phase}/snapshots,TestSynchronousStorageProcessesCollectedRecords/${phase}/outputs,TestReadMicrobatchStorageWorkingSet/${phase}"
fi

coverage_flags=(--profile "${artifacts}/${phase}.out" --report "${artifacts}/${phase}-coverage.md")
if [[ "${phase}" == local ]]; then
  coverage_flags+=(--minimums "${suite_dir}/.github/candidate-coverage-minimums.json")
fi
python3 "${suite_dir}/scripts/check-coverage.py" "${coverage_flags[@]}"
