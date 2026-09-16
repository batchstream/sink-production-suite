#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
suite_dir="$(cd "${script_dir}/.." && pwd)"
export SINK_SERVER_DIR="${SINK_SERVER_DIR:-${suite_dir}/../sink}"
phase="${1:-unit}"
artifacts="${SINK_CANDIDATE_ARTIFACTS:-$(mktemp -d "${TMPDIR:-/tmp}/sink-candidate.XXXXXXXX")}"
mkdir -p "${artifacts}"
trap 'echo "Candidate regression evidence: ${artifacts}"' EXIT
git -C "${SINK_SERVER_DIR}" rev-parse HEAD > "${artifacts}/candidate-revision.txt"
git -C "${SINK_SERVER_DIR}" diff HEAD > "${artifacts}/candidate.patch"

case "${phase}" in
  unit)
    # Mutation decoder fuzz seeds intentionally skip invalid inputs. Fuzzing has
    # its own gate; require every ordinary regression in this invocation.
    packages=()
    while IFS= read -r package; do
      [[ -z "${package}" ]] || packages+=("${package}")
    done < <(go -C "${SINK_SERVER_DIR}" list -mod=readonly -f '{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}}{{end}}' ./...)
    if [[ "${#packages[@]}" == 0 ]]; then
      echo "candidate contains no ordinary test packages" >&2
      exit 1
    fi
    flags=(-run '^(Test|Example)')
    required='TestApplicationAlwaysBatchesGRPCRequests,TestLuaTestNumberComparisonIsExact,TestDeadLetterReplayPreservesPublisherRouting,TestLuaMergePreservesBSONScalarTypesAndLiteralObjects,TestLuaMergeRejectsJSONIntegerPrecisionLoss,TestLuaNativeBudgetFailureDoesNotCommitPartialMutation,TestLuaConversionDeadlineDoesNotCommitMutation,TestSinkV1CumulativeBudgetFailureDoesNotCommitMutation,TestLuaPatternCaptureStackFailureDoesNotCommitMutation,TestWriteCancellationInterruptsLuaCompilation,TestInvalidDocumentFailsBeforeWriteOrPublish,TestVTRecordRequestsRejectInvalidUTF8Identities,TestNativeRPCKeepsBackendDiagnosticsBounded,TestFailureSpaceIsReservedBeforeMutation,TestTinyFailureBudgetsKeepNonemptyMessages,TestReadMicrobatchRetainsCallerLimitsAndOrderAcrossShrinking,TestCountReservesBoundedResponsesAndReleasesCapacity,TestProcessorSplitsRejectedWritesAndDeletesWithoutReplayingSuccess,TestWorkerSplitsCapacityRejectedPollAndCommits,TestClientBulkDiscoveryCannotUndoCommandRejection,TestManagedQueriesFailOverToHealthyEndpoint,TestReadSplitsOversizedResponsesWithoutEndpointFailover,TestCanceledReadDoesNotFailOverOrCoolDownEndpoints,TestMultiGetRejectsUnidentifiedAndMisorderedResults,TestBulkValidatesAllResultsBeforeAcknowledging'
    required+=',TestDirectAdmissionQueuesBurst,TestDirectAdmissionBoundsQueuedCountAndBytes,TestDirectAdmissionWaitDeadlineAndImpossibleRequests,TestAdmissionHandoffFillsEveryAvailableSlot,TestDirectAdmissionWaitConsumesRequestDeadline,TestReturnedPutsShareKnownDocumentReservation,TestAdmissionPreservesCancellationAfterWakeup,TestScanAdmissionAndExecutionSharePageDeadline,TestScanAdmissionWaitIsBoundedAndCanceled,TestScanWaitQueueBounds,TestOversizedScanDoesNotWaitOrAdvertiseRetry,TestWriteBatchSplitsAtExecutionByteLimit,TestReadBatchSplitsAtExecutionByteLimit,TestStoreForwardingLimitDoesNotBlockHealthyStore,TestCrossStoreReturnBudgetCheckedBeforeCommit,TestMisroutedEngineCannotWrite,TestIsolatedRoles,TestPrometheusRequiresExplicitOptIn,TestHealthEndpointsDoNotRequirePrometheus,TestRejectRemovedStoreSettings,TestRejectRemovedGatewaySettings,TestHealthAddressIsIndependentOfPrometheus,TestRoutesRejectDuplicateStoreNames,TestGatewayRoutesChangeOnlyAfterRestart,TestEngineRejectsStaleProtocolAndMismatchedStoreBeforeWrites,TestScanProjectionSurvivesWireAndRejectsInvalidFields'
    required+=',TestRecordAffinityAcrossGatewaysAndRPCBoundaries,TestAffinityMembershipChangesMoveOnlyAffectedOwners,TestReplicaReturnBudgetRemainsScopedToOriginalRPC,TestDiscoverySnapshotsSurviveRefreshAndErrors,TestRecordAddressContainsOnlyCanonicalURI,TestNativeCommandUsesResourceURI,TestNativeURITargetsResourceAndKeepsOperationSeparate,TestNativeURIQueryUsesResourceWithoutOperationSuffix,TestMongoNativeURISelectsDatabase'
    required+=',TestHTTPReadinessRejectsClosedRoles,TestReadinessProbeCannotRestoreClosedRole,TestShutdownWithdrawsReadinessAndDrainsAcceptedRPC,TestMembershipWithdrawalRetainsInFlightRequestSnapshot,TestAffinityRoutingKeepsPublishedHashMapping'
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
  *) echo "usage: $0 [unit|elasticsearch|opensearch|mongodb]" >&2; exit 1 ;;
esac

go -C "${SINK_SERVER_DIR}" test -mod=readonly -race "${flags[@]}" "${packages[@]}" \
  -count=1 -timeout=10m -json > "${artifacts}/${phase}.jsonl"
go -C "${suite_dir}" run ./cmd/check-test-events --file "${artifacts}/${phase}.jsonl" --require "${required}"

if [[ "${phase}" == mongodb || "${phase}" == opensearch ]]; then
  go -C "${SINK_SERVER_DIR}" test -mod=readonly -race -tags=integration ./internal/service \
    -run "^(TestSynchronousStorageStreamsLargeRecords|TestReadMicrobatchStorageWorkingSet)$/^${phase}$" \
    -count=1 -timeout=5m -json > "${artifacts}/${phase}-service.jsonl"
  go -C "${suite_dir}" run ./cmd/check-test-events --file "${artifacts}/${phase}-service.jsonl" \
    --require "TestSynchronousStorageStreamsLargeRecords/${phase}/snapshots,TestSynchronousStorageStreamsLargeRecords/${phase}/outputs,TestReadMicrobatchStorageWorkingSet/${phase}"
fi
