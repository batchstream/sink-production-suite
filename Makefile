.PHONY: test test-race fuzz test-integration test-production test-reliability test-conformance test-candidate test-isolated test-quorum lint

STATICCHECK_VERSION := v0.8.1
FUZZ_TIME ?= 180s

test:
	go test ./... -count=1

test-race:
	go test -race ./... -count=1

test-candidate:
	bash scripts/test-candidate.sh unit

fuzz:
	FUZZ_TIME=$(FUZZ_TIME) bash scripts/test-fuzz.sh FuzzProductMergeSequence
	FUZZ_TIME=$(FUZZ_TIME) bash scripts/test-fuzz.sh FuzzOfferMergeSequence

test-isolated:
	bash scripts/test-isolated.sh

test-conformance:
	bash scripts/test-conformance.sh

test-integration: test-conformance
	bash scripts/test-integration.sh

test-production: export SINK_SOAK_DURATION ?= 6m
test-production: test-candidate test-conformance
	SINK_RUN_LOAD=1 SINK_RUN_RESILIENCE=1 SINK_RUN_SCALING=1 bash scripts/test-integration.sh
	bash scripts/test-mongodb-quorum.sh

test-quorum:
	bash scripts/test-mongodb-quorum.sh

test-reliability: export SINK_CONFORMANCE_TEST_TIMEOUT ?= 30m
test-reliability: test-candidate test-conformance
	SINK_RUN_LOAD=1 SINK_RUN_RESILIENCE=1 SINK_RUN_SCALING=1 SINK_SOAK_DURATION=2h SINK_SOAK_CONCURRENCY=16 SINK_SOAK_MIN_CYCLES=1000 SINK_SOAK_TEST_TIMEOUT=150m SINK_FAULT_CYCLES=12 SINK_FAULT_INTERVAL_SECONDS=300 bash scripts/test-integration.sh

lint:
	@test -z "$$(gofmt -l .)"
	go vet -tags=integration ./...
	go run honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION) -tags=integration -checks=all ./...
