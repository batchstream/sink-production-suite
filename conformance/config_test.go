//go:build integration

package conformance_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// Component and Store are independently parsed YAML documents.
func candidateConfigs(opts serverOptions, addresses []string) (string, string) {
	mode := opts.role
	if mode == "" {
		mode = "engine"
	}
	if opts.worker {
		mode = "worker"
	}
	var component strings.Builder
	fmt.Fprintf(&component, "mode: %s\nhealth: {address: %q}\nprometheus: {enabled: true, address: %q}\nshutdown_timeout: 2s\n", mode, addresses[2], addresses[1])
	if mode != "worker" {
		fmt.Fprintf(&component, "grpc: {address: %q, max_send_message_bytes: %d}\n", addresses[0], defaultInt(opts.readBytes, 64<<20))
	}
	if opts.memoryBytes > 0 {
		fmt.Fprintf(&component, "memory: {max_bytes: %s}\n", readableByteSize(opts.memoryBytes))
	}
	component.WriteString(opts.logging)
	if mode == "gateway" {
		fmt.Fprintf(&component, "request: {max_operations: %d}\nforwarding:\n%s\n", defaultInt(opts.maxOps, 1000), opts.routes)
		return component.String(), ""
	}
	fmt.Fprintf(&component, "execution:\n  merge:\n    max_attempts: 50\n    lua: {max_instructions: %d}\n", defaultInt(opts.luaInstructions, 1000000))
	if mode == "engine" {
		fmt.Fprintf(&component, "  queue: {max_tasks: %d}\n", defaultInt(opts.queuedTasks, 10000))
		fmt.Fprintf(&component, "batching:\n  max_operations: %d\n  max_wait: %dms\n  queue: {max_operations: %d}\n", defaultInt(opts.batchOps, 1000), defaultInt(opts.batchWait, 2), defaultInt(opts.queued, 10000))
	} else {
		fmt.Fprintf(&component, "consumer:\n  group_id: %s-workers\n  retry: {max_attempts: %d, backoff: 10ms, max_backoff: 100ms}\n", opts.topic, defaultInt(opts.workerAttempts, 10))
	}
	store := opts.store
	if store == "" {
		store = "primary"
	}
	endpoints := opts.endpoints
	if len(endpoints) == 0 {
		endpoints = []string{opts.backend.endpoint}
	}
	encoded, _ := json.Marshal(endpoints)
	shared := fmt.Sprintf("name: %s\nmax_concurrent: %d\nstorage: {driver: %s, search: {endpoints: %s}}\n", store, defaultInt(opts.storeConcurrent, 64), opts.backend.driver, encoded)
	if opts.broker != "" {
		shared += fmt.Sprintf("kafka:\n  enabled: true\n  brokers: [%q]\n  partitions: 1\n  replication_factor: 1\n  topic: {name: %s}\n  dead_letter: {name: %s.dlq}\n", opts.broker, opts.topic, opts.topic)
	}
	return component.String(), shared
}

func TestCandidateConfigsPlaceConcurrencyInStoreFile(t *testing.T) {
	opts := serverOptions{
		storeConcurrent: 8,
		backend:         backend{driver: "elasticsearch", endpoint: "http://search:9200"},
	}
	component, store := candidateConfigs(opts, []string{"127.0.0.1:1", "127.0.0.1:2", "127.0.0.1:3"})
	if strings.Contains(component, "store_max_concurrent") {
		t.Fatal("role config still contains the removed setting")
	}
	if !strings.Contains(store, "max_concurrent: 8\n") {
		t.Fatalf("Store config lacks max_concurrent: %s", store)
	}
	if !strings.Contains(component, "queue: {max_tasks: 10000}\n") {
		t.Fatalf("Engine config lacks a default ready-task bound: %s", component)
	}
	opts.queued = 12
	opts.queuedTasks = 7
	configured, _ := candidateConfigs(opts, []string{"127.0.0.1:1", "127.0.0.1:2", "127.0.0.1:3"})
	if !strings.Contains(configured, "queue: {max_tasks: 7}\n") || !strings.Contains(configured, "queue: {max_operations: 12}") {
		t.Fatalf("batch operation and admission task limits were conflated: %s", configured)
	}
}

func readableByteSize(value int) string {
	for _, unit := range []struct {
		name  string
		bytes int
	}{
		{name: "GiB", bytes: 1 << 30},
		{name: "MiB", bytes: 1 << 20},
		{name: "KiB", bytes: 1 << 10},
	} {
		if value >= unit.bytes && value%unit.bytes == 0 {
			return fmt.Sprintf("%d%s", value/unit.bytes, unit.name)
		}
	}
	return fmt.Sprintf("%dB", value)
}
