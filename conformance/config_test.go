//go:build integration

package conformance_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type admissionQueueOptions struct {
	requests int
	bytes    int
	wait     time.Duration
}

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
	if opts.snapshotBytes > 0 {
		fmt.Fprintf(&component, "  max_snapshot_bytes: %d\n", opts.snapshotBytes)
	}
	if opts.outputBytes > 0 {
		fmt.Fprintf(&component, "  max_output_bytes: %d\n", opts.outputBytes)
	}
	if mode == "engine" {
		fmt.Fprintf(&component, "batching:\n  max_operations: %d\n  max_wait: %dms\n  queue: {max_operations: %d}\n", defaultInt(opts.batchOps, 1000), defaultInt(opts.batchWait, 2), defaultInt(opts.queued, 10000))
	} else {
		fmt.Fprintf(&component, "consumer:\n  group_id: %s-workers\n  retry: {backoff: 10ms, max_backoff: 100ms}\n", opts.topic)
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
	shared := fmt.Sprintf("name: %s\nstorage: {driver: %s, search: {endpoints: %s}}\n", store, opts.backend.driver, encoded)
	if opts.broker != "" {
		shared += fmt.Sprintf("kafka:\n  enabled: true\n  brokers: [%q]\n  topic: {name: %s, partitions: 1, replication_factor: 1}\n  dead_letter: {topic: %s.dlq}\n", opts.broker, opts.topic, opts.topic)
	}
	return component.String(), shared
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
