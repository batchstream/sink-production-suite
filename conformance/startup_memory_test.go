//go:build integration

package conformance_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStartupRejectsInsufficientMemoryBeforeDependencies(t *testing.T) {
	binary := os.Getenv("SINK_SERVER_BINARY")
	if binary == "" {
		t.Fatal("SINK_SERVER_BINARY is required")
	}
	for _, role := range []string{"gateway", "engine", "worker"} {
		t.Run(role, func(t *testing.T) {
			directory := t.TempDir()
			component := "mode: " + role + "\nmemory: {max_bytes: 128MiB}\n"
			if role == "gateway" {
				component += "forwarding: {routes: [{store: primary, target: '127.0.0.1:1', tls: {insecure: true}}]}\n"
			}
			if role == "worker" {
				component += "consumer: {group_id: startup-test}\n"
			}
			componentPath := filepath.Join(directory, "component.yaml")
			if err := os.WriteFile(componentPath, []byte(component), 0600); err != nil {
				t.Fatal(err)
			}
			arguments := []string{"--config", componentPath}
			if role != "gateway" {
				shared := "name: primary\nstorage: {driver: mongodb, mongodb: {uri: 'mongodb://127.0.0.1:1'}}\nkafka: {enabled: true, brokers: ['127.0.0.1:1'], topic: {name: startup-test}}\n"
				sharedPath := filepath.Join(directory, "store.yaml")
				if err := os.WriteFile(sharedPath, []byte(shared), 0600); err != nil {
					t.Fatal(err)
				}
				arguments = append(arguments, "--store-config", sharedPath)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			checkArgs := append([]string{"config", "check"}, arguments...)
			checked, err := exec.CommandContext(ctx, binary, checkArgs...).CombinedOutput()
			if err != nil {
				t.Fatalf("schema check should be independent of startup sizing: %s %v", checked, err)
			}
			output, err := exec.CommandContext(ctx, binary, arguments...).CombinedOutput()
			if err == nil || ctx.Err() != nil {
				t.Fatalf("process did not panic promptly: %s %v", output, err)
			}
			for _, part := range []string{"panic: insufficient startup memory", role, "available=134217728 bytes", "minimum working memory=", "runtime and drivers="} {
				if !strings.Contains(string(output), part) {
					t.Fatalf("missing startup diagnostic %q: %s", part, output)
				}
			}
		})
	}
}
