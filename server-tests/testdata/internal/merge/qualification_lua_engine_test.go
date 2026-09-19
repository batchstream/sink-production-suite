package merge_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/liran/sink/internal/merge"
)

func BenchmarkLuaMergeProduct5KB(b *testing.B) {
	source := productMergeSource(b)
	options := merge.LuaOptions{}
	merger := compileTestProgram(b, source, options)
	currentValue := map[string]any{
		"uid":         "product-1",
		"title":       "current",
		"description": strings.Repeat("current product data ", 250),
		"uids":        []string{"legacy-product-1"},
	}
	incomingValue := map[string]any{
		"uid":         "product-1",
		"title":       "incoming",
		"description": strings.Repeat("incoming product data ", 240),
		"available":   true,
	}
	currentJSON, err := json.Marshal(currentValue)
	if err != nil {
		b.Fatalf("encode current product: %v", err)
	}
	incomingJSON, err := json.Marshal(incomingValue)
	if err != nil {
		b.Fatalf("encode incoming product: %v", err)
	}
	current := jsonDocument(string(currentJSON))
	incoming := jsonDocument(string(incomingJSON))
	request := merge.Request{Current: &current, Incoming: incoming}
	b.SetBytes(int64(len(currentJSON) + len(incomingJSON)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := merger.Merge(context.Background(), request); err != nil {
			b.Fatalf("Merge() error = %v", err)
		}
	}
}
