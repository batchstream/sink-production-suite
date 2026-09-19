//go:build integration

package conformance_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
)

func TestScanProjectionSurvivesConcurrentCount(t *testing.T) {
	for _, store := range searchBackends(t) {
		t.Run(store.driver, func(t *testing.T) {
			index := indexFor(t, store, "100ms")
			proxy := proxyBackend(t, store)
			opts := serverOptions{backend: proxy.backend, readBytes: 2048}
			server := startCandidate(t, opts)
			padding := strings.Repeat("x", 4096)
			for i := range 3 {
				address := addressFor(t, index, fmt.Sprint(i))
				operation := put(t, address, fmt.Sprintf(`{"counter":%d,"padding":%q}`, i, padding), sink.WriteUpsert)
				applied(t, writeAsync(t.Context(), server.client, sink.CompletionWaitUntilVisible, operation), 1)
			}
			projection := &sink.Projection{Fields: []string{"counter"}}
			scan := sink.ScanRequest{Command: nativeSearch(index), BatchSize: 1, Projection: projection}
			first, err := server.client.Scan(t.Context(), scan)
			if err != nil || len(first.Documents) != 1 || len(first.NextCursor) == 0 {
				t.Fatalf("projected first page: %+v %v", first, err)
			}
			scan.Cursor = first.NextCursor
			checkpoint := string(scan.Cursor)
			gate := proxy.hold("/"+index+"/_search", "match_all", 1)
			t.Cleanup(gate.open)
			countRequest := sink.CountRequest{Command: nativeSearch(index)}
			busy := countAsync(t.Context(), server.client, countRequest)
			gate.wait(t)
			type scanOutcome struct {
				page sink.ScanResponse
				err  error
			}
			finished := make(chan scanOutcome, 1)
			go func() {
				page, err := server.client.Scan(t.Context(), scan)
				outcome := scanOutcome{page: page, err: err}
				finished <- outcome
			}()
			var second sink.ScanResponse
			select {
			case result := <-finished:
				if result.err != nil || len(result.page.Documents) != 1 || len(result.page.NextCursor) == 0 {
					t.Fatalf("projected page failed alongside held Count: %+v", result)
				}
				second = result.page
			case <-time.After(5 * time.Second):
				t.Fatal("Scan was blocked by an independent Count")
			}
			gate.open()
			select {
			case result := <-busy:
				if result.err != nil || result.response.Count != 3 || result.response.Estimated {
					t.Fatalf("held Count failed: %+v", result)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("held Count did not release capacity")
			}
			if string(scan.Cursor) != checkpoint {
				t.Fatal("Scan mutated the caller's last processed checkpoint")
			}
			scan.Cursor = second.NextCursor
			third, err := server.client.Scan(t.Context(), scan)
			if err != nil || len(third.Documents) != 1 || len(third.NextCursor) != 0 {
				t.Fatalf("projected final page: %+v %v", third, err)
			}
			for i, page := range []sink.ScanResponse{first, second, third} {
				var document struct {
					Source map[string]any `json:"_source"`
				}
				if err := page.Documents[0].Decode(&document); err != nil || len(document.Source) != 1 || document.Source["counter"] != float64(i) {
					t.Fatalf("projection lost page order/fields: page=%d document=%v error=%v", i, document, err)
				}
			}
			proxy.mu.Lock()
			searches := 0
			for _, request := range proxy.requests {
				if request.Phase == "" && strings.Split(request.Path, "?")[0] == "/"+index+"/_search" {
					searches++
				}
			}
			proxy.mu.Unlock()
			if searches != 4 {
				t.Fatalf("Scan replayed or skipped a page: searches=%d", searches)
			}
			server.waitIdle(t)
		})
	}
}
