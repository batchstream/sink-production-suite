package search

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/liran/sink/internal/storage"
)

func BenchmarkSearchReadDocument(b *testing.B) {
	for _, size := range []int{64 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("bytes-%d", size), func(b *testing.B) {
			document := `{"text":"` + strings.Repeat("x", size) + `"}`
			payload := []byte(`{"docs":[{"_index":"legacy-records","_id":"one","found":true,"_seq_no":0,"_primary_term":1,"_source":` + document + `}]}`)
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(payload) })
			backend := httptest.NewServer(handler)
			b.Cleanup(backend.Close)
			opts := Options{Driver: DriverOpenSearch, Store: "primary", Endpoints: []string{backend.URL}}
			store, err := New(opts)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(store.Close)
			operation := storage.ReadOperation{Address: testAddress("one")}
			request := storage.ReadRequest{Operations: []storage.ReadOperation{operation}}
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				response, err := store.Read(b.Context(), request)
				if err != nil || response.Results[0].Status != storage.ReadStatusFound {
					b.Fatalf("read: %+v %v", response, err)
				}
			}
		})
	}
}
