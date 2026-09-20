package service

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	sink "github.com/batchstream/sink/gen/sink"
	"github.com/batchstream/sink/internal/storage/search"
)

func TestSearchScanUsesProcessAdmission(t *testing.T) {
	var calls atomic.Int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{"timed_out":false,"_shards":{"total":1,"successful":1,"failed":0},"hits":{"hits":[]}}`))
	})
	backend := httptest.NewServer(handler)
	defer backend.Close()
	opts := search.Options{Driver: search.DriverOpenSearch, Store: "primary", Endpoints: []string{backend.URL}}
	store, err := search.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	core := completionServer(t, store).server
	command := &sink.Command{Uri: "sink://primary", Method: "POST", Path: "/products/_search",
		ContentType: "application/json; charset=utf-8", Payload: []byte(`{"sort":["uid"]}`)}
	request := &sink.ScanRequest{Command: command}
	if _, err := core.Scan(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("Scan did not execute exactly once")
	}
}
