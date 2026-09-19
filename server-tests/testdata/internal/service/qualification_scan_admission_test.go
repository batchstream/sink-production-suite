package service

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/storage/search"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSearchScanReservesLookaheadAndDecodeBuffers(t *testing.T) {
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
	core.maxScanBytes = 12 << 20
	command := &sink.Command{Uri: "sink://primary", Method: "POST", Path: "/products/_search",
		ContentType: "application/json; charset=utf-8", Payload: []byte(`{"sort":["uid"]}`)}
	request := &sink.ScanRequest{Command: command}
	_, err = core.Scan(t.Context(), request)
	if status.Code(err) != codes.ResourceExhausted || calls.Load() != 0 {
		t.Fatalf("Scan exceeded its working-buffer quota: calls=%d, err=%v", calls.Load(), err)
	}
	core.maxScanBytes = 20 << 20
	if _, err := core.Scan(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || core.scanBytes != 0 || core.inFlightBytes != 0 {
		t.Fatal("Scan did not release its working-buffer reservation")
	}
}
