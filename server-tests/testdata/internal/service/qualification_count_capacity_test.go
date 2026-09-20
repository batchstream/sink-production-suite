package service_test

import (
	"testing"

	sink "github.com/batchstream/sink/gen/sink"
	"github.com/batchstream/sink/internal/storage"
)

func TestCountRetainsSmallerConfiguredResponseBudget(t *testing.T) {
	client, backend := nativeRPCFixture(t, false)
	backend.counts = make(chan storage.CountRequest, 1)
	request := &sink.CountRequest{Command: nativeSearchRequest().Command}
	if _, err := client.Count(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if request := <-backend.counts; request.Request.MaxBytes != 4096 {
		t.Fatalf("Count expanded the configured response budget: %d", request.Request.MaxBytes)
	}
}
