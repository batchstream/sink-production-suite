package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/liran/sink/internal/queue"
)

type benchmarkRetryHandler struct {
	results []error
}

func (h *benchmarkRetryHandler) HandleBatch(_ context.Context, mutations []queue.Mutation) []error {
	return h.results[:len(mutations)]
}

func BenchmarkWorkerRetryBuffers(b *testing.B) {
	for _, count := range []int{1, 128, 500} {
		for _, retry := range []bool{false, true} {
			b.Run(fmt.Sprintf("operations=%d/retry=%t", count, retry), func(b *testing.B) {
				handler := &benchmarkRetryHandler{results: make([]error, count)}
				if retry {
					failure := retryHandlerError{retryable: true}
					for index := range handler.results {
						handler.results[index] = failure
					}
				}
				worker := &Worker{handler: handler, maxRetryAttempts: 3, retryBackoff: time.Nanosecond, maxRetryBackoff: time.Nanosecond}
				mutations := make([]queue.Mutation, count)
				ctx := b.Context()
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					results := worker.handleWithRetry(ctx, mutations)
					if len(results) != count {
						b.Fatal("invalid result count")
					}
				}
			})
		}
	}
}
