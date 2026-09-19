package service

import (
	"fmt"
	"testing"
)

func BenchmarkBlockedQueueSelection(b *testing.B) {
	for _, count := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			key := recordIdentity("hot-record")
			active := map[recordIdentity]int{key: 1}
			pending := make([]*batchCall[int, int], count)
			for index := range pending {
				call := &batchCall[int, int]{records: []recordIdentity{key}, operationCount: 1, encodedBytes: 128}
				pending[index] = call
			}
			batcher := &requestBatcher[int, int]{maxOperations: 1000, maxBytes: 16 << 20}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				selected, remaining, _ := batcher.selectReady(pending, active)
				if len(selected) != 0 || len(remaining) != count {
					b.Fatal("invalid selection")
				}
			}
		})
	}
}
