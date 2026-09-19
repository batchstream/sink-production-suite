package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/queue"
	"github.com/twmb/franz-go/pkg/kfake"
)

func BenchmarkPublisherBatch(b *testing.B) {
	for _, closed := range []bool{false, true} {
		for _, count := range []int{1, 128, 1024} {
			b.Run(fmt.Sprintf("closed=%t/operations=%d", closed, count), func(b *testing.B) {
				const topic = "publisher-benchmark"
				cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(cluster.Close)
				opts := PublisherOptions{Brokers: cluster.ListenAddrs(), Topic: topic}
				publisher, err := NewPublisher(opts)
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(publisher.Close)
				want := queue.PublishStatusAccepted
				if closed {
					publisher.Close()
					want = queue.PublishStatusFailed
				}
				request := queue.PublishRequest{Mutations: make([]queue.Mutation, count)}
				for index := range request.Mutations {
					request.Mutations[index] = reliabilityPut(sink.WriteMode_WRITE_MODE_UPSERT, `{"value":1}`)
				}
				ctx, cancel := context.WithTimeout(b.Context(), time.Minute)
				defer cancel()
				if _, err := publisher.Publish(ctx, request); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				for b.Loop() {
					response, err := publisher.Publish(ctx, request)
					if err != nil {
						b.Fatal(err)
					}
					for index, result := range response.Results {
						if result.Status != want {
							b.Fatalf("result %d: %+v", index, result)
						}
					}
				}
			})
		}
	}
}
