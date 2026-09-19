package mongodb

import (
	"fmt"
	"strings"
	"testing"

	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/testuri"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func BenchmarkMongoReadDocument(b *testing.B) {
	for _, size := range []int{64 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("bytes-%d", size), func(b *testing.B) {
			store, deployment := newReadWireFixture(b)
			document := bson.D{{Key: "_id", Value: "one"}, {Key: "text", Value: strings.Repeat("x", size)}}
			reply := readWireReply(document)
			key := storage.Key{Type: "string", Data: []byte("one")}
			address := testuri.Address("primary", []string{"test", "documents"}, key)
			operation := storage.ReadOperation{Address: address}
			request := storage.ReadRequest{Operations: []storage.ReadOperation{operation}}
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for b.Loop() {
				deployment.AddResponses(reply)
				response, err := store.Read(b.Context(), request)
				if err != nil || response.Results[0].Status != storage.ReadStatusFound {
					b.Fatalf("read: %+v %v", response, err)
				}
			}
		})
	}
}
