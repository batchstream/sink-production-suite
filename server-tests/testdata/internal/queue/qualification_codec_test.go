package queue_test

import (
	"bytes"
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"google.golang.org/protobuf/proto"
)

func BenchmarkMutationPayloadMarshal(b *testing.B) {
	address := testQueueAddress("benchmark")
	document := &sink.Document{
		Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON,
		Payload:  bytes.Repeat([]byte("x"), 4096),
	}
	put := &sink.PutOperation{Document: document, Mode: sink.WriteMode_WRITE_MODE_UPSERT}
	operation := &sink.WriteOperation{Address: address, Action: &sink.WriteOperation_Put{Put: put}}

	b.Run("vtproto", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			encoded, err := operation.MarshalVT()
			if err != nil {
				b.Fatal(err)
			}
			_ = encoded
		}
	})
	b.Run("protobuf", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			encoded, err := proto.Marshal(operation)
			if err != nil {
				b.Fatal(err)
			}
			_ = encoded
		}
	})
}

func BenchmarkMutationPayloadUnmarshal(b *testing.B) {
	address := testQueueAddress("benchmark")
	document := &sink.Document{
		Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON,
		Payload:  bytes.Repeat([]byte("x"), 4096),
	}
	put := &sink.PutOperation{Document: document, Mode: sink.WriteMode_WRITE_MODE_UPSERT}
	operation := &sink.WriteOperation{Address: address, Action: &sink.WriteOperation_Put{Put: put}}
	encoded, err := operation.MarshalVT()
	if err != nil {
		b.Fatal(err)
	}

	b.Run("vtproto", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			decoded := &sink.WriteOperation{}
			if err := decoded.UnmarshalVT(encoded); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("protobuf", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			decoded := &sink.WriteOperation{}
			if err := proto.Unmarshal(encoded, decoded); err != nil {
				b.Fatal(err)
			}
		}
	})
}
