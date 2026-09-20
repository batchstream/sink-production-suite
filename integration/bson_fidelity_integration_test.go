//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Compare BSON types and bytes. Extended JSON can hide a literal becoming a scalar.
func TestMongoBSONFidelityAcrossMergeAndKafka(t *testing.T) {
	environment := newTestEnvironment(t)
	stores := 0
	for _, spec := range configuredBackendStores(t) {
		if !strings.HasPrefix(spec.name, "mongodb-") {
			continue
		}
		stores++
		t.Run(spec.name, func(t *testing.T) {
			dataset := fmt.Sprintf("sink-bson-fidelity-%d", time.Now().UnixNano())
			decimal, err := bson.ParseDecimal128("123.45")
			if err != nil {
				t.Fatal(err)
			}
			literal := bson.D{{Key: "$numberInt", Value: "1"}}
			fields := bson.D{
				{Key: "small", Value: int32(1)}, {Key: "wide", Value: int64(1)},
				{Key: "double", Value: float64(1)}, {Key: "large", Value: int64(math.MaxInt64)},
				{Key: "negative_zero", Value: math.Copysign(0, -1)},
				{Key: "timestamp", Value: bson.Timestamp{T: math.MaxUint32, I: 1}},
				{Key: "date", Value: bson.DateTime(math.MaxInt64)},
				{Key: "object_id", Value: bson.NewObjectID()},
				{Key: "binary", Value: bson.Binary{Subtype: 0x80, Data: []byte{0, 1, 255}}},
				{Key: "decimal", Value: decimal}, {Key: "regex", Value: bson.Regex{Pattern: "abc", Options: "im"}},
				{Key: "min", Value: bson.MinKey{}}, {Key: "max", Value: bson.MaxKey{}},
				{Key: "literal", Value: literal},
				{Key: "array", Value: bson.A{int32(1), int64(1), float64(1)}},
			}
			document, err := sink.NewDocument(fields, sink.DocumentEncodingBSON)
			if err != nil {
				t.Fatal(err)
			}
			program, err := sink.NewLuaProgram([]byte(`return function(current, incoming)
 incoming.processed = true
 return incoming
end`))
			if err != nil {
				t.Fatal(err)
			}
			modes := []sink.CompletionMode{sink.CompletionWaitUntilApplied}
			if spec.asynchronous {
				modes = append(modes, sink.CompletionReturnAfterAccepted)
			}
			for _, mode := range modes {
				t.Run(fmt.Sprint(mode), func(t *testing.T) {
					address := sinkAddressForStore(t, spec.name, dataset, fmt.Sprint(mode))
					seedFields := bson.D{{Key: "counter", Value: int32(0)}}
					seedDocument, err := sink.NewDocument(seedFields, sink.DocumentEncodingBSON)
					if err != nil {
						t.Fatal(err)
					}
					seed, err := sink.NewPut(address, seedDocument, sink.WriteCreate)
					if err != nil {
						t.Fatal(err)
					}
					writeRequest := sink.WriteRequest{
						CompletionMode: sink.CompletionWaitUntilApplied,
						Operations:     []sink.WriteOperation{seed},
					}
					results, err := environment.client.Write(t.Context(), writeRequest)
					if err != nil {
						t.Fatal(err)
					}
					assertWriteResults(t, results, sink.WriteApplied)
					opts := sink.MergeOptions{Incoming: document, Program: program}
					operation, err := sink.NewMerge(address, opts)
					if err != nil {
						t.Fatal(err)
					}
					if mode != sink.CompletionReturnAfterAccepted {
						operation = operation.WithReturnedDocument()
					}
					writeRequest2 := sink.WriteRequest{
						CompletionMode: mode,
						Operations:     []sink.WriteOperation{operation},
					}
					results, err = environment.client.Write(t.Context(), writeRequest2)
					if err != nil {
						t.Fatal(err)
					}
					if mode == sink.CompletionReturnAfterAccepted {
						assertWriteResults(t, results, sink.WriteAccepted)
					} else {
						assertWriteResults(t, results, sink.WriteApplied)
						assertBSONFields(t, document, results[0].Document)
					}
					ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
					defer cancel()
					for {
						readRequest := sink.ReadRequest{
							Addresses: []sink.Address{address},
						}
						read, err := environment.secondaryClient.Read(ctx, readRequest)
						if err != nil || len(read) != 1 || read[0].Status != sink.ReadFound {
							t.Fatalf("persisted BSON read: %+v, %v", read, err)
						}
						raw := bson.Raw(read[0].Document.Payload())
						if raw.Lookup("large").Type != 0 {
							assertBSONFields(t, document, read[0].Document)
							var value struct {
								Processed bool `bson:"processed"`
							}
							if err := read[0].Document.Decode(&value); err != nil || !value.Processed {
								t.Fatalf("merge execution: %+v, %v", value, err)
							}
							break
						}
						select {
						case <-ctx.Done():
							t.Fatal("accepted BSON merge was not persisted")
						case <-time.After(20 * time.Millisecond):
						}
					}
				})
			}
		})
	}
	if stores != 2 {
		t.Fatalf("BSON qualification requires both MongoDB stores, got %d", stores)
	}
}

func assertBSONFields(t *testing.T, before, after sink.Document) {
	t.Helper()
	if after.Encoding() != sink.DocumentEncodingBSON {
		t.Fatal("document lost BSON encoding")
	}
	elements, err := bson.Raw(before.Payload()).Elements()
	if err != nil {
		t.Fatal(err)
	}
	raw := bson.Raw(after.Payload())
	for _, field := range elements {
		got := raw.Lookup(field.Key())
		if !field.Value().Equal(got) {
			t.Errorf("BSON field %s changed type or value: %v -> %v", field.Key(), field.Value(), got)
		}
	}
}
