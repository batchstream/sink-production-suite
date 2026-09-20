//go:build integration

package mongodb_test

import (
	"testing"

	"github.com/batchstream/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCountFindProcessesSmallPages(t *testing.T) {
	fixture := newIntegrationFixture(t)
	documents := make([]any, 10000)
	for i := range documents {
		doc := bson.D{{Key: "number", Value: i}}
		documents[i] = doc
	}
	if _, err := fixture.collection.InsertMany(t.Context(), documents); err != nil {
		t.Fatal(err)
	}
	filter := bson.D{{Key: "$where", Value: "this.number >= 0"}}
	find := bson.D{{Key: "find", Value: "documents"}, {Key: "filter", Value: filter}}
	native := mongoNativeRequest(t, fixture.database, find)
	request := storage.CountRequest{Request: native}
	control, err := fixture.store.Count(t.Context(), request)
	if err != nil || control.Count != uint64(len(documents)) {
		t.Fatalf("control: count=%d err=%v", control.Count, err)
	}
	request.Request.MaxBytes = 256
	result, err := fixture.store.Count(t.Context(), request)
	if err != nil || result.Count != control.Count {
		t.Fatalf("small-page count lost documents: count=%d want=%d err=%v", result.Count, control.Count, err)
	}
}
