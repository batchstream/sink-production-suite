package storage

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func FuzzValidateBSONDocument(f *testing.F) {
	f.Add([]byte{5, 0, 0, 0, 0})
	f.Add(bsonEnvelope(bson.TypeString, []byte{2, 0, 0, 0, 'a', 0}))
	f.Add(bsonEnvelope(bson.TypeEmbeddedDocument, []byte{5, 0, 0, 0, 0}))
	scope := bson.D{{Key: "x", Value: "value"}}
	value := bson.D{{Key: "scope", Value: bson.CodeWithScope{Code: "return x", Scope: scope}},
		{Key: "array", Value: bson.A{true, int32(1), "value", scope}},
		{Key: "binary", Value: bson.Binary{Subtype: 2, Data: []byte{1, 2}}}}
	payload, err := bson.Marshal(value)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(payload)
	f.Fuzz(func(t *testing.T, payload []byte) {
		if err := ValidateBSONDocument(payload); err != nil {
			return
		}
		var decoded bson.D
		if err := bson.Unmarshal(payload, &decoded); err != nil {
			t.Fatalf("validated BSON cannot be decoded: %v", err)
		}
	})
}
