package merge

import (
	"context"
	"strconv"
	"testing"

	"github.com/iceisfun/golua/vm"
	"github.com/liran/sink/internal/storage"
)

func BenchmarkLuaObjectConversion(b *testing.B) {
	for _, fields := range []int{1000, 10000, 30000} {
		b.Run(strconv.Itoa(fields), func(b *testing.B) {
			state := vm.New()
			defer state.Close(context.Background())
			bridge := newLuaJSONBridge(state)
			table := bridge.newObject(fields)
			for index := range fields {
				table.SetString("field"+strconv.Itoa(index), vm.NewInt(int64(index)))
			}
			value := vm.NewTable(table)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := validateResultBudget(b.Context(), value, defaultMaxResultBytes); err != nil {
					b.Fatal(err)
				}
				if _, err := bridge.encodeJSONObject(value, storage.DocumentEncodingJSON); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
