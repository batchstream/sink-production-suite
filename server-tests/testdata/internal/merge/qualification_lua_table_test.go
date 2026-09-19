package merge

import (
	"context"
	"fmt"
	"testing"

	"github.com/iceisfun/golua/stdlib"
	"github.com/iceisfun/golua/vm"
)

func BenchmarkLuaTableSort(b *testing.B) {
	for _, bounded := range []bool{false, true} {
		b.Run(fmt.Sprintf("bounded=%t", bounded), func(b *testing.B) {
			state := vm.New(vm.WithContext(b.Context()))
			defer state.Close(context.Background())
			stdlib.Open(state)
			if bounded {
				boundLuaAllocations(state, 16384)
			}
			function := state.GetGlobal("table").AsTable().(*vm.Table).GetString("sort")
			table := vm.NewTableWithSize(1000, 0)
			table.EnsureArraySize(1000)
			arguments := []vm.Value{vm.NewTable(table)}
			b.ReportAllocs()
			for b.Loop() {
				for index := 1; index <= 1000; index++ {
					table.RawSetArray(index, vm.NewInt(int64(1001-index)))
				}
				if _, err := state.ProtectedCall(function, arguments); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
