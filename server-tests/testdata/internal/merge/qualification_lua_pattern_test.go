package merge

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/iceisfun/golua/stdlib"
	"github.com/iceisfun/golua/vm"
)

func BenchmarkLuaPatternMatching(b *testing.B) {
	for _, tc := range []struct {
		name    string
		text    string
		pattern string
	}{
		{name: "identifier", text: "product-1234-variant", pattern: "(%d+)"},
		{name: "unmatched", text: strings.Repeat("x", 4096), pattern: "z+"},
		{name: "balanced", text: "(" + strings.Repeat("a", 4096) + ")", pattern: "%b()"},
		{name: "class", text: strings.Repeat("ab12", 1024), pattern: "^[%w]+$"},
		{name: "backreference", text: strings.Repeat("a", 4096), pattern: "^(.*)%1$"},
	} {
		for _, bounded := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/bounded=%t", tc.name, bounded), func(b *testing.B) {
				state := vm.New(vm.WithContext(b.Context()))
				defer state.Close(context.Background())
				stdlib.Open(state)
				if bounded {
					boundLuaAllocations(state, 16384)
				}
				function := state.GetGlobal("string").AsTable().(*vm.Table).GetString("find")
				arguments := []vm.Value{vm.NewString(tc.text), vm.NewString(tc.pattern)}
				b.ReportAllocs()
				for b.Loop() {
					if _, err := state.ProtectedCall(function, arguments); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
