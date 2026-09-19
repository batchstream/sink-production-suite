package search

import (
	"net/http"
	"testing"
)

func BenchmarkSearchConnectionReuse(b *testing.B) {
	for _, name := range []string{"http-default", "store-pool"} {
		b.Run(name, func(b *testing.B) {
			var client *http.Client
			if name == "http-default" {
				transport := http.DefaultTransport.(*http.Transport).Clone()
				b.Cleanup(transport.CloseIdleConnections)
				client = &http.Client{Transport: transport, Timeout: defaultRequestTimeout}
			}
			store, connections, arrived, proceed := connectionBurstStore(b, client)
			pingBurst(b, store, arrived, proceed)
			initial := connections.Load()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				pingBurst(b, store, arrived, proceed)
			}
			b.ReportMetric(float64(connections.Load()-initial)/float64(b.N), "connections/burst")
		})
	}
}
