package logging

import (
	"context"
	"log/slog"
	"testing"
)

func BenchmarkDisabledDebug(b *testing.B) {
	h := &handler{level: slog.LevelWarn, minimum: slog.LevelWarn}
	logger := slog.New(h)
	b.ReportAllocs()
	for b.Loop() {
		logger.DebugContext(context.Background(), "batch", "operations", 100)
	}
}
