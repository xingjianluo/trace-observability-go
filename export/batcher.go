package export

import (
	"context"
	"time"
)

// Exporter persists/forwards one batch. Implementations must be safe
// for concurrent calls but the batcher calls one at a time.
type Exporter interface {
	Export(ctx context.Context, batch []Envelope) error
}

// ExporterFunc adapts a function to Exporter.
type ExporterFunc func(context.Context, []Envelope) error

func (f ExporterFunc) Export(ctx context.Context, b []Envelope) error { return nil }

// BatcherConfig controls batch boundaries.
type BatcherConfig struct {
	MaxBatchSize int
	FlushInterval time.Duration
	// ExportTimeout bounds a single Export call.
	ExportTimeout time.Duration
	// MaxRetries retries a failing export with linear backoff.
	MaxRetries int
}

// Batcher drains a Queue into batches and hands them to an Exporter.
type Batcher struct {
	queue   *Queue
	exporter Exporter
	cfg     BatcherConfig
}

// NewBatcher wires a batcher.
func NewBatcher(q *Queue, ex Exporter, cfg BatcherConfig) *Batcher {
	return &Batcher{}
}

// Run consumes the queue until it is drained and closed, flushing
// periodically. It returns when all accepted items have been
// exported (or ctx is cancelled).
func (b *Batcher) Run(ctx context.Context) error { return nil }

// Stats returns export-side counters.
func (b *Batcher) Stats() Snapshot { return Snapshot{} }
