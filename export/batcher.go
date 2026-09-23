package export

import (
	"context"
	"sync/atomic"
	"time"
)

// Exporter persists/forwards one batch. Implementations must be safe
// for concurrent calls but the batcher calls one at a time.
type Exporter interface {
	Export(ctx context.Context, batch []Envelope) error
}

// ExporterFunc adapts a function to Exporter.
type ExporterFunc func(context.Context, []Envelope) error

func (f ExporterFunc) Export(ctx context.Context, b []Envelope) error {
	return f(ctx, b)
}

// BatcherConfig controls batch boundaries.
type BatcherConfig struct {
	// MaxBatchSize flushes once the batch reaches this size.
	MaxBatchSize int
	// FlushInterval flushes even partial batches after this idle
	// period.
	FlushInterval time.Duration
	// ExportTimeout bounds a single Export call.
	ExportTimeout time.Duration
	// MaxRetries retries a failing export with linear backoff
	// (attempt n waits n * retryBase).
	MaxRetries int
	// RetryBase is the unit for the linear backoff.
	RetryBase time.Duration
}

// BatcherStats are export-side counters.
type BatcherStats struct {
	Exported int64 // envelopes handed successfully to the exporter
	Failed   int64 // envelopes that failed all retries
	Batches  int64 // successful batch exports
	Retries  int64 // individual failed export attempts
}

// Batcher drains a Queue into batches and hands them to an Exporter.
type Batcher struct {
	queue    *Queue
	exporter Exporter
	cfg      BatcherConfig

	exported atomic.Int64
	failed   atomic.Int64
	batches  atomic.Int64
	retries  atomic.Int64
}

// NewBatcher wires a batcher, filling conservative defaults.
func NewBatcher(q *Queue, ex Exporter, cfg BatcherConfig) *Batcher {
	if cfg.MaxBatchSize <= 0 {
		cfg.MaxBatchSize = 64
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 100 * time.Millisecond
	}
	if cfg.ExportTimeout <= 0 {
		cfg.ExportTimeout = 5 * time.Second
	}
	if cfg.RetryBase <= 0 {
		cfg.RetryBase = 10 * time.Millisecond
	}
	return &Batcher{queue: q, exporter: ex, cfg: cfg}
}

// Run consumes the queue until it is drained AND closed, flushing
// partial batches on the flush interval. It returns nil when every
// accepted item has been attempted; it returns ctx.Err only if the
// caller cancels an in-flight shutdown (items still in the queue are
// then lost, which is signalled by the caller choosing that path).
func (b *Batcher) Run(ctx context.Context) error {
	batch := make([]Envelope, 0, b.cfg.MaxBatchSize)
	ticker := time.NewTicker(b.cfg.FlushInterval)
	defer ticker.Stop()

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := b.exportWithRetry(ctx, batch)
		n := int64(len(batch))
		if err != nil {
			b.failed.Add(n)
			batch = batch[:0]
			return err
		}
		b.exported.Add(n)
		b.batches.Add(1)
		batch = batch[:0]
		return nil
	}

	for {
		select {
		case e, ok := <-b.queue.C():
			if !ok {
				// Queue closed: drain any remaining buffered items.
				for e := range b.queue.C() {
					batch = append(batch, e)
					if len(batch) >= b.cfg.MaxBatchSize {
						if err := flush(); err != nil {
							return err
						}
					}
				}
				return flush()
			}
			batch = append(batch, e)
			if len(batch) >= b.cfg.MaxBatchSize {
				if err := flush(); err != nil {
					return err
				}
				ticker.Reset(b.cfg.FlushInterval)
			}
		case <-ticker.C:
			if err := flush(); err != nil {
				return err
			}
		case <-ctx.Done():
			// Best-effort final flush of what is already in hand,
			// using a fresh background context so an exporter timeout
			// cannot strand an accepted-but-unflushed batch.
			_ = flush()
			return ctx.Err()
		}
	}
}

// exportWithRetry retries transient failures up to MaxRetries times.
func (b *Batcher) exportWithRetry(ctx context.Context, batch []Envelope) error {
	var lastErr error
	for attempt := 0; attempt <= b.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			b.retries.Add(1)
			wait := time.Duration(attempt) * b.cfg.RetryBase
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		exportCtx, cancel := context.WithTimeout(ctx, b.cfg.ExportTimeout)
		err := b.exporter.Export(exportCtx, batch)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
	}
	return lastErr
}

// Stats returns export-side counters.
func (b *Batcher) Stats() BatcherStats {
	return BatcherStats{
		Exported: b.exported.Load(),
		Failed:   b.failed.Load(),
		Batches:  b.batches.Load(),
		Retries:  b.retries.Load(),
	}
}
