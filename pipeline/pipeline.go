// Package pipeline wires tracing, logging, sampling and local export
// into one object with a deterministic, lossless-on-graceful-shutdown
// lifecycle.
package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/xingjianluo/trace-observability-go/export"
	"github.com/xingjianluo/trace-observability-go/logx"
	"github.com/xingjianluo/trace-observability-go/sampler"
	"github.com/xingjianluo/trace-observability-go/tracing"
)

// Config configures a Pipeline.
type Config struct {
	// QueueCapacity bounds the in-memory export queue (default 256).
	QueueCapacity int
	// Policy chooses backpressure behaviour on overflow:
	// PolicyBlock (default) blocks producers; PolicyDropNewest drops
	// the incoming item and counts it.
	Policy export.Policy
	// Batch controls batching/flush/retry behaviour.
	Batch export.BatcherConfig
	// Log configures level filtering and fixed fields.
	Log logx.Config
	// Sampler decides root-span sampling; nil keeps every trace.
	Sampler sampler.Sampler
	// Redactor rewrites sensitive values before logging/export;
	// nil disables redaction.
	Redactor *logx.Redactor
	// IDs/Clock make runs deterministic in tests.
	IDs   tracing.IDs
	Clock func() time.Time
}

// Pipeline is the assembled runtime.
type Pipeline struct {
	cfg      Config
	queue    *export.Queue
	exporter export.Exporter
	batcher  *export.Batcher
	tracer   *tracing.Tracer
	logger   *logx.Logger

	startOnce sync.Once
	stopOnce  sync.Once
	runDone   chan struct{}
	runErr    error
	bgCancel  context.CancelFunc
}

// New assembles the pipeline. Call Start to launch background work
// and Shutdown for a lossless graceful stop.
func New(ex export.Exporter, cfg Config) *Pipeline {
	if cfg.QueueCapacity <= 0 {
		cfg.QueueCapacity = 256
	}
	if cfg.IDs == nil {
		cfg.IDs = tracing.NewRandomIDs()
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	queue := export.NewQueue(cfg.QueueCapacity, cfg.Policy)

	rec := spanRecorder{
		enqueue: func(e export.Envelope) { _ = queue.Enqueue(context.Background(), e) },
		now:     cfg.Clock,
	}
	var trSampler tracing.Sampler = tracing.SamplerFunc(
		func(t tracing.TraceID, name string) (bool, string) { return true, "always_on" })
	if cfg.Sampler != nil {
		trSampler = samplerAdapter{s: cfg.Sampler}
	}
	tr := tracing.New(rec, tracing.WithIDs(cfg.IDs),
		tracing.WithClock(cfg.Clock), tracing.WithSampler(trSampler))

	sink := logSink{
		enqueue: func(e export.Envelope) { _ = queue.Enqueue(context.Background(), e) },
		now:     cfg.Clock,
	}
	lg := logx.New(sink, cfg.Redactor, cfg.Log)

	p := &Pipeline{
		cfg:      cfg,
		queue:    queue,
		exporter: ex,
		batcher:  export.NewBatcher(queue, ex, cfg.Batch),
		tracer:   tr,
		logger:   lg,
		runDone:  make(chan struct{}),
	}
	// Start the drain worker immediately so data can flow before the
	// caller remembers to call Start; Start itself stays idempotent.
	p.Start(context.Background())
	return p
}

// Tracer returns the configured tracer.
func (p *Pipeline) Tracer() *tracing.Tracer { return p.tracer }

// Logger returns the configured logger.
func (p *Pipeline) Logger() *logx.Logger { return p.logger }

// Queue exposes the queue for inspection/tests.
func (p *Pipeline) Queue() *export.Queue { return p.queue }

// Batcher exposes the batcher for stats inspection.
func (p *Pipeline) Batcher() *export.Batcher { return p.batcher }

// Start launches the background batcher. It is idempotent.
func (p *Pipeline) Start(ctx context.Context) {
	p.startOnce.Do(func() {
		bg, cancel := context.WithCancel(context.Background())
		p.bgCancel = cancel
		go func() {
			defer close(p.runDone)
			p.runErr = p.batcher.Run(bg)
		}()
	})
}

// Shutdown stops accepting new data and drains accepted items:
//
//  1. the queue is closed so producers get ErrQueueClosed instead of
//     silently enqueueing after shutdown began;
//  2. the batcher drains the buffer and flushes final batches;
//  3. Shutdown blocks until the worker has returned or ctx expires.
//
// Data already accepted before Close is exported; only an expired ctx
// can leave items unflushed, in which case the error is reported.
func (p *Pipeline) Shutdown(ctx context.Context) error {
	p.stopOnce.Do(func() {
		p.queue.Close()
	})
	select {
	case <-p.runDone:
		// Worker saw the closed queue and drained normally.
		if p.bgCancel != nil {
			p.bgCancel()
		}
		return p.runErr
	case <-ctx.Done():
		if p.bgCancel != nil {
			p.bgCancel()
		}
		<-p.runDone
		return ctx.Err()
	}
}

// samplerAdapter bridges sampler.Sampler to tracing.Sampler.
type samplerAdapter struct{ s sampler.Sampler }

func (a samplerAdapter) Decide(t tracing.TraceID, name string) (bool, string) {
	d := a.s.Decide(t, name)
	return d.Sampled, d.Reason
}
