// Package pipeline wires tracing, logging, sampling and local export
// into one object with a deterministic, lossless-on-graceful-shutdown
// lifecycle.
package pipeline

import (
	"context"
	"time"

	"github.com/xingjianluo/trace-observability-go/export"
	"github.com/xingjianluo/trace-observability-go/logx"
	"github.com/xingjianluo/trace-observability-go/sampler"
	"github.com/xingjianluo/trace-observability-go/tracing"
)

// Config configures a Pipeline.
type Config struct {
	QueueCapacity int
	Policy        export.Policy
	Batch         export.BatcherConfig
	Log           logx.Config
	// Sampler decides root-span sampling; nil means keep everything.
	Sampler sampler.Sampler
	// IDs/Clock make runs deterministic in tests.
	IDs   tracing.IDs
	Clock func() time.Time
}

// Pipeline is the assembled runtime.
type Pipeline struct {
	tracer *tracing.Tracer
	logger *logx.Logger
	queue  *export.Queue
}

// New assembles the pipeline and starts the batcher writing to the
// given exporter.
func New(ex export.Exporter, cfg Config) *Pipeline { return &Pipeline{} }

// Tracer returns the configured tracer.
func (p *Pipeline) Tracer() *tracing.Tracer { return nil }

// Logger returns the configured logger.
func (p *Pipeline) Logger() *logx.Logger { return nil }

// Queue exposes the queue for inspection/tests.
func (p *Pipeline) Queue() *export.Queue { return nil }

// Start launches the background batcher.
func (p *Pipeline) Start(ctx context.Context) {}

// Shutdown stops accepting new data, drains accepted items through
// the exporter and waits for the batcher to finish. Data accepted
// before Shutdown is never lost. The provided context bounds the
// drain wait.
func (p *Pipeline) Shutdown(ctx context.Context) error { return nil }
