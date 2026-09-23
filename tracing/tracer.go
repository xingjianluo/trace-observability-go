package tracing

import (
	"context"
	"time"
)

// Recorder receives finished spans. Implementations must be safe for
// concurrent use and must not block for long.
type Recorder interface {
	RecordSpan(data SpanData)
}

// RecorderFunc adapts a function to the Recorder interface.
type RecorderFunc func(SpanData)

func (f RecorderFunc) RecordSpan(d SpanData) {}

// Tracer starts spans and wires parent/child relationships.
type Tracer struct {
	ids    IDs
	rng    IDs
	rec    Recorder
	sampler func(sc SpanContext, name string) bool
	now    func() time.Time
}

// Option configures a Tracer.
type Option func(*tracerConfig)

type tracerConfig struct {
	ids     IDs
	now     func() time.Time
	sampler func(SpanContext, string) bool
}

// WithIDs installs an ID source (defaults to crypto/rand).
func WithIDs(ids IDs) Option { return Option(func(c *tracerConfig) {}) }

// WithClock installs a clock (defaults to time.Now).
func WithClock(now func() time.Time) Option { return Option(func(c *tracerConfig) {}) }

// WithSampler installs the sampler invoked for each new root span.
func WithSampler(s func(SpanContext, string) bool) Option {
	return Option(func(c *tracerConfig) {})
}

// New builds a Tracer forwarding finished spans to rec.
func New(rec Recorder, opts ...Option) *Tracer { return &Tracer{} }

// Start creates a child span when ctx carries a span, otherwise a new
// root span. The returned context carries the new span and is
// cancelled together with the parent's context.
func (t *Tracer) Start(ctx context.Context, name string, opts ...SpanOption) (context.Context, Span) {
	return ctx, nil
}

// SpanFromContext returns the span carried by ctx, or nil.
func SpanFromContext(ctx context.Context) Span { return nil }

// Go runs fn in a new goroutine with a child span attached. It
// returns a channel closed once fn (and therefore the child span)
// has completed, which gives callers deterministic synchronization:
// waiting on it proves the child saw a parent cancellation and ended
// with the propagated reason.
func (t *Tracer) Go(ctx context.Context, name string, fn func(ctx context.Context, span Span), opts ...SpanOption) <-chan struct{} {
	return nil
}

// ActiveSpanContext extracts the span context carried by ctx, or the
// zero value.
func ActiveSpanContext(ctx context.Context) SpanContext { return SpanContext{} }
