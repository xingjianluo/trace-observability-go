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

func (f RecorderFunc) RecordSpan(d SpanData) { f(d) }

// Sampler decides sampling for a freshly created ROOT span. Child
// spans inherit the parent's decision, which keeps a trace
// all-or-nothing and makes decisions reproducible.
type Sampler interface {
	Decide(traceID TraceID, name string) (sampled bool, reason string)
}

// SamplerFunc adapts a function to Sampler.
type SamplerFunc func(TraceID, string) (bool, string)

func (f SamplerFunc) Decide(t TraceID, n string) (bool, string) { return f(t, n) }

// Tracer starts spans and wires parent/child relationships.
type Tracer struct {
	ids     IDs
	rec     Recorder
	sampler Sampler
	now     func() time.Time
}

// Option configures a Tracer.
type Option func(*tracerConfig)

type tracerConfig struct {
	ids     IDs
	now     func() time.Time
	sampler Sampler
}

// WithIDs installs an ID source (defaults to crypto/rand).
func WithIDs(ids IDs) Option {
	return func(c *tracerConfig) { c.ids = ids }
}

// WithClock installs a clock (defaults to time.Now).
func WithClock(now func() time.Time) Option {
	return func(c *tracerConfig) { c.now = now }
}

// WithSampler installs the sampler invoked for each new root span.
func WithSampler(s Sampler) Option {
	return func(c *tracerConfig) { c.sampler = s }
}

// New builds a Tracer forwarding finished spans to rec.
func New(rec Recorder, opts ...Option) *Tracer {
	cfg := tracerConfig{
		ids:     NewRandomIDs(),
		now:     time.Now,
		sampler: SamplerFunc(func(TraceID, string) (bool, string) { return true, "always_on" }),
	}
	for _, o := range opts {
		o(&cfg)
	}
	return &Tracer{
		ids:     cfg.ids,
		rec:     rec,
		sampler: cfg.sampler,
		now:     cfg.now,
	}
}

// Start creates a child span when ctx carries a span, otherwise a new
// root span. The returned context carries the new span. The new span
// observes cancellation of ctx: a root records "context_cancelled",
// a child records "parent_cancelled".
func (t *Tracer) Start(ctx context.Context, name string, opts ...SpanOption) (context.Context, Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	cfg := spanConfig{kind: KindInternal, start: t.now()}
	for _, o := range opts {
		o(&cfg)
	}

	var (
		sc     SpanContext
		parent SpanID
		depth  int
		isRoot bool
	)
	if p := spanFromContext(ctx); p != nil {
		psc := p.SpanContext()
		sc = SpanContext{
			TraceID: psc.TraceID,
			SpanID:  t.ids.NewSpanID(),
			Sampled: psc.Sampled, // decision inherited, never re-rolled
		}
		parent = psc.SpanID
		depth = spanDepth(ctx) + 1
	} else {
		isRoot = true
		tid := t.ids.NewTraceID()
		sampled, _ := t.sampler.Decide(tid, name)
		sc = SpanContext{TraceID: tid, SpanID: t.ids.NewSpanID(), Sampled: sampled}
	}

	if !sc.Sampled {
		// Unsampled trace: a no-op span still propagates identity so
		// that descendants keep the same decision, but nothing is
		// recorded and no resources are spent.
		ns := &noopSpan{sc: sc, parent: parent, depth: depth}
		ns.ctx = withSpan(ctx, ns)
		return ns.ctx, ns
	}

	s := newSpan(sc, parent, name, cfg.kind, depth, cfg.start, ctx, t.now,
		func(d SpanData) {
			if t.rec != nil {
				t.rec.RecordSpan(d)
			}
		})
	go s.watchCancellation(ctx, isRoot)
	return s.Context(), s
}

// Go runs fn in a new goroutine with a child span attached and
// returns a channel closed once fn (and therefore the child span) has
// fully ended. Waiting on the returned channel is the deterministic
// join point: after it fires the child span has observed any parent
// cancellation and its end reason is recorded.
//
// The child span is ended exactly once, by Go itself, never by fn.
func (t *Tracer) Go(ctx context.Context, name string,
	fn func(ctx context.Context, span Span), opts ...SpanOption) <-chan struct{} {
	childCtx, sp := t.Start(ctx, name, opts...)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer sp.End()
		fn(childCtx, sp)
	}()
	return done
}

// SpanFromContext returns the span carried by ctx, or nil.
func SpanFromContext(ctx context.Context) Span { return spanFromContext(ctx) }

// ActiveSpanContext extracts the span context carried by ctx, or the
// zero value.
func ActiveSpanContext(ctx context.Context) SpanContext {
	if s := spanFromContext(ctx); s != nil {
		return s.SpanContext()
	}
	return SpanContext{}
}

// spanDepth returns the depth of the span on ctx, or -1 if there is
// none (so a first child lands at depth 0).
func spanDepth(ctx context.Context) int {
	s := spanFromContext(ctx)
	if s == nil {
		return -1
	}
	return s.Depth()
}

// noopSpan is returned for unsampled traces.
type noopSpan struct {
	sc     SpanContext
	parent SpanID
	depth  int
	ctx    context.Context
}

func (n *noopSpan) Depth() int                             { return n.depth }
func (n *noopSpan) SpanContext() SpanContext              { return n.sc }
func (n *noopSpan) ParentID() SpanID                      { return n.parent }
func (n *noopSpan) SetAttribute(string, any)              {}
func (n *noopSpan) AddEvent(string, map[string]any)       {}
func (n *noopSpan) RecordError(error)                     {}
func (n *noopSpan) End()                                  {}
func (n *noopSpan) Done() <-chan struct{}                 { return nil }
func (n *noopSpan) Context() context.Context              { return n.ctx }
