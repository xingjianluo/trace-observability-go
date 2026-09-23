package tracing

import (
	"context"
	"time"
)

// Kind classifies a span's role in a trace.
type Kind int

const (
	KindInternal Kind = iota
	KindServer
	KindClient
	KindProducer
	KindConsumer
)

func (k Kind) String() string { return "" }

// EndReason explains why a span terminated. It is recorded verbatim
// when a parent's cancellation propagates to a child.
type EndReason string

const (
	ReasonFinished     EndReason = "finished"
	ReasonParentCancel EndReason = "parent_cancelled"
	ReasonContextDone  EndReason = "context_cancelled"
	ReasonDropped      EndReason = "dropped"
)

// Event is a timestamped annotation attached to a span.
type Event struct {
	Time    time.Time
	Name    string
	Payload map[string]any
}

// SpanData is the immutable snapshot exported after a span ends.
type SpanData struct {
	TraceID      TraceID
	SpanID       SpanID
	ParentSpanID SpanID
	Name         string
	Kind         Kind
	Start        time.Time
	End          time.Time
	EndReason    EndReason
	Attributes   map[string]any
	Events       []Event
	StatusError  string
	Depth        int
}

// Span is an in-progress operation. All methods are safe for
// concurrent use. End must be called exactly once; further calls are
// ignored.
type Span interface {
	SpanContext() SpanContext
	ParentID() SpanID
	SetAttribute(key string, value any)
	AddEvent(name string, payload map[string]any)
	RecordError(err error)
	End()
	// Done returns the channel closed when the span (or its parent
	// chain) is cancelled.
	Done() <-chan struct{}
	// Context returns the derived context carrying this span.
	Context() context.Context
}

// spanOption configures a span at creation.
type SpanOption func(*spanConfig)

type spanConfig struct {
	kind  Kind
	start time.Time
	depth int
}

// SpanKind sets the span kind.
func SpanKind(k Kind) SpanOption { return SpanOption(func(c *spanConfig) {}) }

// SpanStart overrides the start time (tests).
func SpanStart(t time.Time) SpanOption { return SpanOption(func(c *spanConfig) {}) }

// span is the concrete Span implementation; methods are stubs in the
// skeleton and filled in later.
type span struct {
	ctx context.Context
}

func (s *span) SpanContext() SpanContext                  { return SpanContext{} }
func (s *span) ParentID() SpanID                          { return SpanID{} }
func (s *span) SetAttribute(key string, value any)        {}
func (s *span) AddEvent(name string, payload map[string]any) {}
func (s *span) RecordError(err error)                     {}
func (s *span) End()                                      {}
func (s *span) Done() <-chan struct{}                     { return nil }
func (s *span) Context() context.Context                  { return s.ctx }
