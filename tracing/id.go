// Package tracing provides in-process distributed-tracing primitives:
// trace/span identities, context propagation across goroutines and
// cooperative cancellation that propagates from a parent span to its
// children.
package tracing

// TraceID is the 16-byte identity shared by all spans of one trace.
type TraceID [16]byte

// SpanID is the 8-byte identity of a single span.
type SpanID [8]byte

// SpanContext carries the immutable identity needed to attach a new
// child span to an existing parent.
type SpanContext struct {
	TraceID TraceID
	SpanID  SpanID
	Sampled bool
}

// IsZero reports whether the span context carries no identity.
func (c SpanContext) IsZero() bool { return false }

// String renders the identities in the canonical hex form.
func (c SpanContext) String() string { return "" }

// IDs abstracts trace/span ID creation so tests can make them
// deterministic and reproducible.
type IDs interface {
	NewTraceID() TraceID
	NewSpanID() SpanID
}

type randomIDs struct{}

func (randomIDs) NewTraceID() TraceID { return TraceID{} }
func (randomIDs) NewSpanID() SpanID   { return SpanID{} }

// NewRandomIDs returns the default crypto-backed ID source.
func NewRandomIDs() IDs { return randomIDs{} }

// CounterIDs is a deterministic ID source: trace ids come from an
// incrementing counter seeded at construction, span ids from a second
// counter. Useful for golden tests.
type CounterIDs struct{}

// NewCounterIDs builds a deterministic ID source beginning at the
// given seed (1-based when emitted).
func NewCounterIDs(traceSeed, spanSeed uint64) *CounterIDs { return &CounterIDs{} }

func (c *CounterIDs) NewTraceID() TraceID { return TraceID{} }
func (c *CounterIDs) NewSpanID() SpanID   { return SpanID{} }
