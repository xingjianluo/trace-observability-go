// Package tracing provides in-process distributed-tracing primitives:
// trace/span identities, context propagation across goroutines and
// cooperative cancellation that propagates from a parent span to its
// children.
package tracing

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
)

// TraceID is the 16-byte identity shared by all spans of one trace.
type TraceID [16]byte

// SpanID is the 8-byte identity of a single span.
type SpanID [8]byte

// Hex renders the trace id as 32 lowercase hex characters.
func (t TraceID) Hex() string {
	b := make([]byte, 32)
	hex.Encode(b, t[:])
	return string(b)
}

// Hex renders the span id as 16 lowercase hex characters.
func (s SpanID) Hex() string {
	b := make([]byte, 16)
	hex.Encode(b, s[:])
	return string(b)
}

// SpanContext carries the immutable identity needed to attach a new
// child span to an existing parent.
type SpanContext struct {
	TraceID TraceID
	SpanID  SpanID
	Sampled bool
}

// IsZero reports whether the span context carries no identity.
func (c SpanContext) IsZero() bool {
	return c.TraceID == TraceID{} && c.SpanID == SpanID{}
}

// String renders the identities as "<trace-hex>:<span-hex>".
func (c SpanContext) String() string {
	return c.TraceID.Hex() + ":" + c.SpanID.Hex()
}

// IDs abstracts trace/span ID creation so tests can make them
// deterministic and reproducible.
type IDs interface {
	NewTraceID() TraceID
	NewSpanID() SpanID
}

type randomIDs struct{}

func (randomIDs) NewTraceID() TraceID {
	var t TraceID
	if _, err := rand.Read(t[:]); err != nil {
		// crypto/rand only fails on a broken runtime; panic to keep
		// the zero-id ambiguity from silently corrupting traces.
		panic("tracing: cannot read random bytes: " + err.Error())
	}
	return t
}

func (randomIDs) NewSpanID() SpanID {
	var s SpanID
	if _, err := rand.Read(s[:]); err != nil {
		panic("tracing: cannot read random bytes: " + err.Error())
	}
	return s
}

// NewRandomIDs returns the default crypto-backed ID source.
func NewRandomIDs() IDs { return randomIDs{} }

// CounterIDs is a deterministic ID source: the 64-bit counter is
// placed in the low 8 bytes of the 16-byte trace id (span ids are
// 8 bytes directly), big-endian. The high bytes are zero. This makes
// golden tests reproducible across processes and platforms.
type CounterIDs struct {
	mu       sync.Mutex
	traceSeq uint64
	spanSeq  uint64
}

// NewCounterIDs builds a deterministic ID source. The first emitted
// trace/span id encodes traceSeed+1 / spanSeed+1, so a 0 seed yields
// ...0001.
func NewCounterIDs(traceSeed, spanSeed uint64) *CounterIDs {
	return &CounterIDs{traceSeq: traceSeed, spanSeq: spanSeed}
}

func (c *CounterIDs) NewTraceID() TraceID {
	c.mu.Lock()
	c.traceSeq++
	v := c.traceSeq
	c.mu.Unlock()
	var t TraceID
	for i := 0; i < 8; i++ {
		t[15-i] = byte(v >> (8 * i))
	}
	return t
}

func (c *CounterIDs) NewSpanID() SpanID {
	c.mu.Lock()
	c.spanSeq++
	v := c.spanSeq
	c.mu.Unlock()
	var s SpanID
	for i := 0; i < 8; i++ {
		s[7-i] = byte(v >> (8 * i))
	}
	return s
}
