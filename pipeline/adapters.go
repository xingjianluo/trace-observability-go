package pipeline

import (
	"context"
	"time"

	"github.com/xingjianluo/trace-observability-go/export"
	"github.com/xingjianluo/trace-observability-go/logx"
	"github.com/xingjianluo/trace-observability-go/tracing"
)

// spanRecorder adapts finished spans to export envelopes. It is
// concurrency-safe: the only shared state is the clock function and
// the queue, both of which are safe for concurrent use.
type spanRecorder struct {
	enqueue func(export.Envelope)
	now     func() time.Time
}

func (r spanRecorder) RecordSpan(d tracing.SpanData) {
	r.enqueue(export.Envelope{
		Kind:      export.KindSpan,
		Timestamp: r.now().UnixNano(),
		Payload:   d,
	})
}

// logSink adapts redacted log entries to export envelopes.
type logSink struct {
	enqueue func(export.Envelope)
	now     func() time.Time
}

func (s logSink) Write(e logx.Entry) {
	s.enqueue(export.Envelope{
		Kind:      export.KindLog,
		Timestamp: s.now().UnixNano(),
		Payload:   e,
	})
}

// blockingEnqueue offers an item with the BLOCK policy semantics in
// the common case; it is invoked from hot paths (span End / log), so
// the context is just background: ordering/loss is governed by the
// configured queue policy, not by a request context.
func blockingEnqueue(q *export.Queue, e export.Envelope) {
	_ = q.Enqueue(context.Background(), e)
}
