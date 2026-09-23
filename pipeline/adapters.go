package pipeline

import (
	"time"

	"github.com/xingjianluo/trace-observability-go/export"
	"github.com/xingjianluo/trace-observability-go/logx"
	"github.com/xingjianluo/trace-observability-go/tracing"
)

// spanRecorder adapts finished spans to export envelopes.
type spanRecorder struct {
	now func() time.Time
}

func (r spanRecorder) RecordSpan(d tracing.SpanData) {}

// logSink adapts log entries to export envelopes.
type logSink struct{}

func (s logSink) Write(e logx.Entry) {}

func envelopeForSpan(now func() time.Time, d tracing.SpanData) export.Envelope {
	return export.Envelope{}
}

func envelopeForLog(now func() time.Time, e logx.Entry) export.Envelope {
	return export.Envelope{}
}
