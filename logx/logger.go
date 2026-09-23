// Package logx provides tracing-correlated structured logging. Every
// emitted entry carries the trace/span ids and the parent-chain depth
// found on the context, so concurrent operations can never mix their
// correlation fields.
package logx

import (
	"context"
	"time"

	"github.com/xingjianluo/trace-observability-go/tracing"
)

// Level is a log severity.
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

func (l Level) String() string { return "" }

// Field is one structured key/value pair.
type Field struct {
	Key   string
	Value any
}

// F is shorthand for constructing a Field.
func F(key string, value any) Field { return Field{Key: key, Value: value} }

// Entry is the redacted, serializable record handed to sinks.
type Entry struct {
	Time    time.Time
	Level   Level
	Message string
	TraceID string
	SpanID  string
	ParentID string
	Depth   int
	Fields  []Field
}

// Sink receives completed entries. Must be concurrency-safe.
type Sink interface {
	Write(Entry)
}

// SinkFunc adapts a function to Sink.
type SinkFunc func(Entry)

func (f SinkFunc) Write(e Entry) {}

// Logger resolves correlation information from contexts and applies
// redaction before entries reach the sink.
type Logger struct {
	sink  Sink
	redact func(Entry) Entry
	now   func() time.Time
}

// Config configures a Logger.
type Config struct {
	// MinLevel suppresses entries below it.
	MinLevel Level
	// ExtraFixed fields are merged into every entry.
	ExtraFixed []Field
}

// New builds a Logger. redactor may be nil, in which case entries
// pass through unchanged.
func New(sink Sink, redactor func(Entry) Entry, cfg Config) *Logger { return &Logger{} }

// Log emits one entry at level, correlating it with the span on ctx.
func (l *Logger) Log(ctx context.Context, level Level, msg string, fields ...Field) {}

func (l *Logger) Debug(ctx context.Context, msg string, fields ...Field) {}
func (l *Logger) Info(ctx context.Context, msg string, fields ...Field)  {}
func (l *Logger) Warn(ctx context.Context, msg string, fields ...Field)  {}
func (l *Logger) Error(ctx context.Context, msg string, fields ...Field) {}

var _ = tracing.SpanContext{}
