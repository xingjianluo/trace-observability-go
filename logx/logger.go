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

func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "debug"
	case LevelInfo:
		return "info"
	case LevelWarn:
		return "warn"
	case LevelError:
		return "error"
	default:
		return "unknown"
	}
}

// Field is one structured key/value pair.
type Field struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

// F is shorthand for constructing a Field.
func F(key string, value any) Field { return Field{Key: key, Value: value} }

// Entry is the redacted, serializable record handed to sinks.
type Entry struct {
	Time     time.Time `json:"time"`
	Level    Level     `json:"level"`
	Message  string    `json:"message"`
	TraceID  string    `json:"trace_id,omitempty"`
	SpanID   string    `json:"span_id,omitempty"`
	ParentID string    `json:"parent_id,omitempty"`
	Depth    int       `json:"depth"`
	Fields   []Field   `json:"fields,omitempty"`
}

// Sink receives completed entries. Must be concurrency-safe.
type Sink interface {
	Write(Entry)
}

// SinkFunc adapts a function to Sink.
type SinkFunc func(Entry)

func (f SinkFunc) Write(e Entry) { f(e) }

// Logger resolves correlation information from contexts and applies
// redaction before entries reach the sink.
type Logger struct {
	sink       Sink
	redactor   *Redactor
	now        func() time.Time
	minLevel   Level
	fixed      []Field
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
func New(sink Sink, redactor *Redactor, cfg Config) *Logger {
	return &Logger{
		sink:     sink,
		redactor: redactor,
		now:      time.Now,
		minLevel: cfg.MinLevel,
		fixed:    append([]Field(nil), cfg.ExtraFixed...),
	}
}

// Log emits one entry at level, correlating it with the span on ctx.
// Redaction happens before the entry touches the sink, so no sink
// implementation can observe an unredacted value.
func (l *Logger) Log(ctx context.Context, level Level, msg string, fields ...Field) {
	if level < l.minLevel {
		return
	}
	e := Entry{
		Time:    l.now(),
		Level:   level,
		Message: msg,
		Depth:   -1,
	}
	if sp := tracing.SpanFromContext(ctx); sp != nil {
		sc := sp.SpanContext()
		e.TraceID = sc.TraceID.Hex()
		e.SpanID = sc.SpanID.Hex()
		if pid := sp.ParentID(); pid != (tracing.SpanID{}) {
			e.ParentID = pid.Hex()
		}
		e.Depth = sp.Depth()
	}
	if len(l.fixed) > 0 {
		e.Fields = append(e.Fields, l.fixed...)
	}
	e.Fields = append(e.Fields, fields...)
	if l.redactor != nil {
		e = l.redactor.Apply(e)
	}
	l.sink.Write(e)
}

func (l *Logger) Debug(ctx context.Context, msg string, fields ...Field) {
	l.Log(ctx, LevelDebug, msg, fields...)
}
func (l *Logger) Info(ctx context.Context, msg string, fields ...Field) {
	l.Log(ctx, LevelInfo, msg, fields...)
}
func (l *Logger) Warn(ctx context.Context, msg string, fields ...Field) {
	l.Log(ctx, LevelWarn, msg, fields...)
}
func (l *Logger) Error(ctx context.Context, msg string, fields ...Field) {
	l.Log(ctx, LevelError, msg, fields...)
}
