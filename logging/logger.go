package logging

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/xingjianluo/trace-observability-go/trace"
)

// 日志等级常量。
const (
	LevelDebug = "debug"
	LevelInfo  = "info"
	LevelWarn  = "warn"
	LevelError = "error"
)

// Entry 是一条结构化日志的不可变快照。
type Entry struct {
	Seq          int64          `json:"seq"`
	Time         time.Time      `json:"time"`
	Level        string         `json:"level"`
	Message      string         `json:"message"`
	TraceID      string         `json:"trace_id"`
	SpanID       string         `json:"span_id"`
	ParentSpanID string         `json:"parent_span_id"`
	Depth        int            `json:"depth"`
	Fields       map[string]any `json:"fields,omitempty"`
}

// Sink 接收完成脱敏的日志条目。
type Sink interface {
	OnLog(e Entry)
}

// DropFunc 在日志因采样决策被跳过/接收失败时被回调，便于计数。
type DropFunc func(e Entry, reason string)

// Logger 产出与当前 Span 关联的结构化日志。
type Logger struct {
	sink     Sink
	masker   *Masker
	dropped  DropFunc
	seq      *atomic.Int64
	now      func() time.Time
	defaults map[string]any
}

// Option 配置 Logger。
type Option func(*Logger)

// WithClock 注入时钟（测试用）。
func WithClock(f func() time.Time) Option {
	return func(l *Logger) { l.now = f }
}

// WithDropObserver 注册日志被跳过时的观察者。
func WithDropObserver(f DropFunc) Option {
	return func(l *Logger) { l.dropped = f }
}

// NewLogger 创建日志器；sink 通常是导出管线，masker 为 nil 时不做脱敏。
func NewLogger(sink Sink, masker *Masker, opts ...Option) *Logger {
	l := &Logger{sink: sink, masker: masker, now: time.Now, seq: &atomic.Int64{}}
	for _, o := range opts {
		o(l)
	}
	return l
}

// With 返回携带预置字段的派生日志器（原日志器不受影响）。
func (l *Logger) With(fields map[string]any) *Logger {
	merged := make(map[string]any, len(l.defaults)+len(fields))
	for k, v := range l.defaults {
		merged[k] = v
	}
	for k, v := range fields {
		merged[k] = v
	}
	cp := *l
	cp.defaults = merged
	return &cp
}

// Debug/Info/Warn/Error 在当前 ctx 对应 Span 下记录日志。
func (l *Logger) Debug(ctx context.Context, msg string, fields map[string]any) {
	l.Emit(ctx, LevelDebug, msg, fields)
}
func (l *Logger) Info(ctx context.Context, msg string, fields map[string]any) {
	l.Emit(ctx, LevelInfo, msg, fields)
}
func (l *Logger) Warn(ctx context.Context, msg string, fields map[string]any) {
	l.Emit(ctx, LevelWarn, msg, fields)
}
func (l *Logger) Error(ctx context.Context, msg string, fields map[string]any) {
	l.Emit(ctx, LevelError, msg, fields)
}

// Emit 是所有等级共用的记录入口：先合并字段，再脱敏，最后投递。
// 脱敏在投递之前完成，因此下游 Sink 永远接触不到原始敏感值。
func (l *Logger) Emit(ctx context.Context, level, msg string, fields map[string]any) {
	merged := make(map[string]any, len(l.defaults)+len(fields))
	for k, v := range l.defaults {
		merged[k] = v
	}
	for k, v := range fields {
		merged[k] = v
	}
	var masked map[string]any
	if l.masker != nil {
		masked = l.masker.MaskFields(merged)
	} else {
		masked = merged
	}

	e := Entry{
		Seq:     l.seq.Add(1),
		Time:    l.now(),
		Level:   level,
		Message: msg,
		Fields:  masked,
	}
	if s := trace.SpanFromContext(ctx); s != nil {
		e.TraceID = s.TraceID()
		e.SpanID = s.SpanID()
		e.ParentSpanID = s.ParentSpanID()
		e.Depth = s.Depth()
	}

	// 采样状态为 Drop 的链路上不产生日志；Accept/Defer/无 Span 均输出。
	if s := trace.SpanFromContext(ctx); s != nil && s.SamplingState() == trace.SamplingDrop {
		if l.dropped != nil {
			l.dropped(e, "sampled_out")
		}
		return
	}
	if l.sink != nil {
		l.sink.OnLog(e)
	}
}
