package trace

import (
	"context"
	"strconv"
)

// Carrier 协议使用的标准键。
const (
	HeaderTraceID = "x-trace-traceid"
	HeaderSpanID  = "x-trace-spanid"
	HeaderSampled = "x-trace-sampled"
)

// Carrier 是跨进程边界传递追踪上下文的载体（如 HTTP 头、消息属性）。
type Carrier interface {
	Get(key string) string
	Set(key, value string)
}

// MapCarrier 是基于 map 的内存 Carrier 实现，便于本地验证与测试。
type MapCarrier struct{ M map[string]string }

func (c *MapCarrier) Get(key string) string {
	if c == nil || c.M == nil {
		return ""
	}
	return c.M[key]
}

func (c *MapCarrier) Set(key, value string) {
	if c.M == nil {
		c.M = map[string]string{}
	}
	c.M[key] = value
}

// remoteSpan 表示从进程外恢复出来的父级：它只携带标识，不是活动 Span，
// 不会被导出，End/Cancel 均为空操作；其 Context 即提取时传入的本地 ctx。
type remoteSpan struct {
	traceID string
	spanID  string
	state   SamplingState
	ctx     context.Context
}

func (s *remoteSpan) Context() context.Context     { return s.ctx }
func (s *remoteSpan) SpanID() string               { return s.spanID }
func (s *remoteSpan) TraceID() string              { return s.traceID }
func (s *remoteSpan) ParentSpanID() string         { return "" }
func (s *remoteSpan) Name() string                 { return "remote-parent" }
func (s *remoteSpan) Depth() int                   { return 0 }
func (s *remoteSpan) SamplingState() SamplingState { return s.state }
func (s *remoteSpan) SetAttribute(string, string)  {}
func (s *remoteSpan) Cancel()                      {}
func (s *remoteSpan) End(string)                   {}
func (s *remoteSpan) Record() SpanRecord           { return SpanRecord{RemoteParent: true} }
func (s *remoteSpan) Ended() bool                  { return true }

// Inject 把 ctx 中的 Span 上下文写入 Carrier。
func Inject(ctx context.Context, c Carrier) {
	s := SpanFromContext(ctx)
	if s == nil || c == nil {
		return
	}
	c.Set(HeaderTraceID, s.TraceID())
	c.Set(HeaderSpanID, s.SpanID())
	switch s.SamplingState() {
	case SamplingAccept:
		c.Set(HeaderSampled, "1")
	case SamplingDrop:
		c.Set(HeaderSampled, "0")
	default:
		c.Set(HeaderSampled, "")
	}
}

// Extract 从 Carrier 恢复 Span 上下文并返回携带该（远端）Span 的 context。
// Carrier 为空时原样返回 ctx。
func Extract(ctx context.Context, c Carrier) context.Context {
	if c == nil {
		return ctx
	}
	tid := c.Get(HeaderTraceID)
	sid := c.Get(HeaderSpanID)
	if tid == "" || sid == "" {
		return ctx
	}
	state := SamplingDefer
	switch c.Get(HeaderSampled) {
	case "1":
		state = SamplingAccept
	case "0":
		state = SamplingDrop
	default:
		if v, err := strconv.Atoi(c.Get(HeaderSampled)); err == nil {
			if v == 1 {
				state = SamplingAccept
			} else if v == 0 {
				state = SamplingDrop
			}
		}
	}
	rs := &remoteSpan{traceID: tid, spanID: sid, state: state, ctx: ctx}
	return ContextWithSpan(ctx, rs)
}
