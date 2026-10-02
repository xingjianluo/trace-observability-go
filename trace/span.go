package trace

import (
	"context"
	"sync"
	"time"
)

// 终止原因常量。调用方也可以向 End 传入自定义原因字符串覆盖自动判定。
const (
	ReasonCompleted        = "completed"
	ReasonError            = "error"
	ReasonCanceled         = "canceled"
	ReasonParentCanceled   = "parent_canceled"
	ReasonDeadlineExceeded = "deadline_exceeded"
)

// SpanRecord 是一个 Span 结束后的不可变快照，供导出使用。
type SpanRecord struct {
	TraceID       string
	SpanID        string
	ParentSpanID  string
	Name          string
	StartTime     time.Time
	EndTime       time.Time
	Attributes    map[string]string
	Status        string
	EndReason     string
	Depth         int
	RemoteParent  bool
	SamplingState SamplingState
}

// Span 表示一个进行中的操作片段。
type Span interface {
	Context() context.Context
	SpanID() string
	TraceID() string
	ParentSpanID() string
	Name() string
	Depth() int
	SamplingState() SamplingState
	SetAttribute(key, value string)
	// Cancel 取消该 Span 派生的上下文，子操作会同步收到取消信号。
	// 它不会自动结束 Span；调用方仍应调用 End 记录终止。
	Cancel()
	// End 结束 Span。reason 为空时依据 ctx 状态自动判定终止原因；
	// 重复调用是幂等的，只有第一次生效，避免重复计入。
	End(reason string)
	Record() SpanRecord
	Ended() bool
}

type spanKey struct{}

type spanImpl struct {
	mu       sync.Mutex
	cfg      SpanConfig
	traceID  string
	spanID   string
	parentID string
	depth    int
	name     string
	start    time.Time
	end      time.Time
	attrs    map[string]string
	status   string
	reason   string
	ended    bool
	ctx      context.Context
	cancel   context.CancelFunc
	parent   Span
	attached []func(SpanRecord)
}

func (s *spanImpl) Context() context.Context     { return s.ctx }
func (s *spanImpl) SpanID() string               { return s.spanID }
func (s *spanImpl) TraceID() string              { return s.traceID }
func (s *spanImpl) ParentSpanID() string         { return s.parentID }
func (s *spanImpl) Name() string                 { return s.name }
func (s *spanImpl) Depth() int                   { return s.depth }
func (s *spanImpl) SamplingState() SamplingState { return s.cfg.FinalSampling }

func (s *spanImpl) SetAttribute(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return
	}
	s.attrs[key] = value
}

func (s *spanImpl) Cancel() {
	s.cancel()
}

// attachEndObserver 登记一个附加结束观察者；Span 已结束时为空操作，
// 保证不会对同一成员触发两次。
func (s *spanImpl) attachEndObserver(fn func(SpanRecord)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return
	}
	s.attached = append(s.attached, fn)
}

func (s *spanImpl) Ended() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ended
}

func (s *spanImpl) End(reason string) {
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.ended = true

	if reason == "" {
		reason = detectEndReason(s.ctx, s.parent)
	}
	s.reason = reason
	s.status = "ok"
	if reason == ReasonError {
		s.status = "error"
	}
	s.end = time.Now()
	if s.end.Before(s.start) {
		s.end = s.start
	}
	rec := s.snapshotLocked()
	cb := s.cfg.onEnd
	attached := s.attached
	s.attached = nil
	s.mu.Unlock()

	s.cancel()
	if cb != nil {
		cb(rec)
	}
	// 附加观察者只针对被登记的这一个 Span，且在 End 的首次生效调用
	// 里随主回调一起触发，保证“一个成员只被汇总一次”。
	for _, ob := range attached {
		ob(rec)
	}
}

// detectEndReason 在结束前依据上下文错误判定终止原因。
// 必须在调用本 Span 自身的 cancel 之前判定。
func detectEndReason(ctx context.Context, parent Span) string {
	switch ctx.Err() {
	case nil:
		return ReasonCompleted
	case context.DeadlineExceeded:
		return ReasonDeadlineExceeded
	case context.Canceled:
		if parent != nil && parent.Context().Err() != nil {
			return ReasonParentCanceled
		}
		return ReasonCanceled
	default:
		return ReasonCompleted
	}
}

func (s *spanImpl) Record() SpanRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked()
}

func (s *spanImpl) snapshotLocked() SpanRecord {
	attrs := make(map[string]string, len(s.attrs))
	for k, v := range s.attrs {
		attrs[k] = v
	}
	return SpanRecord{
		TraceID:       s.traceID,
		SpanID:        s.spanID,
		ParentSpanID:  s.parentID,
		Name:          s.name,
		StartTime:     s.start,
		EndTime:       s.end,
		Attributes:    attrs,
		Status:        s.status,
		EndReason:     s.reason,
		Depth:         s.depth,
		RemoteParent:  s.cfg.RemoteParent,
		SamplingState: s.cfg.FinalSampling,
	}
}

// StartSpan 在 ctx 中查找父 Span 并创建子 Span。
// 子 Span 的 context 派生自父 Span 的 context，因此父取消会同步传播到子。
func StartSpan(ctx context.Context, name string, opts ...SpanOption) (context.Context, Span) {
	cfg := SpanConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.Parent == nil {
		if p, ok := ctx.Value(spanKey{}).(Span); ok {
			cfg.Parent = p
		}
	}
	// 应用装配层（如 obs.Pipeline）经由 context 下发的继承选项。
	applyInherited(ctx, &cfg)

	ids := DefaultIDSource()
	s := &spanImpl{
		cfg:   cfg,
		name:  name,
		start: time.Now(),
		attrs: map[string]string{},
	}
	if cfg.Parent != nil {
		s.parent = cfg.Parent
		s.traceID = cfg.Parent.TraceID()
		s.parentID = cfg.Parent.SpanID()
		s.depth = cfg.Parent.Depth() + 1
		if cfg.FinalSampling == SamplingDefer {
			s.cfg.FinalSampling = cfg.Parent.SamplingState()
		}
		s.ctx, s.cancel = context.WithCancel(cfg.Parent.Context())
	} else {
		s.parentID = cfg.RemoteParentID
		if cfg.PresetTraceID != "" {
			s.traceID = cfg.PresetTraceID
		} else {
			s.traceID = ids.NewTraceID()
		}
		s.ctx, s.cancel = context.WithCancel(ctx)
	}
	if cfg.RemoteParent {
		s.parentID = cfg.RemoteParentID
	}
	s.spanID = ids.NewSpanID()
	for k, v := range cfg.Attributes {
		s.attrs[k] = v
	}

	// 把本 Span 的最终采样状态与结束回调继续向下游传播，
	// 使 Group.Go 等“看不到原始 opts”的派生点也能自动继承。
	childCtx := contextWithInherit(s.ctx, inheritedOptions{
		finalSampling: s.cfg.FinalSampling,
		onEnd:         s.cfg.onEnd,
		hasOnEnd:      s.cfg.onEnd != nil,
	})
	childCtx = context.WithValue(childCtx, spanKey{}, Span(s))
	return childCtx, s
}

// SpanFromContext 从 ctx 中提取当前 Span。
func SpanFromContext(ctx context.Context) Span {
	if s, ok := ctx.Value(spanKey{}).(Span); ok {
		return s
	}
	return nil
}

// ContextWithSpan 返回携带指定 Span 的上下文。
func ContextWithSpan(ctx context.Context, s Span) context.Context {
	return context.WithValue(ctx, spanKey{}, s)
}
