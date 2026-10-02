package trace

import (
	"context"
	"errors"
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

// withCancelCause 派生一个可取消子 ctx，并保证“未显式给出 cause 时
// 透传父级真实原因”：context.WithCancelCause(ctx) 在父 ctx 取消而本层
// 未显式 cancel(cause) 时，context.Cause 返回父 ctx 的原始 cause
// （DeadlineExceeded 不会被扁平化成 Canceled）。
func withCancelCause(ctx context.Context) (context.Context, context.CancelCauseFunc) {
	return context.WithCancelCause(ctx)
}

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
	// cancel 为 WithCancelCause 的取消函数：传 nil 时 context.Cause 会
	// 透传父级真实取消原因（如 DeadlineExceeded），避免被扁平化。
	cancel context.CancelCauseFunc
	// origin 仅在“根/业务组中间 Span”上非空，指向真正的外部派生源 ctx。
	origin context.Context
	parent Span
	// observer 是创建时 ctx 中“最近业务组”的观察者；随 Span 结束报告一次。
	// 它不随 SpanConfig 向后代复制，后代通过自己 ctx 中的值找到最近的组。
	observer SpanObserver
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
	s.cancel(nil)
}

func (s *spanImpl) Ended() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ended
}

// parentChain 返回本 Span 的父 Span，供终止原因沿祖先链追溯。
func (s *spanImpl) parentChain() Span { return s.parent }

func (s *spanImpl) End(reason string) {
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.ended = true

	if reason == "" {
		reason = detectEndReason(s.ctx, s.parent, s.origin)
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
	ob := s.observer
	s.mu.Unlock()

	s.cancel(nil)
	// 结束回调恰好触发一次（End 幂等），投递结论随后交给观察者，
	// 使分组能够区分“已纳入统计 / 未采样 / 队列丢弃”的成员。
	delivery := DeliveryResult{Outcome: DeliveryOutcomeUnknown}
	if cb != nil {
		delivery = cb(rec)
		if delivery.Outcome == "" {
			delivery.Outcome = DeliveryOutcomeUnknown
		}
	}
	if ob != nil {
		ob.OnSpanEnd(rec, delivery)
	}
}

// detectEndReason 在结束前依据上下文取消原因判定终止原因。
// 必须在调用本 Span 自身的 cancel 之前判定。
//
// 关键点：
//   - 中间的 context.WithCancel 会把祖先的 DeadlineExceeded 扁平化成
//     Canceled，因此用 context.Cause 沿取消链读取最原始原因；
//   - 但本 Span（尤其业务组 Span）可能已被自己的 cancel(nil) 取消，从而把
//     ctx 的 cause 重写成 Canceled。因此判定时优先看“未被本层污染”的
//     外部源 origin（根 Span 的派生源 ctx），再沿父 Span 链，最后才看自身。
//
// 结论在停止与超时几乎同时发生时也保持唯一一致。
func detectEndReason(ctx context.Context, parent Span, origin context.Context) string {
	// 1. 最外层派生源的真实原因优先。组 Stop 的 cancel 不会污染 origin
	//    （它是调用方传入的最外层 ctx），因此即使“组取消与父 deadline 几乎
	//    同时发生”、本 Span ctx 的 cause 被抢先写成 Canceled，这里仍能稳定
	//    识别出外部超时 => deadline_exceeded。
	if origin != nil && errors.Is(context.Cause(origin), context.DeadlineExceeded) {
		return ReasonDeadlineExceeded
	}
	// 2. 自己 ctx 的真实原因：普通（非组）链路的直接 deadline。
	if errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
		return ReasonDeadlineExceeded
	}
	// 3. 最外层源已取消（非超时）：上级停止 => parent_canceled。
	if origin != nil && origin.Err() != nil {
		return ReasonParentCanceled
	}
	// 4. 沿父 Span 链：祖先先取消（非超时）=> parent_canceled。
	for p := parent; p != nil; {
		if p.Context().Err() != nil {
			return ReasonParentCanceled
		}
		gp, ok := p.(interface{ parentChain() Span })
		if !ok {
			break
		}
		p = gp.parentChain()
	}
	// 5. 仅自己 ctx 被取消、上级都存活 => 自己取消。
	if ctx.Err() != nil {
		return ReasonCanceled
	}
	return ReasonCompleted
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
		// 外部派生源沿 Span 链继承（由 applyInherited 从父 ctx 填充）。
		s.origin = cfg.origin
		// 从传入 ctx 派生而非直接用 Parent.Context()：传入 ctx 既继承了父
		// Span 的取消（父 Span 的 ctx 本身就在该 ctx 链上），又保留了
		// observer / group id / 继承选项等附加键，避免后代丢失分组归属。
		s.ctx, s.cancel = withCancelCause(ctx)
		// 非根 Span 不设置 origin：取消/超时沿父 Span 链追溯即可。
	} else {
		s.parentID = cfg.RemoteParentID
		if cfg.PresetTraceID != "" {
			s.traceID = cfg.PresetTraceID
		} else {
			s.traceID = ids.NewTraceID()
		}
		s.ctx, s.cancel = withCancelCause(ctx)
		// 根 Span 的真正外部派生源优先级：装配/继承选项 > 业务组注入的
		// 最外层 ctx（不被组自身 cancel 污染）> 创建时传入的 ctx。
		switch {
		case s.origin != nil:
		case cfg.origin != nil:
			s.origin = cfg.origin
		case originFromContext(ctx) != nil:
			s.origin = originFromContext(ctx)
		default:
			s.origin = ctx
		}
	}
	if cfg.RemoteParent {
		s.parentID = cfg.RemoteParentID
	}
	s.spanID = ids.NewSpanID()
	for k, v := range cfg.Attributes {
		s.attrs[k] = v
	}
	// 归属标记：在某个分组上下文内创建的 Span 都带上最近分组的 SpanID，
	// 汇总与成员明细据此对齐，不允许出现找不到归属的成员。
	if gid := groupIDFromContext(ctx); gid != "" {
		s.attrs["group_span_id"] = gid
	}
	// 观察者来自创建时 ctx 中“最近的业务组”（独立 context 键透传，
	// 不随 SpanConfig 复制）。本 Span 报告给该组；后代从自己的 ctx 取值。
	ob := spanObserverFromContext(ctx)
	s.observer = ob

	// 把本 Span 的最终采样状态与结束回调继续向下游传播，
	// 使 Group.Go 等“看不到原始 opts”的派生点也能自动继承。
	// observerContextKey 已在父 ctx 中，s.ctx 派生自父 ctx，自然透传，
	// 嵌套组可用 ContextWithSpanObserver 在更内层覆盖为自己的收集器。
	childCtx := contextWithInherit(s.ctx, inheritedOptions{
		finalSampling: s.cfg.FinalSampling,
		onEnd:         s.cfg.onEnd,
		hasOnEnd:      s.cfg.onEnd != nil,
		origin:        s.origin,
	})
	childCtx = context.WithValue(childCtx, spanKey{}, Span(s))
	// 开始通知在返回给调用方之前同步触发，保证并发派生点的名额登记
	// 与 Wait 的归零判定之间有明确的 happens-before 关系。
	if ob != nil {
		ob.OnSpanStart(Span(s))
	}
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
