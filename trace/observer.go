package trace

import "context"

// 投递结果分类。分组汇总用它区分“实际纳入统计”与“没能纳入统计”的成员。
const (
	// DeliveryOutcomeAccepted 表示记录已被下游（如有界队列）接受。
	DeliveryOutcomeAccepted = "accepted"
	// DeliveryOutcomeDroppedQueue 表示记录因队列满/背压策略被丢弃。
	DeliveryOutcomeDroppedQueue = "queue_dropped"
	// DeliveryOutcomeNotSampled 表示成员所在链路未被采样，明细不会导出。
	DeliveryOutcomeNotSampled = "sampled_out"
	// DeliveryOutcomeClosed 表示记录到达时下游已关闭（如收尾之后）。
	DeliveryOutcomeClosed = "closed"
	// DeliveryOutcomeUnknown 表示没有装配层报告投递结果（如直接使用 trace 包）。
	DeliveryOutcomeUnknown = "unknown"
)

// DeliveryResult 描述一条 Span 结束记录在下游的投递结论。
// trace 包本身只负责把它交给观察者；具体 Outcome 由装配层（obs）填写。
type DeliveryResult struct {
	Outcome string // accepted / queue_dropped / sampled_out / closed / unknown
	Reason  string // 可读的丢弃/接受原因
}

// SpanObserver 观察 Span 的开始与结束。
//
// 分组用它来确定“谁是本组的直接成员”以及成员的最终结果：
//   - OnSpanStart 在 Span 创建时同步调用（在返回给调用方之前），
//     用于在并发派生点登记成员名额；
//   - OnSpanEnd 在 Span 结束回调触发后、恰好调用一次（End 幂等保证），
//     rec 为结束快照，delivery 为下游投递结论（未经装配层时为 unknown）。
//
// 实现必须并发安全；不得在回调中阻塞等待本组成员结束，否则会死锁。
type SpanObserver interface {
	OnSpanStart(s Span)
	OnSpanEnd(rec SpanRecord, delivery DeliveryResult)
}

// observerContextKey 携带“最近一个外层分组”的观察者。
// 嵌套分组在自己的上下文中覆盖该值，因此每个 Span 只报告给最近的分组。
type observerContextKey struct{}

// contextWithSpanObserver 返回携带指定观察者的上下文。
func contextWithSpanObserver(ctx context.Context, ob SpanObserver) context.Context {
	if ob == nil {
		return ctx
	}
	return context.WithValue(ctx, observerContextKey{}, ob)
}

// ContextWithSpanObserver 是 contextWithSpanObserver 的导出版本，
// 供 group 等外部包把自己的成员收集器放进派生上下文。
func ContextWithSpanObserver(ctx context.Context, ob SpanObserver) context.Context {
	return contextWithSpanObserver(ctx, ob)
}

// spanObserverFromContext 取出最近的分组观察者；没有则返回 nil。
func spanObserverFromContext(ctx context.Context) SpanObserver {
	ob, _ := ctx.Value(observerContextKey{}).(SpanObserver)
	return ob
}

// groupIDContextKey 携带最近一个外层分组的 SpanID，用于给成员明细
// 打上 group_span_id 归属标记（汇总与明细据此对齐）。
type groupIDContextKey struct{}

// ContextWithGroupID 返回携带分组 SpanID 的上下文（分组创建自己的 Span 后使用）。
func ContextWithGroupID(ctx context.Context, groupSpanID string) context.Context {
	return context.WithValue(ctx, groupIDContextKey{}, groupSpanID)
}

// groupIDFromContext 返回 ctx 中最近分组的 SpanID；没有则返回空串。
func groupIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(groupIDContextKey{}).(string)
	return v
}

// originContextKey 携带“整条 Span 链真正的外部派生源 ctx”。
// 业务组把它设置为组的 parent（调用方传入的最外层 ctx，不会被组自己
// cancel 污染），从而在“组 Stop 与父 deadline 几乎同时发生”的竞态下，
// 仍能稳定判定成员是因外部超时（deadline_exceeded）还是被组停（parent_canceled）。
type originContextKey struct{}

// ContextWithOrigin 在 ctx 中记录外部派生源 ctx（供组 Span 创建时读取）。
func ContextWithOrigin(ctx, origin context.Context) context.Context {
	if origin == nil {
		return ctx
	}
	return context.WithValue(ctx, originContextKey{}, origin)
}

// originFromContext 取出外部派生源 ctx；没有则返回 nil。
func originFromContext(ctx context.Context) context.Context {
	v, _ := ctx.Value(originContextKey{}).(context.Context)
	return v
}
