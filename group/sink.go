package group

import "context"

// MissingReason* 描述一个成员“没能纳入统计”的原因。
// 队列相关的原因与 export 包回执中的字符串保持一致。
const (
	// MissingSampledOut：整条链路未被采样，成员明细不导出。
	MissingSampledOut = "sampled_out"
	// MissingQueueDroppedNew：队列积压，本条作为最新记录被丢弃。
	MissingQueueDroppedNew = "queue_dropped_new"
	// MissingQueueDroppedOld：队列积压，本条挤掉了更旧的记录后入队
	// （对本条而言仍被接受，旧条的丢失计入全局 DroppedOld）。
	MissingQueueDroppedOld = "queue_dropped_old"
	// MissingExporterClosed：导出器已关闭，记录未被接收。
	MissingExporterClosed = "exporter_closed"
)

// Delivery 是一个成员 Span 向导出管线投递的回执。
type Delivery struct {
	// Accepted 表示成员明细是否真正进入导出队列。
	Accepted bool
	// MissingReason 在未纳入时给出具体原因；Accepted=true 时为空。
	MissingReason string
}

// DeliverySink 由装配层（obs.Pipeline）实现。组成员的 Span 仍由
// 管线统一导出（Span 结束回调里入队），组只通过本接口“旁观”投递结果，
// 因此同一条成员明细绝不会被导出两次：
//   - Expect 在成员创建后登记：该 SpanID 的投递回执有人认领；
//   - TakeDelivery 在成员结束（管线回调已执行）后取走并删除一次性回执。
type DeliverySink interface {
	Expect(spanID string)
	TakeDelivery(spanID string) (Delivery, bool)
}

// SummarySink 由装配层实现：接收组收尾时唯一的一条组级汇总。
type SummarySink interface {
	DeliverSummary(s Summary)
}

type deliverySinkKey struct{}
type summarySinkKey struct{}
type currentGroupKey struct{}

// ContextWithSinks 把装配层的投递/汇总回执接口注入上下文，
// 供该上下文内创建的业务组拾取（由 obs.Pipeline.StartSpan 调用）。
func ContextWithSinks(ctx context.Context, delivery DeliverySink, summary SummarySink) context.Context {
	if delivery != nil {
		ctx = context.WithValue(ctx, deliverySinkKey{}, delivery)
	}
	if summary != nil {
		ctx = context.WithValue(ctx, summarySinkKey{}, summary)
	}
	return ctx
}

func deliverySinkFromContext(ctx context.Context) DeliverySink {
	if s, ok := ctx.Value(deliverySinkKey{}).(DeliverySink); ok {
		return s
	}
	return nil
}

func summarySinkFromContext(ctx context.Context) SummarySink {
	if s, ok := ctx.Value(summarySinkKey{}).(SummarySink); ok {
		return s
	}
	return nil
}

func contextWithGroup(ctx context.Context, g *Group) context.Context {
	return context.WithValue(ctx, currentGroupKey{}, g)
}

func groupFromContext(ctx context.Context) *Group {
	if g, ok := ctx.Value(currentGroupKey{}).(*Group); ok {
		return g
	}
	return nil
}
