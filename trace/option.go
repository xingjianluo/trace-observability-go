package trace

import "context"

// SamplingState 表示采样决策在追踪链路上的传播状态。
type SamplingState int

const (
	// SamplingDefer 表示本 Span 不做决策，沿用链路后续/上级决策。
	SamplingDefer SamplingState = iota
	// SamplingAccept 表示该追踪链路被采样（应导出）。
	SamplingAccept
	// SamplingDrop 表示该追踪链路未被采样（不导出）。
	SamplingDrop
)

// String 返回采样状态的可读名称。
func (s SamplingState) String() string {
	switch s {
	case SamplingAccept:
		return "accept"
	case SamplingDrop:
		return "drop"
	default:
		return "defer"
	}
}

// SpanConfig 描述创建一个 Span 时使用的配置。
type SpanConfig struct {
	Parent         Span
	RemoteParent   bool
	RemoteParentID string
	Attributes     map[string]string
	FinalSampling  SamplingState
	PresetTraceID  string
	onEnd          func(SpanRecord) DeliveryResult
	// origin 是整条 Span 链真正的外部派生源 ctx（根 Span 的创建 ctx；
	// 业务组场景即组的可取消 parent ctx），供子 Span 判定 deadline/取消来源。
	origin context.Context
}

// SpanOption 用于在创建 Span 时修改 SpanConfig。
type SpanOption func(*SpanConfig)

// WithParent 显式指定父 Span（通常不需要，StartSpan 会从 ctx 自动读取）。
func WithParent(parent Span) SpanOption {
	return func(c *SpanConfig) { c.Parent = parent }
}

// WithRemoteParentID 标记父级来自进程外，并携带其 SpanID。
func WithRemoteParentID(spanID string) SpanOption {
	return func(c *SpanConfig) {
		c.RemoteParent = true
		c.RemoteParentID = spanID
	}
}

// WithAttribute 为 Span 附加一个字符串属性。
func WithAttribute(key, value string) SpanOption {
	return func(c *SpanConfig) {
		if c.Attributes == nil {
			c.Attributes = map[string]string{}
		}
		c.Attributes[key] = value
	}
}

// WithFinalSampling 由装配层在采样决策完成后注入最终采样状态。
func WithFinalSampling(state SamplingState) SpanOption {
	return func(c *SpanConfig) { c.FinalSampling = state }
}

// WithTraceID 预指定根 Span 的 TraceID（供装配层在采样决策后使用）。
func WithTraceID(traceID string) SpanOption {
	return func(c *SpanConfig) { c.PresetTraceID = traceID }
}

// WithOnEnd 注册 Span 结束回调（装配层用它把记录投递到导出器）。
// 返回值描述本次投递的下游结论，供分组观察者统计覆盖情况；
// 直接使用 trace 包时可返回零值 DeliveryResult（按 unknown 处理）。
func WithOnEnd(fn func(SpanRecord) DeliveryResult) SpanOption {
	return func(c *SpanConfig) { c.onEnd = fn }
}
