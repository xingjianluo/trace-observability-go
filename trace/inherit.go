package trace

import "context"

// inheritedOptions 承载需要沿 Span 链路自动向下继承的创建选项，
// 例如装配层注入的采样状态、结束回调与外部派生源 ctx。Group 等跨协程
// 派生点无法直接拿到父 Span 创建时的选项，因此经由 context 传播。
//
// 注意：Span 观察者不放在这里。观察者表示“最近一个业务组”，
// 必须独立于单个 Span 的创建配置沿 context 透传，嵌套组在自己的
// 上下文里覆盖它，组 Span 之外的后代始终报告给最近的组。
type inheritedOptions struct {
	finalSampling SamplingState
	onEnd         func(SpanRecord) DeliveryResult
	hasOnEnd      bool
	origin        context.Context
}

type inheritKey struct{}

func contextWithInherit(ctx context.Context, io inheritedOptions) context.Context {
	return context.WithValue(ctx, inheritKey{}, io)
}

func inheritFromContext(ctx context.Context) (inheritedOptions, bool) {
	v, ok := ctx.Value(inheritKey{}).(inheritedOptions)
	return v, ok
}

// applyInherited 把 ctx 中继承的选项应用到 cfg（已显式设置的不覆盖）。
func applyInherited(ctx context.Context, cfg *SpanConfig) {
	io, ok := inheritFromContext(ctx)
	if !ok {
		return
	}
	if cfg.FinalSampling == SamplingDefer {
		cfg.FinalSampling = io.finalSampling
	}
	if cfg.onEnd == nil && io.hasOnEnd {
		cfg.onEnd = io.onEnd
	}
	if cfg.origin == nil {
		cfg.origin = io.origin
	}
}
