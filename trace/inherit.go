package trace

import "context"

// inheritedOptions 承载需要沿 Span 链路自动向下继承的创建选项，
// 例如装配层注入的采样状态与结束回调。Group 等跨协程派生点
// 无法直接拿到父 Span 创建时的选项，因此经由 context 传播。
type inheritedOptions struct {
	finalSampling SamplingState
	onEnd         func(SpanRecord)
	hasOnEnd      bool
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
}
