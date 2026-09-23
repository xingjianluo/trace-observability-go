package tracing

import "context"

type ctxKey struct{}

// withSpan returns a context carrying s.
func withSpan(ctx context.Context, s Span) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, ctxKey{}, s)
}

// spanFromContext returns the span carried by ctx, or nil.
func spanFromContext(ctx context.Context) Span {
	if ctx == nil {
		return nil
	}
	s, _ := ctx.Value(ctxKey{}).(Span)
	return s
}
