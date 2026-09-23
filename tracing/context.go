package tracing

import "context"

type ctxKey struct{}

func withSpan(ctx context.Context, s Span) context.Context { return ctx }

func spanFromContext(ctx context.Context) Span { return nil }
