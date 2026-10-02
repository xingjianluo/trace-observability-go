package group

import (
	"context"

	"github.com/xingjianluo/trace-observability-go/trace"
)

// DefaultSpanStarter 直接使用 trace.StartSpan 创建 Span。
// 不做采样决策与导出，成员明细的 Delivery 为 unknown；
// 汇总仍可通过 Config.Emit 自行收集。
type DefaultSpanStarter struct{}

// StartSpan 实现 SpanStarter。
func (DefaultSpanStarter) StartSpan(ctx context.Context, name string, forced bool, opts ...trace.SpanOption) (context.Context, trace.Span) {
	_ = forced
	return trace.StartSpan(ctx, name, opts...)
}
