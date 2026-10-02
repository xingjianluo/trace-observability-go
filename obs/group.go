package obs

import (
	"context"
	"encoding/json"

	"github.com/xingjianluo/trace-observability-go/export"
	"github.com/xingjianluo/trace-observability-go/group"
	"github.com/xingjianluo/trace-observability-go/trace"
)

// pipelineStarter 让 group 用本管线创建 Span（带采样决策与导出回调）。
type pipelineStarter struct{ p *Pipeline }

// StartSpan 实现 group.SpanStarter。
func (s pipelineStarter) StartSpan(ctx context.Context, name string, forced bool,
	opts ...trace.SpanOption) (context.Context, trace.Span) {
	return s.p.StartSpan(ctx, name, forced, opts...)
}

// StartGroup 在 parent 下创建一个接入本管线的业务组。
//
// 组 Span 与成员 Span 都经由管线采样决策；组结束时产出恰好一条
// "group" 导出记录（组属性脱敏后才入队）。成员明细的投递结论
// （accepted/sampled_out/queue_dropped/closed）反映在汇总 Coverage 中。
func (p *Pipeline) StartGroup(parent context.Context, name string,
	opts ...group.Option) *group.Group {
	opts = append([]group.Option{
		group.WithStarter(pipelineStarter{p}),
		group.WithSummaryEmitter(p.emitGroupSummary),
	}, opts...)
	return group.New(parent, name, opts...)
}

// emitGroupSummary 是组汇总出口：脱敏属性、序列化为 GroupView 并投递。
// 汇总也走同一条有界队列，因此与成员明细一致，受采样/背压约束；
// 但只要被入队接受，Shutdown 时同样不丢。
func (p *Pipeline) emitGroupSummary(s group.Summary) {
	// 未被采样的链路：既不导出成员明细，也不导出组汇总，
	// 覆盖统计仍保留在内存返回的 Summary 中供调用方判断。
	if s.SamplingState == trace.SamplingDrop {
		return
	}
	members := make([]export.GroupMemberView, 0, len(s.Members))
	for _, m := range s.Members {
		members = append(members, export.GroupMemberView{
			SpanID:        m.SpanID,
			ParentSpanID:  m.ParentSpanID,
			Name:          m.Name,
			Depth:         m.Depth,
			Outcome:       m.Outcome,
			Status:        m.Status,
			StartSeq:      m.StartSeq,
			DurationNanos: m.DurationNanos,
			Delivery:      m.Delivery,
			Panic:         m.Panic,
		})
	}
	// 组级属性在入队前脱敏；成员属性已在 Span 路径经过同样的规则。
	var attrs map[string]string
	if len(s.Attributes) > 0 && p.masker != nil {
		attrs = p.masker.MaskStringMap(s.Attributes)
	} else {
		attrs = s.Attributes
	}
	v := export.GroupView{
		TraceID:       s.TraceID,
		GroupSpanID:   s.GroupSpanID,
		ParentSpanID:  s.ParentSpanID,
		ParentGroup:   s.ParentGroup,
		Name:          s.Name,
		StartTime:     s.StartTime,
		EndTime:       s.EndTime,
		DurationNanos: s.Duration.Nanoseconds(),
		FailFast:      s.FailFast,
		Stopped:       s.Stopped,
		StopReason:    s.StopReason,
		GroupStatus:   s.GroupStatus,
		EndReason:     s.EndReason,
		OutcomeCounts: s.OutcomeCounts,
		ErrorCount:    s.ErrorCount,
		SuccessCount:  s.SuccessCount,
		CanceledCount: s.CanceledCount,
		Coverage: export.GroupCoverageView{
			Total:        s.Coverage.Total,
			Observed:     s.Coverage.Observed,
			SampledOut:   s.Coverage.SampledOut,
			QueueDropped: s.Coverage.QueueDropped,
			Closed:       s.Coverage.Closed,
			Unknown:      s.Coverage.Unknown,
		},
		Members:    members,
		Attributes: attrs,
	}
	data, err := json.Marshal(v)
	if err != nil {
		return
	}
	_ = p.exporter.EnqueueR(context.Background(), export.Record{
		Kind:     export.KindGroup,
		Data:     data,
		TraceID:  s.TraceID,
		SpanID:   s.GroupSpanID,
		Accepted: true,
	})
}

// 确保 Pipeline 满足 group.SpanStarter 所需方法集的间接装配。
var _ group.SpanStarter = pipelineStarter{}
