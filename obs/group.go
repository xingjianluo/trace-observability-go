package obs

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/xingjianluo/trace-observability-go/export"
	"github.com/xingjianluo/trace-observability-go/group"
	"github.com/xingjianluo/trace-observability-go/logging"
	"github.com/xingjianluo/trace-observability-go/trace"
)

// groupSinks 是装配层对 group.DeliverySink / group.SummarySink 的实现：
//   - 成员 Span 仍由管线统一的 onSpanEnd 导出，组只通过回执“旁观”投递结论，
//     同一条成员明细绝不会被导出两次；
//   - 组级汇总在这里脱敏后作为一条 group_summary 记录进入同一条有界队列，
//     因此正常 Shutdown 时与成员明细一起被排空，不会丢失。
type groupSinks struct {
	exporter *export.Exporter
	masker   *logging.Masker // nil 表示不脱敏

	mu       sync.Mutex
	pending  map[string]struct{}       // 已被 Expect 认领、尚未见到投递结论
	receipts map[string]group.Delivery // 已有投递结论、等待组取走
}

func newGroupSinks(exporter *export.Exporter, masker *logging.Masker) *groupSinks {
	return &groupSinks{
		exporter: exporter,
		masker:   masker,
		pending:  map[string]struct{}{},
		receipts: map[string]group.Delivery{},
	}
}

// Expect 登记：该 SpanID 的投递回执有人（某个业务组）认领。
// 注意只登记“认领”而不预填结论：若 Span 在 Expect 之前就已经结束
// （投递结论先于认领出现），TakeDelivery 会如实返回“无回执”，
// 由组按采样状态兜底判定，而不是乐观地算作已纳入。
func (s *groupSinks) Expect(spanID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.receipts[spanID]; !ok {
		s.pending[spanID] = struct{}{}
	}
}

// TakeDelivery 取走并删除一次性回执；组保证每个成员只取一次。
func (s *groupSinks) TakeDelivery(spanID string) (group.Delivery, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if d, ok := s.receipts[spanID]; ok {
		delete(s.receipts, spanID)
		return d, true
	}
	// 认领过但投递结论先于认领出现（或 Span 未经过管线投递）：
	// 清除认领标记并如实告知“无回执”。
	delete(s.pending, spanID)
	return group.Delivery{}, false
}

// recordDelivery 由 onSpanEnd 在每条 Span 投递后调用：
// 只有被 Expect 认领的 SpanID 才会留下回执，未认领的不占内存。
func (s *groupSinks) recordDelivery(spanID string, d group.Delivery) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.pending[spanID]; ok {
		delete(s.pending, spanID)
		s.receipts[spanID] = d
	}
}

// DeliverSummary 接收组收尾时唯一的一条组级汇总：
// 先按现有规则脱敏组级属性，再无条件进入有界队列——
// 汇总同时承担“覆盖统计”职责，即使整条链路未被采样也要导出，
// 否则采样丢弃的成员就无处对账。汇总通过 SubmitReliable 阻塞投递，
// 队列积压时等待空位而不是被丢弃（否则覆盖统计本身会丢失）。
func (s *groupSinks) DeliverSummary(sum group.Summary) {
	attrs := sum.Attributes
	if s.masker != nil && attrs != nil {
		attrs = s.masker.MaskFields(attrs)
	}
	// 成员明细按登记顺序展开，使组级结果能与每个成员的导出明细逐条对上号。
	members := make([]export.GroupMemberView, 0, len(sum.MemberSpanIDs))
	for _, id := range sum.MemberSpanIDs {
		mr := sum.Members[id]
		members = append(members, export.GroupMemberView{
			SpanID:        mr.SpanID,
			Name:          mr.Name,
			Status:        mr.Status,
			EndReason:     mr.EndReason,
			MissingReason: mr.MissingReason,
		})
	}
	view := export.GroupSummaryView{
		TraceID:        sum.TraceID,
		GroupSpanID:    sum.GroupSpanID,
		ParentSpanID:   sum.ParentSpanID,
		Name:           sum.Name,
		StartTime:      sum.StartTime,
		EndTime:        sum.EndTime,
		DurationNanos:  sum.Duration.Nanoseconds(),
		MembersTotal:   sum.MembersTotal,
		StatusCounts:   sum.StatusCounts,
		ReasonCounts:   sum.ReasonCounts,
		Members:        members,
		StoppedEarly:   sum.StoppedEarly,
		StopReason:     sum.StopReason,
		MissingCount:   sum.MissingCount,
		MissingReasons: sum.MissingReasons,
		Depth:          sum.Depth,
		SamplingState:  samplingStateName(sum.SamplingState),
		Attributes:     attrs,
	}
	data, err := json.Marshal(view)
	if err != nil {
		return
	}
	_ = s.exporter.SubmitReliable(context.Background(), export.Record{
		Kind:     export.KindGroup,
		Data:     data,
		TraceID:  sum.TraceID,
		SpanID:   sum.GroupSpanID,
		Accepted: true,
	})
}

// deliveryFor 把一条成员 Span 的投递结论翻译成组能理解的回执。
func deliveryFor(rec trace.SpanRecord, res export.Result) group.Delivery {
	if rec.SamplingState != trace.SamplingAccept {
		return group.Delivery{Accepted: false, MissingReason: group.MissingSampledOut}
	}
	if !res.Accepted {
		return group.Delivery{Accepted: false, MissingReason: res.MissingReason}
	}
	return group.Delivery{Accepted: true}
}
