// Package group 提供“业务组”能力：把一批并发操作（可嵌套子组）
// 作为一个整体管理停止传播、失败策略与组级汇总。
//
// 每个组拥有自己的组 Span；在组上下文中创建的成员 Span 只归属最近的组，
// 成员结果（completed/error/canceled/...）各计一次，组结束时产出
// 恰好一条 GroupSummary。
package group

import (
	"time"

	"github.com/xingjianluo/trace-observability-go/trace"
)

// 成员最终结果分类（与 trace.Span 的 EndReason 同源，另加 panic 细分）。
const (
	OutcomeCompleted        = trace.ReasonCompleted
	OutcomeError            = trace.ReasonError
	OutcomeCanceled         = trace.ReasonCanceled
	OutcomeParentCanceled   = trace.ReasonParentCanceled
	OutcomeDeadlineExceeded = trace.ReasonDeadlineExceeded
)

// 组整体的停止原因（Summary.StopReason）。空串表示自然跑完。
const (
	// StopReasonExternal 外层 context/父 Span 取消或超时，组被动停止。
	StopReasonExternal = "external_cancel"
	// StopReasonStopped 业务显式调用了组的 Stop。
	StopReasonStopped = "stopped"
	// StopReasonFailFast FailFast 模式下某个成员 error/panic，整体提前停。
	StopReasonFailFast = "fail_fast_member_error"
)

// Member 是组汇总中一个直接成员的明细。
type Member struct {
	SpanID        string `json:"span_id"`
	ParentSpanID  string `json:"parent_span_id"`
	Name          string `json:"name"`
	Depth         int    `json:"depth"`
	Outcome       string `json:"outcome"`
	Status        string `json:"status"`
	StartSeq      int    `json:"start_seq"` // 组内登记顺序，保证输出可复现
	DurationNanos int64  `json:"duration_nanos"`
	// Delivery 描述成员明细在下游的纳入情况：
	// accepted / sampled_out / queue_dropped / closed / unknown。
	Delivery string `json:"delivery"`
	Panic    bool   `json:"panic,omitempty"`
}

// Coverage 描述“有多少成员没能纳入统计”及原因。
type Coverage struct {
	// Total 是组的直接成员总数（无论是否采样/是否入队）。
	Total int `json:"total"`
	// Observed 是明细被下游实际接受（accepted）的成员数。
	Observed int `json:"observed"`
	// SampledOut 因链路未被采样而没有明细导出的成员数。
	SampledOut int `json:"sampled_out"`
	// QueueDropped 采样命中但因有界队列背压没能入队的成员数。
	QueueDropped int `json:"queue_dropped"`
	// Closed 下游已关闭（如收尾之后）导致明细未被接受的成员数。
	Closed int `json:"closed"`
	// Unknown 没有装配层报告投递结论的成员数（直接使用 trace 包时）。
	Unknown int `json:"unknown"`
}

// Summary 是一个组结束时产出的唯一一条组级汇总。
type Summary struct {
	TraceID      string        `json:"trace_id"`
	GroupSpanID  string        `json:"group_span_id"`
	ParentSpanID string        `json:"parent_span_id"`
	ParentGroup  string        `json:"parent_group_span_id,omitempty"`
	Name         string        `json:"name"`
	StartTime    time.Time     `json:"start_time"`
	EndTime      time.Time     `json:"end_time"`
	Duration     time.Duration `json:"duration"`
	FailFast     bool          `json:"fail_fast"`
	// Stopped 表示组是否被提前停止（外部取消/显式 Stop/fail-fast）。
	Stopped bool `json:"stopped"`
	// StopReason 为停止原因；自然跑完为空。
	StopReason string `json:"stop_reason,omitempty"`
	// GroupStatus 为组 Span 的状态：ok / error / canceled。
	GroupStatus string `json:"group_status"`
	// EndReason 为组 Span 的终止原因。
	EndReason string `json:"end_reason"`
	// OutcomeCounts 按成员结果分布计数，键为 Outcome* 常量。
	OutcomeCounts map[string]int `json:"outcome_counts"`
	// ErrorCount 为 error/panic 成员数。
	ErrorCount int `json:"error_count"`
	// SuccessCount 为 completed 成员数。
	SuccessCount int `json:"success_count"`
	// CanceledCount 为各类取消/超时成员数。
	CanceledCount int      `json:"canceled_count"`
	Coverage      Coverage `json:"coverage"`
	// Members 为全部直接成员明细，按 StartSeq 排序，每个成员恰好出现一次。
	Members []Member `json:"members"`
	// Attributes 为组级属性（导出前经脱敏处理）。
	Attributes map[string]string `json:"attributes,omitempty"`
	// SamplingState 为组 Span 所在链路的采样结论（accept/drop/defer）。
	// drop 链路不导出任何成员明细，汇总本身也不导出。
	SamplingState trace.SamplingState `json:"-"`
}
