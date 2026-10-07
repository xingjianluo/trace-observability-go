package export

import "time"

// GroupMemberView 是组级汇总中单个直接成员的稳定 JSON 视图，
// 使组级结果能与每个成员的明细逐条对上号。
type GroupMemberView struct {
	SpanID    string `json:"span_id"`
	Name      string `json:"name"`
	Status    string `json:"status"`
	EndReason string `json:"end_reason"`
	// MissingReason 非空表示该成员明细未能纳入导出（采样/队列原因）。
	MissingReason string `json:"missing_reason,omitempty"`
}

// GroupSummaryView 是业务组汇总记录的稳定 JSON 视图，
// 与 Span/Log 一样作为一条记录进入导出管线。
type GroupSummaryView struct {
	TraceID        string            `json:"trace_id"`
	GroupSpanID    string            `json:"group_span_id"`
	ParentSpanID   string            `json:"parent_span_id"`
	Name           string            `json:"name"`
	StartTime      time.Time         `json:"start_time"`
	EndTime        time.Time         `json:"end_time"`
	DurationNanos  int64             `json:"duration_nanos"`
	MembersTotal   int               `json:"members_total"`
	StatusCounts   map[string]int    `json:"status_counts"`
	ReasonCounts   map[string]int    `json:"reason_counts"`
	Members        []GroupMemberView `json:"members"`
	StoppedEarly   bool              `json:"stopped_early"`
	StopReason     string            `json:"stop_reason,omitempty"`
	MissingCount   int               `json:"missing_count"`
	MissingReasons map[string]int    `json:"missing_reasons"`
	Depth          int               `json:"depth"`
	SamplingState  string            `json:"sampling_state"`
	Attributes     map[string]any    `json:"attributes,omitempty"`
}
