package export

import "time"

// GroupSummaryView 是业务组汇总记录的稳定 JSON 视图，
// 与 Span/Log 一样作为一条记录进入导出管线。
type GroupSummaryView struct {
	TraceID         string            `json:"trace_id"`
	GroupSpanID     string            `json:"group_span_id"`
	ParentSpanID    string            `json:"parent_span_id"`
	Name            string            `json:"name"`
	StartTime       time.Time         `json:"start_time"`
	EndTime         time.Time         `json:"end_time"`
	DurationNanos   int64             `json:"duration_nanos"`
	MembersTotal    int               `json:"members_total"`
	StatusCounts    map[string]int    `json:"status_counts"`
	ReasonCounts    map[string]int    `json:"reason_counts"`
	StoppedEarly    bool              `json:"stopped_early"`
	StopReason      string            `json:"stop_reason,omitempty"`
	MissingCount    int               `json:"missing_count"`
	MissingReasons  map[string]int    `json:"missing_reasons"`
	Depth           int               `json:"depth"`
	SamplingState   string            `json:"sampling_state"`
	Attributes      map[string]any    `json:"attributes,omitempty"`
}
