package export

import "time"

// GroupCoverageView 是组汇总覆盖统计的稳定 JSON 视图。
type GroupCoverageView struct {
	Total        int `json:"total"`
	Observed     int `json:"observed"`
	SampledOut   int `json:"sampled_out"`
	QueueDropped int `json:"queue_dropped"`
	Closed       int `json:"closed"`
	Unknown      int `json:"unknown"`
}

// GroupMemberView 是组汇总中单个直接成员的 JSON 视图。
type GroupMemberView struct {
	SpanID        string `json:"span_id"`
	ParentSpanID  string `json:"parent_span_id"`
	Name          string `json:"name"`
	Depth         int    `json:"depth"`
	Outcome       string `json:"outcome"`
	Status        string `json:"status"`
	StartSeq      int    `json:"start_seq"`
	DurationNanos int64  `json:"duration_nanos"`
	Delivery      string `json:"delivery"`
	Panic         bool   `json:"panic,omitempty"`
}

// GroupView 是组级汇总的稳定 JSON 视图（export 包不依赖 group 包）。
type GroupView struct {
	TraceID       string            `json:"trace_id"`
	GroupSpanID   string            `json:"group_span_id"`
	ParentSpanID  string            `json:"parent_span_id"`
	ParentGroup   string            `json:"parent_group_span_id,omitempty"`
	Name          string            `json:"name"`
	StartTime     time.Time         `json:"start_time"`
	EndTime       time.Time         `json:"end_time"`
	DurationNanos int64             `json:"duration_nanos"`
	FailFast      bool              `json:"fail_fast"`
	Stopped       bool              `json:"stopped"`
	StopReason    string            `json:"stop_reason,omitempty"`
	GroupStatus   string            `json:"group_status"`
	EndReason     string            `json:"end_reason"`
	OutcomeCounts map[string]int    `json:"outcome_counts"`
	ErrorCount    int               `json:"error_count"`
	SuccessCount  int               `json:"success_count"`
	CanceledCount int               `json:"canceled_count"`
	Coverage      GroupCoverageView `json:"coverage"`
	Members       []GroupMemberView `json:"members"`
	Attributes    map[string]string `json:"attributes,omitempty"`
}
