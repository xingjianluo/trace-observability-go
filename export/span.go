package export

import "time"

// SpanView 是导出层对 SpanRecord 的稳定 JSON 视图，
// 避免 export 包反向依赖 trace 包。
type SpanView struct {
	TraceID       string            `json:"trace_id"`
	SpanID        string            `json:"span_id"`
	ParentSpanID  string            `json:"parent_span_id"`
	Name          string            `json:"name"`
	StartTime     time.Time         `json:"start_time"`
	EndTime       time.Time         `json:"end_time"`
	DurationNanos int64             `json:"duration_nanos"`
	Attributes    map[string]string `json:"attributes,omitempty"`
	Status        string            `json:"status"`
	EndReason     string            `json:"end_reason"`
	Depth         int               `json:"depth"`
	RemoteParent  bool              `json:"remote_parent"`
	SamplingState string            `json:"sampling_state"`
}

// StatsView 用于把计数本身作为一条可导出的观测记录。
type StatsView struct {
	Enqueued   uint64 `json:"enqueued"`
	DroppedNew uint64 `json:"dropped_new"`
	DroppedOld uint64 `json:"dropped_old"`
	Exported   uint64 `json:"exported"`
	Batches    uint64 `json:"batches"`
}
