package group

import (
	"time"

	"github.com/xingjianluo/trace-observability-go/trace"
)

// MemberResult 是一个直接成员的最终结果快照。
type MemberResult struct {
	SpanID        string
	Name          string
	Status        string // ok / error
	EndReason     string
	SamplingState trace.SamplingState
	// MissingReason 非空表示该成员的明细未能纳入导出（采样/队列原因）。
	MissingReason string
}

// Summary 是一个业务组收尾时“有且仅有一条”的组级汇总。
type Summary struct {
	TraceID      string
	GroupSpanID  string
	ParentSpanID string
	Name         string

	StartTime time.Time
	EndTime   time.Time
	Duration  time.Duration

	// MembersTotal 是直接成员数（成员 Span 与嵌套子组各自计 1）。
	MembersTotal int
	// StatusCounts / ReasonCounts 是成员结果分布。
	StatusCounts map[string]int
	ReasonCounts map[string]int
	// Members 按 SpanID 给出每个直接成员的最终结果；MemberSpanIDs 保留登记顺序。
	Members map[string]MemberResult
	// MemberSpanIDs 按登记顺序给出成员 SpanID，用于和明细逐条对齐。
	MemberSpanIDs []string

	// StoppedEarly 表示组是否被提前停掉（外部取消/父级取消/失败联动/超时）。
	StoppedEarly bool
	// StopReason 是组整体的终止原因（completed 时为空）。
	StopReason string

	// MissingCount / MissingReasons 描述“没被纳入统计”的成员数量与原因分布。
	MissingCount   int
	MissingReasons map[string]int

	Depth         int
	SamplingState trace.SamplingState
	// Attributes 是组级自定义属性；装配层导出前必须先脱敏。
	Attributes map[string]any
}
