package group

import (
	"sort"

	"github.com/xingjianluo/trace-observability-go/trace"
)

// OnSpanStart 实现 trace.SpanObserver：成员 Span 创建时登记为直接成员。
//
// 名额（slots）只作为 Wait 的等待判据，分两类：
//   - Go 协程槽 / 子组槽：由 reserve() 在派生调用方协程同步预占，
//     覆盖整个 fn 执行期（或子组等待期），因此这里不再增减 slots；
//   - 未预占的内部 Span：成员 fn 内直接 StartSpan 出的后代，没有协程槽，
//     这里 +1 个 slot，OnSpanEnd 时 -1。
//
// 因为 Go 协程槽直到 fn 返回才释放，成员自身 Span 的 End 与内部 Span 的
// Start 之间不可能出现 slots=0 的窗口，Wait 不会提前归零。
func (g *Group) OnSpanStart(s trace.Span) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return
	}
	if _, exists := g.seen[s.SpanID()]; exists {
		return
	}
	g.seen[s.SpanID()] = false
	g.members = append(g.members, Member{
		SpanID:       s.SpanID(),
		ParentSpanID: s.ParentSpanID(),
		Name:         s.Name(),
		Depth:        s.Depth(),
		StartSeq:     g.seq,
	})
	g.seq++
	// 只有“未被 reserve 预占”的内部 Span 才需要自己占一个 slot。
	// Go/子组成员 Span 已由协程槽/子组槽覆盖（slots 不动），并记入 reserved。
	if g.pending > 0 {
		g.pending--
		g.reserved[s.SpanID()] = struct{}{}
	} else {
		g.slots++ // 内部 Span 自己占一个 slot
	}
}

// OnSpanEnd 实现 trace.SpanObserver：成员结束时恰好记录一次最终结果。
// Span.End 幂等，保证本回调对每个成员只触发一次：成员不会既算正常完成
// 又算被停，停止与超时几乎同时发生时也以第一次结论为准。
func (g *Group) OnSpanEnd(rec trace.SpanRecord, delivery trace.DeliveryResult) {
	outcome := rec.EndReason
	isPanic := rec.Attributes["panic"] != ""
	if isPanic && outcome != trace.ReasonError {
		outcome = trace.ReasonError
	}

	g.mu.Lock()
	ended, exists := g.seen[rec.SpanID]
	if !exists {
		// 防御：开始必须先于结束。兜底登记为内部 Span。
		g.seen[rec.SpanID] = false
		g.members = append(g.members, Member{
			SpanID:       rec.SpanID,
			ParentSpanID: rec.ParentSpanID,
			Name:         rec.Name,
			Depth:        rec.Depth,
			StartSeq:     g.seq,
		})
		g.seq++
		g.slots++
		ended = false
	}
	if ended {
		g.mu.Unlock()
		return // 防御重复结束：只统计第一次
	}
	idx := -1
	for i := range g.members {
		if g.members[i].SpanID == rec.SpanID {
			idx = i
			break
		}
	}
	g.seen[rec.SpanID] = true
	if idx >= 0 {
		m := g.members[idx]
		m.Outcome = outcome
		m.Status = rec.Status
		m.Delivery = delivery.Outcome
		m.DurationNanos = rec.EndTime.Sub(rec.StartTime).Nanoseconds()
		m.Panic = isPanic
		g.members[idx] = m
	}
	// 预占成员（Go 协程/子组）的 slot 由协程/子组槽统一释放，不在 End 释放；
	// 只有未预占的内部 Span 在这里释放自己 OnSpanStart 时占用的 slot。
	if _, ok := g.reserved[rec.SpanID]; !ok {
		if g.slots > 0 {
			g.slots--
		}
	}
	failFast := g.cfg.FailFast && outcome == trace.ReasonError
	g.cond.Broadcast()
	g.mu.Unlock()

	if failFast {
		g.markFailFast()
	}
}

// sortMembers 按 StartSeq 升序排列成员（登记即编号，输出天然可复现）。
func sortMembers(ms []Member) {
	sort.SliceStable(ms, func(i, j int) bool { return ms[i].StartSeq < ms[j].StartSeq })
}
