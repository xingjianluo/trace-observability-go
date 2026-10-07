package group_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xingjianluo/trace-observability-go/group"
	"github.com/xingjianluo/trace-observability-go/trace"
)

// manualSink 是装配层投递回执的测试替身：
// 记录每个被登记成员的最终投递结论，验证组如何区分
// “实际纳入”与“因采样/队列原因未能纳入”。
type manualSink struct {
	mu        sync.Mutex
	delivered map[string]group.Delivery
	expectN   int
}

func newManualSink() *manualSink { return &manualSink{delivered: map[string]group.Delivery{}} }

func (m *manualSink) Expect(spanID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expectN++
	if _, ok := m.delivered[spanID]; !ok {
		m.delivered[spanID] = group.Delivery{Accepted: true}
	}
}

func (m *manualSink) fail(spanID string, d group.Delivery) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.delivered[spanID] = d
}

func (m *manualSink) TakeDelivery(spanID string) (group.Delivery, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.delivered[spanID]
	if ok {
		delete(m.delivered, spanID)
	}
	return d, ok
}

// discardSink 是组级汇总的测试替身。
type discardSink struct{ n atomic.Int32 }

func (d *discardSink) DeliverSummary(s group.Summary) { d.n.Add(1) }

func newCtx(sink group.DeliverySink, sum group.SummarySink) context.Context {
	ctx := context.Background()
	trace.SetIDSource(trace.NewCounterIDSource())
	return group.ContextWithSinks(ctx, sink, sum)
}

func failCheck(t *testing.T, name string, in, got, want any) {
	t.Helper()
	t.Errorf("[%s] 判定失败\n  输入:   %#v\n  实际:   %#v\n  期望:   %#v\n  判定标准: %s", name, in, got, want, name)
}

func TestNestedGroupsAttributionAndWait(t *testing.T) {
	sink := newManualSink()
	sum := &discardSink{}
	g := group.New(newCtx(sink, sum), group.WithName("outer"))

	var innerRan, leafRan atomic.Int32
	sub := g.Sub(group.WithName("inner"))
	g.Go("outer-worker", func(ctx context.Context, s trace.Span) {
		innerRan.Add(1)
	})
	// 内层组里再扇出，并在成员里再嵌套一个子组，覆盖多层嵌套。
	sub.Go("inner-worker", func(ctx context.Context, s trace.Span) {
		leaf := group.New(ctx).Sub() // 自动挂到 inner 下
		leaf.Go("leaf", func(ctx context.Context, s trace.Span) { leafRan.Add(1) })
		leaf.Wait()
	})

	innerSum := sub.Wait()
	outerSum := g.Wait()

	if innerRan.Load() != 1 || leafRan.Load() != 1 {
		failCheck(t, "members ran", "nested fanout",
			[]int32{innerRan.Load(), leafRan.Load()}, []int32{1, 1})
	}
	// 内层：inner-worker + leaf（嵌套子组自动登记）= 2 个直接成员。
	if innerSum.MembersTotal != 2 {
		failCheck(t, "inner members", "1 worker + 1 nested subgroup",
			innerSum.MembersTotal, 2)
	}
	// 外层：outer-worker + inner 子组 = 2 个直接成员。
	if outerSum.MembersTotal != 2 {
		failCheck(t, "outer members", "1 worker + 1 subgroup",
			outerSum.MembersTotal, 2)
	}
	if outerSum.TraceID != innerSum.TraceID {
		failCheck(t, "trace id aligned", "outer vs inner",
			[]string{outerSum.TraceID, innerSum.TraceID}, "same trace id")
	}
	if outerSum.StopReason != trace.ReasonCompleted || innerSum.StopReason != trace.ReasonCompleted {
		failCheck(t, "normal completion", "no cancellation",
			[]string{outerSum.StopReason, innerSum.StopReason}, []string{"completed", "completed"})
	}
	// 每条成员明细都必须能按 ID 在汇总里找到归属，且不重复。
	seen := map[string]bool{}
	for _, id := range outerSum.MemberSpanIDs {
		if seen[id] {
			t.Errorf("外层成员重复计入: %s", id)
		}
		seen[id] = true
	}
}

func TestFanOutStoppedFromOutside(t *testing.T) {
	sink := newManualSink()
	g := group.New(newCtx(sink, &discardSink{}), group.WithName("stop"))

	var received atomic.Int32
	const n = 16
	for i := 0; i < n; i++ {
		g.Go("worker", func(ctx context.Context, s trace.Span) {
			<-ctx.Done() // 模拟等待工作
			received.Add(1)
		})
	}
	time.Sleep(30 * time.Millisecond)
	// 从最外层发起停止；也覆盖“中间层/成员调用方发起”的语义：同一条组 ctx 取消。
	g.Stop()
	sum := g.Wait()

	if received.Load() != n {
		failCheck(t, "all members observe stop", "cancel from outside",
			received.Load(), n)
	}
	if sum.MembersTotal != n {
		failCheck(t, "member count", n, sum.MembersTotal, n)
	}
	if !sum.StoppedEarly || sum.StopReason != trace.ReasonCanceled {
		failCheck(t, "group stopped early (self)", "g.Stop()",
			[]any{sum.StoppedEarly, sum.StopReason}, []any{true, "canceled"})
	}
	// 每个成员都必须记下自己是因为什么停下来的，且只能算一次。
	canceled := sum.ReasonCounts[trace.ReasonParentCanceled]
	if canceled != n {
		failCheck(t, "members parent_canceled", "group canceled while members blocked",
			canceled, n)
	}
	for _, id := range sum.MemberSpanIDs {
		mr := sum.Members[id]
		if mr.EndReason != trace.ReasonParentCanceled {
			t.Errorf("成员 %s 停止原因错误: %s", id, mr.EndReason)
		}
	}
}

func TestParentCancelPropagatesToNestedGroups(t *testing.T) {
	sink := newManualSink()
	pctx, cancel := context.WithCancel(context.Background())
	ctx := group.ContextWithSinks(pctx, sink, &discardSink{})
	g := group.New(ctx)
	sub := g.Sub()
	var got atomic.Int32
	sub.Go("blocked", func(ctx context.Context, s trace.Span) {
		<-ctx.Done()
		got.Add(1)
	})
	time.Sleep(30 * time.Millisecond)
	cancel() // 从更外层（组之外）取消
	subSum := sub.Wait()
	sum := g.Wait()
	if got.Load() != 1 {
		failCheck(t, "nested member got signal", "cancel outer context", got.Load(), 1)
	}
	if sum.StopReason != trace.ReasonParentCanceled {
		failCheck(t, "outer group reason", "outer ctx canceled", sum.StopReason, trace.ReasonParentCanceled)
	}
	if subSum.StopReason != trace.ReasonParentCanceled {
		failCheck(t, "inner group reason", "parent group ctx canceled", subSum.StopReason, trace.ReasonParentCanceled)
	}
}

// noReceiptSink 模拟“认领后仍拿不到回执”的装配层
// （例如 Span 在 Expect 之前就已结束、投递结论先于认领出现）。
type noReceiptSink struct{}

func (noReceiptSink) Expect(string) {}
func (noReceiptSink) TakeDelivery(string) (group.Delivery, bool) {
	return group.Delivery{}, false
}

// 覆盖：成员并发结束、重复结束、panic、显式 error 时，
// 组级汇总必须恰好产出一次，且每个成员只被统计一次。
func TestSummaryExactlyOnceWithConcurrentAndDuplicateEnds(t *testing.T) {
	sink := newManualSink()
	sum := &discardSink{}
	g := group.New(newCtx(sink, sum), group.WithName("once"))

	const workers = 64
	for i := 0; i < workers; i++ {
		g.Go("worker", func(ctx context.Context, s trace.Span) {
			if i%3 == 0 {
				// 重复结束：只有第一次生效，不得重复计入、不得改判。
				s.End("")
				s.End(trace.ReasonError)
			}
		})
	}
	g.Go("panicker", func(ctx context.Context, s trace.Span) { panic("boom") })
	g.Go("self-error", func(ctx context.Context, s trace.Span) { s.End(trace.ReasonError) })

	// 两个协程并发 Wait：汇总仍只产出一次，且两次拿到同一份结论。
	var wg sync.WaitGroup
	var s1, s2 group.Summary
	wg.Add(2)
	go func() { defer wg.Done(); s1 = g.Wait() }()
	go func() { defer wg.Done(); s2 = g.Wait() }()
	wg.Wait()

	if sum.n.Load() != 1 {
		failCheck(t, "summary delivered exactly once", "concurrent Wait x2 + 66 members",
			sum.n.Load(), 1)
	}
	const total = workers + 2
	if s1.MembersTotal != total || s2.MembersTotal != total {
		failCheck(t, "member total", "64 workers + panicker + self-error",
			[]int{s1.MembersTotal, s2.MembersTotal}, total)
	}
	if len(s1.Members) != total || len(s1.MemberSpanIDs) != total {
		failCheck(t, "no duplicate members", total,
			[]int{len(s1.Members), len(s1.MemberSpanIDs)}, total)
	}
	// panic 与显式 error 各 1 个；重复结束的第二次 error 不得生效。
	if s1.StatusCounts["error"] != 2 {
		failCheck(t, "error members counted once", "1 panic + 1 explicit error + duplicate End",
			s1.StatusCounts["error"], 2)
	}
	if s1.StatusCounts["ok"] != total-2 {
		failCheck(t, "ok members", "remaining workers",
			s1.StatusCounts["ok"], total-2)
	}
	// 每个成员明细都能按 ID 对上号，且无重复、无遗漏。
	seen := map[string]bool{}
	for _, id := range s1.MemberSpanIDs {
		if seen[id] {
			t.Errorf("成员重复计入: %s", id)
		}
		seen[id] = true
		if _, ok := s1.Members[id]; !ok {
			t.Errorf("成员 %s 在 Members 中找不到归属", id)
		}
	}
}

// 覆盖：低采样 / 队列积压时，组级汇总必须分清“实际纳入”与
// “未能纳入”的成员，并按 ID 给出每个未纳入成员的具体原因。
func TestCoverageStatsDistinguishMissingReasons(t *testing.T) {
	sink := newManualSink()
	sum := &discardSink{}
	g := group.New(newCtx(sink, sum), group.WithName("coverage"))

	// 用 Track 登记手工创建的 Span，便于在结束前指定投递结论。
	mk := func(name string) trace.Span {
		_, s := trace.StartSpan(g.Context(), name)
		g.Track(s)
		return s
	}
	ok1 := mk("ok-1")
	ok2 := mk("ok-2")
	droppedNew := mk("dropped-new")
	sampledOut := mk("sampled-out")

	sink.fail(droppedNew.SpanID(), group.Delivery{Accepted: false, MissingReason: group.MissingQueueDroppedNew})
	sink.fail(sampledOut.SpanID(), group.Delivery{Accepted: false, MissingReason: group.MissingSampledOut})

	ok1.End("")
	droppedNew.End("")
	ok2.End("")
	sampledOut.End("")

	res := g.Wait()
	if res.MembersTotal != 4 {
		failCheck(t, "member total", "2 ok + 1 queue-dropped + 1 sampled-out",
			res.MembersTotal, 4)
	}
	if res.MissingCount != 2 {
		failCheck(t, "missing count", "2 of 4 not delivered", res.MissingCount, 2)
	}
	if res.MissingReasons[group.MissingQueueDroppedNew] != 1 {
		failCheck(t, "queue_dropped_new reason", "queue full on one member",
			res.MissingReasons[group.MissingQueueDroppedNew], 1)
	}
	if res.MissingReasons[group.MissingSampledOut] != 1 {
		failCheck(t, "sampled_out reason", "trace not sampled on one member",
			res.MissingReasons[group.MissingSampledOut], 1)
	}
	// 每个未纳入成员都能按 ID 查到具体原因；已纳入成员原因为空。
	if mr := res.Members[droppedNew.SpanID()]; mr.MissingReason != group.MissingQueueDroppedNew {
		failCheck(t, "per-member missing reason (queue)", droppedNew.SpanID(),
			mr.MissingReason, group.MissingQueueDroppedNew)
	}
	if mr := res.Members[sampledOut.SpanID()]; mr.MissingReason != group.MissingSampledOut {
		failCheck(t, "per-member missing reason (sampling)", sampledOut.SpanID(),
			mr.MissingReason, group.MissingSampledOut)
	}
	if mr := res.Members[ok1.SpanID()]; mr.MissingReason != "" {
		failCheck(t, "delivered member has no missing reason", ok1.SpanID(),
			mr.MissingReason, "")
	}
}

// 覆盖：装配层拿不出投递回执时（如登记前 Span 已结束），
// 未采样链路的成员必须按 sampled_out 如实记账，不能静默算作已纳入。
func TestMissingReceiptFallsBackToSamplingState(t *testing.T) {
	sum := &discardSink{}
	trace.SetIDSource(trace.NewCounterIDSource())
	ctx := group.ContextWithSinks(context.Background(), noReceiptSink{}, sum)
	g := group.New(ctx, group.WithName("fallback"))

	_, s := trace.StartSpan(g.Context(), "unsampled", trace.WithFinalSampling(trace.SamplingDrop))
	g.Track(s)
	s.End("")

	res := g.Wait()
	if res.MissingCount != 1 || res.MissingReasons[group.MissingSampledOut] != 1 {
		failCheck(t, "sampled_out fallback without receipt",
			"member on dropped trace, no delivery receipt",
			[]any{res.MissingCount, res.MissingReasons}, "1 x sampled_out")
	}
}
