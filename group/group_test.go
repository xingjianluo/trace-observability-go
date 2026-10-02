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
