package trace

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// check 是统一判定辅助：失败时打印输入、实际结果与判定依据。
func check(t *testing.T, name string, in any, got any, want any, ok bool) {
	t.Helper()
	if !ok {
		t.Errorf("[%s] 判定失败\n  输入:   %#v\n  实际:   %#v\n  期望:   %#v\n  判定依据: %s",
			name, in, got, want, name)
	}
}

func TestStartSpanHierarchy(t *testing.T) {
	SetIDSource(NewCounterIDSource())
	ctx, root := StartSpan(context.Background(), "root")
	ctx1, child := StartSpan(ctx, "child")
	_, grand := StartSpan(ctx1, "grand")

	check(t, "trace-id shared", "root trace chain", grand.TraceID(), root.TraceID(),
		grand.TraceID() == root.TraceID())
	check(t, "child parent", "child.parent==root", child.ParentSpanID(), root.SpanID(),
		child.ParentSpanID() == root.SpanID())
	check(t, "grand parent", "grand.parent==child", grand.ParentSpanID(), child.SpanID(),
		grand.ParentSpanID() == child.SpanID())
	check(t, "depth", "root=0,child=1,grand=2",
		[]int{root.Depth(), child.Depth(), grand.Depth()}, []int{0, 1, 2},
		root.Depth() == 0 && child.Depth() == 1 && grand.Depth() == 2)
	check(t, "unique span ids", "root,child,grand ids",
		[]string{root.SpanID(), child.SpanID(), grand.SpanID()}, 3,
		len(map[string]bool{root.SpanID(): true, child.SpanID(): true, grand.SpanID(): true}) == 3)
}

func TestSpanEndIsIdempotent(t *testing.T) {
	SetIDSource(NewCounterIDSource())
	var ends atomic.Int32
	ctx, s := StartSpan(context.Background(), "s", WithOnEnd(func(SpanRecord) { ends.Add(1) }))
	_ = ctx
	s.End("")
	s.End("")
	s.End(ReasonError)
	check(t, "end once", "End called 3 times", ends.Load(), int32(1), ends.Load() == 1)
	check(t, "first reason wins", "first=completed, later=error", s.Record().EndReason, ReasonCompleted,
		s.Record().EndReason == ReasonCompleted)
	check(t, "ended flag", "Ended()", s.Ended(), true, s.Ended())
}

func TestGroupConcurrentChildrenAttribution(t *testing.T) {
	SetIDSource(NewCounterIDSource())
	const n = 64
	var records []SpanRecord
	var mu sync.Mutex

	ctx, root := StartSpan(context.Background(), "root",
		WithOnEnd(func(r SpanRecord) { mu.Lock(); records = append(records, r); mu.Unlock() }))
	g := NewGroup(ctx)
	for i := 0; i < n; i++ {
		g.Go("child", func(ctx context.Context, s Span) {
			// 从 ctx 取回的必须是自己（而不是兄弟或父级）。
			cur := SpanFromContext(ctx)
			check(t, "ctx span is self", "SpanFromContext inside child",
				cur.SpanID(), s.SpanID(), cur.SpanID() == s.SpanID())
			time.Sleep(time.Millisecond)
		})
	}
	g.Wait()
	root.End("")

	children := 0
	for _, r := range records {
		if r.Name == "child" {
			children++
			check(t, "child trace", "child.TraceID==root.TraceID",
				[]string{r.TraceID, r.ParentSpanID}, []string{root.TraceID(), root.SpanID()},
				r.TraceID == root.TraceID() && r.ParentSpanID == root.SpanID())
			check(t, "child completed", "normal child end reason",
				r.EndReason, ReasonCompleted, r.EndReason == ReasonCompleted)
		}
	}
	check(t, "child count", "64 goroutines, no duplicates/orphans",
		children, n, children == n)
}

func TestGroupParentCancellation(t *testing.T) {
	SetIDSource(NewCounterIDSource())
	var mu sync.Mutex
	var reasons []string

	pctx, cancel := context.WithCancel(context.Background())
	ctx, root := StartSpan(pctx, "root",
		WithOnEnd(func(r SpanRecord) {
			mu.Lock()
			reasons = append(reasons, r.Name+":"+r.EndReason)
			mu.Unlock()
		}))
	g := NewGroup(ctx)
	g.Go("blocked-child", func(ctx context.Context, s Span) {
		<-ctx.Done() // 等待取消传播
	})
	time.Sleep(20 * time.Millisecond)
	cancel()
	g.Wait()
	root.End("")

	joined := strings.Join(reasons, ",")
	check(t, "child parent_canceled", "reasons after cancel()",
		joined, "blocked-child:parent_canceled",
		strings.Contains(joined, "blocked-child:parent_canceled"))
}

func TestChildCancellationNotMarkedParent(t *testing.T) {
	SetIDSource(NewCounterIDSource())
	ctx, root := StartSpan(context.Background(), "root")
	sctx, child := StartSpan(ctx, "child")
	_ = sctx
	child.Cancel()
	child.End("")
	check(t, "self cancel reason", "child canceled itself while root alive",
		child.Record().EndReason, ReasonCanceled, child.Record().EndReason == ReasonCanceled)
	root.End("")
	check(t, "root still completed", "root ctx unaffected by child cancel",
		root.Record().EndReason, ReasonCompleted, root.Record().EndReason == ReasonCompleted)
}

func TestDeadlineExceededReason(t *testing.T) {
	SetIDSource(NewCounterIDSource())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	_, s := StartSpan(ctx, "slow")
	time.Sleep(30 * time.Millisecond)
	s.End("")
	check(t, "deadline reason", "context deadline exceeded",
		s.Record().EndReason, ReasonDeadlineExceeded, s.Record().EndReason == ReasonDeadlineExceeded)
}

func TestInjectExtractRoundTrip(t *testing.T) {
	SetIDSource(NewCounterIDSource())
	ctx, root := StartSpan(context.Background(), "root", WithFinalSampling(SamplingAccept))
	carrier := &MapCarrier{}
	Inject(ctx, carrier)

	restored := Extract(context.Background(), carrier)
	s := SpanFromContext(restored)
	in := map[string]string{"trace": root.TraceID(), "span": root.SpanID()}
	got := map[string]string{"trace": s.TraceID(), "span": s.SpanID(), "state": stateName(s.SamplingState())}
	check(t, "restore trace", "carrier headers", got["trace"], in["trace"], got["trace"] == in["trace"])
	check(t, "restore span", "carrier headers", got["span"], in["span"], got["span"] == in["span"])
	check(t, "restore sampled", "sampled=1", got["state"], "accept", got["state"] == "accept")

	// 从远端上下文派生的子 Span 继承 traceID 且 parent 指向远端 span。
	_, child := StartSpan(restored, "local-child", WithFinalSampling(SamplingAccept))
	check(t, "remote child trace", "child trace == remote trace",
		child.TraceID(), root.TraceID(), child.TraceID() == root.TraceID())
	check(t, "remote child parent", "child parent == remote span",
		child.ParentSpanID(), root.SpanID(), child.ParentSpanID() == root.SpanID())
}

func stateName(s SamplingState) string {
	switch s {
	case SamplingAccept:
		return "accept"
	case SamplingDrop:
		return "drop"
	default:
		return "defer"
	}
}
