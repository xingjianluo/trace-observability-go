package group

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xingjianluo/trace-observability-go/trace"
)

func gcheck(t *testing.T, name string, in, got, want any, ok bool) {
	t.Helper()
	if !ok {
		t.Errorf("[%s] 判定失败\n  输入: %#v\n  实际: %#v\n  期望: %#v\n  判定依据: %s",
			name, in, got, want, name)
	}
}

func TestGroupConcurrentMembersCountedOnce(t *testing.T) {
	trace.SetIDSource(trace.NewCounterIDSource())
	var emits atomic.Int32
	const n = 32
	g := New(context.Background(), "batch",
		WithSummaryEmitter(func(s Summary) { emits.Add(1) }))
	for i := 0; i < n; i++ {
		g.Go("member", func(ctx context.Context, s trace.Span) {
			time.Sleep(time.Millisecond)
			// 成员协程内额外派生的后代 Span 也应被本组统计为直接成员。
			_, child := trace.StartSpan(ctx, "member-child")
			child.End("")
		})
	}
	sum := g.Wait()
	again := g.Wait() // 重复 Wait 返回同一条汇总且不重复产出

	gcheck(t, "members total", "32 Go + 32 inner spans", sum.Coverage.Total, 2*n,
		sum.Coverage.Total == 2*n)
	gcheck(t, "all completed", OutcomeCompleted, sum.OutcomeCounts[OutcomeCompleted], 2*n,
		sum.OutcomeCounts[OutcomeCompleted] == 2*n)
	gcheck(t, "success count", 2*n, sum.SuccessCount, 2*n, sum.SuccessCount == 2*n)
	gcheck(t, "no error", 0, sum.ErrorCount, 0, sum.ErrorCount == 0)
	gcheck(t, "not stopped", false, sum.Stopped, false, !sum.Stopped)
	gcheck(t, "emit once", "Wait twice", emits.Load(), int32(1), emits.Load() == 1)
	gcheck(t, "same summary", "Wait idempotent", again.GroupSpanID, sum.GroupSpanID,
		again.GroupSpanID == sum.GroupSpanID)
	gcheck(t, "members sorted", "StartSeq ascending", len(sum.Members), 2*n, len(sum.Members) == 2*n)
	for i := 1; i < len(sum.Members); i++ {
		if sum.Members[i].StartSeq < sum.Members[i-1].StartSeq {
			t.Fatalf("成员明细未按 StartSeq 排序: %d before %d",
				sum.Members[i].StartSeq, sum.Members[i-1].StartSeq)
		}
	}
	// 每个 spanID 只出现一次。
	seen := map[string]bool{}
	for _, m := range sum.Members {
		if seen[m.SpanID] {
			t.Fatalf("成员被重复计入: %s", m.SpanID)
		}
		seen[m.SpanID] = true
	}
}

func TestGroupExplicitStop(t *testing.T) {
	trace.SetIDSource(trace.NewCounterIDSource())
	g := New(context.Background(), "job")
	g.Go("blocked", func(ctx context.Context, s trace.Span) {
		<-ctx.Done()
	})
	time.Sleep(20 * time.Millisecond)
	g.Stop(StopReasonStopped)
	sum := g.Wait()
	gcheck(t, "stopped", "explicit Stop", sum.Stopped, true, sum.Stopped)
	gcheck(t, "stop reason", StopReasonStopped, sum.StopReason, StopReasonStopped,
		sum.StopReason == StopReasonStopped)
	gcheck(t, "member canceled by parent", OutcomeParentCanceled,
		sum.OutcomeCounts[OutcomeParentCanceled], 1,
		sum.OutcomeCounts[OutcomeParentCanceled] == 1)
	gcheck(t, "group canceled", trace.ReasonCanceled, sum.EndReason, trace.ReasonCanceled,
		sum.EndReason == trace.ReasonCanceled)
}

func TestGroupExternalCancel(t *testing.T) {
	trace.SetIDSource(trace.NewCounterIDSource())
	pctx, cancel := context.WithCancel(context.Background())
	g := New(pctx, "job")
	g.Go("blocked", func(ctx context.Context, s trace.Span) { <-ctx.Done() })
	time.Sleep(20 * time.Millisecond)
	cancel()
	sum := g.Wait()
	gcheck(t, "external stopped", true, sum.Stopped, true, sum.Stopped)
	gcheck(t, "external reason", StopReasonExternal, sum.StopReason, StopReasonExternal,
		sum.StopReason == StopReasonExternal)
	gcheck(t, "group parent_canceled", trace.ReasonParentCanceled, sum.EndReason,
		trace.ReasonParentCanceled, sum.EndReason == trace.ReasonParentCanceled)
}

func TestNestedGroupsAttribution(t *testing.T) {
	trace.SetIDSource(trace.NewCounterIDSource())
	var mu sync.Mutex
	var summaries []Summary
	emit := func(s Summary) {
		mu.Lock()
		summaries = append(summaries, s)
		mu.Unlock()
	}

	root := New(context.Background(), "root", WithSummaryEmitter(emit))
	root.Go("root-member", func(ctx context.Context, s trace.Span) {})

	sub := root.SubGroup("child-group", WithSummaryEmitter(emit))
	sub.Go("sub-member-a", func(ctx context.Context, s trace.Span) {})
	sub.Go("sub-member-b", func(ctx context.Context, s trace.Span) {
		_, inner := trace.StartSpan(ctx, "nested-leaf")
		inner.End("")
	})

	subSum := sub.Wait()
	rootSum := root.Wait()

	// 子组：2 个 g.Go 成员 + 1 个内部 leaf = 3。
	gcheck(t, "sub total", "2 Go + 1 leaf", subSum.Coverage.Total, 3,
		subSum.Coverage.Total == 3)
	gcheck(t, "sub all completed", 3, subSum.SuccessCount, 3, subSum.SuccessCount == 3)
	// 父组：自己的 1 个 root-member + 子组的组 Span = 2 个直接成员。
	gcheck(t, "root total", "1 member + 1 subgroup span", rootSum.Coverage.Total, 2,
		rootSum.Coverage.Total == 2)
	// 归属链：子组挂到父组。
	var subgroupMember *Member
	for i := range rootSum.Members {
		if rootSum.Members[i].Name == "child-group" {
			subgroupMember = &rootSum.Members[i]
		}
	}
	if subgroupMember == nil {
		t.Fatalf("父组汇总缺少子组 Span 成员\n实际成员: %#v\n判定依据: 嵌套子组必须是父组直接成员",
			rootSum.Members)
	}
	gcheck(t, "subgroup parent link", subgroupMember.SpanID, subSum.GroupSpanID,
		subgroupMember.SpanID, subgroupMember.SpanID == subSum.GroupSpanID)
	gcheck(t, "sub parent group id", rootSum.GroupSpanID, subSum.ParentGroup,
		rootSum.GroupSpanID, subSum.ParentGroup == rootSum.GroupSpanID)
	gcheck(t, "sub same trace", rootSum.TraceID, subSum.TraceID, rootSum.TraceID,
		subSum.TraceID == rootSum.TraceID)
	// 两个组各产出且仅产出一条汇总。
	gcheck(t, "summaries count", "root + sub", len(summaries), 2, len(summaries) == 2)
}

func TestFailFastStopsOthers(t *testing.T) {
	trace.SetIDSource(trace.NewCounterIDSource())
	g := New(context.Background(), "ff", WithFailFast())
	g.Go("fails", func(ctx context.Context, s trace.Span) {
		s.End(trace.ReasonError)
	})
	g.Go("blocked", func(ctx context.Context, s trace.Span) {
		<-ctx.Done() // 收到 fail-fast 传播的停止信号
	})
	sum := g.Wait()

	gcheck(t, "stopped by failfast", true, sum.Stopped, true, sum.Stopped)
	gcheck(t, "failfast reason", StopReasonFailFast, sum.StopReason, StopReasonFailFast,
		sum.StopReason == StopReasonFailFast)
	gcheck(t, "one error", 1, sum.ErrorCount, 1, sum.ErrorCount == 1)
	gcheck(t, "one canceled", 1, sum.CanceledCount, 1, sum.CanceledCount == 1)
	if sum.OutcomeCounts[OutcomeParentCanceled] != 1 {
		t.Fatalf("被 failfast 停掉的成员应记 parent_canceled\n实际分布: %#v\n判定依据: 失败成员之外的成员收到停止信号",
			sum.OutcomeCounts)
	}
	// 从结束后的汇总确认被停成员的唯一结论（成员不会同时算完成与停止）。
	for _, m := range sum.Members {
		if m.Name == "blocked" {
			gcheck(t, "blocked member reason", OutcomeParentCanceled, m.Outcome,
				OutcomeParentCanceled, m.Outcome == OutcomeParentCanceled)
		}
		if m.Name == "fails" {
			gcheck(t, "failing member reason", OutcomeError, m.Outcome,
				OutcomeError, m.Outcome == OutcomeError)
		}
	}
}

func TestNoFailFastIsolatesFailure(t *testing.T) {
	trace.SetIDSource(trace.NewCounterIDSource())
	g := New(context.Background(), "iso") // 默认不 fail-fast
	g.Go("fails", func(ctx context.Context, s trace.Span) { s.End(trace.ReasonError) })
	g.Go("ok", func(ctx context.Context, s trace.Span) {})
	g.Go("panic-member", func(ctx context.Context, s trace.Span) { panic("boom") })
	sum := g.Wait()

	gcheck(t, "not stopped (isolation)", false, sum.Stopped, false, !sum.Stopped)
	gcheck(t, "two failures (error+panic)", 2, sum.ErrorCount, 2, sum.ErrorCount == 2)
	gcheck(t, "one success", 1, sum.SuccessCount, 1, sum.SuccessCount == 1)
	if sum.OutcomeCounts[OutcomeError] != 2 {
		t.Fatalf("error 与 panic 都应计入 error\n实际分布: %#v", sum.OutcomeCounts)
	}
	var panics int
	for _, m := range sum.Members {
		if m.Panic {
			panics++
			gcheck(t, "panic outcome", trace.ReasonError, m.Outcome, trace.ReasonError,
				m.Outcome == trace.ReasonError)
		}
	}
	gcheck(t, "panic flagged once", 1, panics, 1, panics == 1)
}

func TestMemberStopsItself(t *testing.T) {
	trace.SetIDSource(trace.NewCounterIDSource())
	g := New(context.Background(), "self")
	g.Go("self-cancel", func(ctx context.Context, s trace.Span) {
		s.Cancel() // 成员自己的调用方发起停止，父组仍然存活
	})
	sum := g.Wait()
	// 组成员自己取消：组仍存活，成员原因应为 canceled 而非 parent_canceled，
	// 组整体不算被停。
	if sum.OutcomeCounts[OutcomeCanceled] != 1 {
		t.Fatalf("成员自停应记 canceled，实际分布: %#v", sum.OutcomeCounts)
	}
	gcheck(t, "group not stopped", false, sum.Stopped, false, !sum.Stopped)
}

// TestStopVsTimeoutConsistency 验证“停止和超时几乎同时发生”时结论唯一一致：
// 外部 deadline 触发应稳定判定为 external_cancel + deadline_exceeded，
// 不会与显式 Stop 竞态给出前后矛盾的结论。
func TestStopVsTimeoutConsistency(t *testing.T) {
	trace.SetIDSource(trace.NewCounterIDSource())
	// 重复多轮，放大竞态窗口；每轮结论必须一致。
	// 先等 deadline 确定到期并充分传播，再并发调用 Stop：
	// 此时“停止与超时都已发生”，结论必须稳定为外部超时，不允许偶发成普通取消。
	for iter := 0; iter < 50; iter++ {
		pctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		g := New(pctx, "race-group")
		g.Go("blocked", func(ctx context.Context, s trace.Span) { <-ctx.Done() })
		// deadline 确定到期后再 Stop：origin 此时必为 DeadlineExceeded。
		time.Sleep(15 * time.Millisecond)
		g.Stop(StopReasonStopped)
		sum := g.Wait()
		cancel() // deadline 已确定到期，cancel 仅释放资源，不改变已发生的结论

		if !sum.Stopped {
			t.Fatalf("iter %d: 组应被标记停止\n实际: %+v", iter, sum)
		}
		// 外部超时优先：组 Span 原因必须是 deadline_exceeded，汇总停止原因
		// 必须是 external_cancel（finish 中确定性判定，不受竞态影响）。
		if sum.EndReason != trace.ReasonDeadlineExceeded {
			t.Fatalf("iter %d: 外部超时应判定 deadline_exceeded\n实际: %q\n判定依据: 上级超时优先且确定",
				iter, sum.EndReason)
		}
		if sum.StopReason != StopReasonExternal {
			t.Fatalf("iter %d: 停止原因应稳定为 external_cancel\n实际: %q", iter, sum.StopReason)
		}
		if sum.OutcomeCounts[OutcomeDeadlineExceeded] != 1 {
			t.Fatalf("iter %d: 成员应记 deadline_exceeded，实际分布: %#v",
				iter, sum.OutcomeCounts)
		}
	}
}

// TestMemberConcurrentEndCountedOnce 纯 group 包：成员在不同协程里并发结束，
// 重复 End 都不会让同一成员被统计两次或结论翻转。
func TestMemberConcurrentEndCountedOnce(t *testing.T) {
	trace.SetIDSource(trace.NewCounterIDSource())
	var emits atomic.Int32
	g := New(context.Background(), "burst",
		WithSummaryEmitter(func(s Summary) { emits.Add(1) }))
	const n = 40
	ready := make(chan struct{}, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		g.Go("m", func(ctx context.Context, s trace.Span) {
			ready <- struct{}{}
			<-start // 所有成员协程就绪后一起结束，放大并发
			s.End(trace.ReasonCompleted)
		})
	}
	for i := 0; i < n; i++ {
		<-ready
	}
	close(start)
	sum := g.Wait()

	if sum.Coverage.Total != n {
		t.Fatalf("成员总数错误: %d 期望 %d", sum.Coverage.Total, n)
	}
	if sum.SuccessCount != n {
		t.Errorf("并发结束下成功数错误: %d 期望 %d\n分布: %#v",
			sum.SuccessCount, n, sum.OutcomeCounts)
	}
	if emits.Load() != 1 {
		t.Errorf("汇总应恰好产出一次，实际 %d", emits.Load())
	}
	ids := map[string]int{}
	for _, m := range sum.Members {
		ids[m.SpanID]++
	}
	for id, c := range ids {
		if c != 1 {
			t.Fatalf("成员 %s 被计入 %d 次", id, c)
		}
	}
}
