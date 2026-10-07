package obs_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/xingjianluo/trace-observability-go/export"
	"github.com/xingjianluo/trace-observability-go/group"
	"github.com/xingjianluo/trace-observability-go/logging"
	"github.com/xingjianluo/trace-observability-go/obs"
	"github.com/xingjianluo/trace-observability-go/trace"
)

// 业务场景：一批并发动作放进一个业务组，成员回调里再嵌套子组，
// 真实管线跑完收尾后，每个组必须恰好产出一条组级结果，
// 且能与每个成员的导出明细对上号；组级属性导出前必须脱敏。
func TestIntegrationGroupSummaryExportedOnceAndCorrelated(t *testing.T) {
	var buf bytes.Buffer
	p := newTestPipeline(t, &buf, 1, 10000)

	ctx, root := p.StartSpan(context.Background(), "batch.job", false)
	g := group.New(ctx,
		group.WithName("nightly-batch"),
		group.WithAttribute("token", "secret-token"),
		group.WithAttribute("batch.size", 3),
	)
	for i := 0; i < 3; i++ {
		g.Go("task", func(ctx context.Context, s trace.Span) {
			p.Logger().Info(ctx, "task done", nil)
		})
	}
	// 成员回调里再派生一批动作、用子组包起来（嵌套分组）。
	g.Go("spawn-sub", func(ctx context.Context, s trace.Span) {
		sub := group.New(ctx).Sub(group.WithName("sub-batch"))
		sub.Go("sub-task", func(ctx context.Context, s trace.Span) {})
		sub.Wait()
	})
	sum := g.Wait()
	again := g.Wait() // 重复 Wait 必须幂等，不能产生第二条组级结果
	root.End("")
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	// 外层直接成员：3 task + spawn-sub + sub-batch 子组 = 5。
	if sum.MembersTotal != 5 || again.MembersTotal != 5 {
		t.Errorf("[outer members] 判定失败\n  输入:   3 task + 1 嵌套派发成员 + 1 子组\n  实际:   %d / %d\n  期望:   5\n  判定标准: 每个动作（含子组）恰好计入所属组一次",
			sum.MembersTotal, again.MembersTotal)
	}

	d := decodeOutput(t, buf.Bytes())
	// 每个组恰好一条组级结果：外层 + 内层 = 2 条，重复 Wait 不得新增。
	if len(d.groups) != 2 {
		t.Fatalf("[exactly one summary per group] 判定失败\n  输入:   2 个组, Wait 调用 3 次\n  实际:   %d 条 group_summary\n  期望:   2",
			len(d.groups))
	}
	byID := map[string]export.GroupSummaryView{}
	for _, gv := range d.groups {
		if _, dup := byID[gv.GroupSpanID]; dup {
			t.Errorf("组级结果重复导出: %s", gv.GroupSpanID)
		}
		byID[gv.GroupSpanID] = gv
	}
	outer, ok := byID[sum.GroupSpanID]
	if !ok {
		t.Fatalf("导出中找不到外层组 %s 的组级结果", sum.GroupSpanID)
	}
	if outer.MembersTotal != 5 || outer.StatusCounts["ok"] != 5 {
		t.Errorf("[outer summary counts] 判定失败\n  实际:   total=%d ok=%d\n  期望:   total=5 ok=5",
			outer.MembersTotal, outer.StatusCounts["ok"])
	}
	if outer.StoppedEarly || outer.StopReason != trace.ReasonCompleted {
		t.Errorf("[outer stop fields] 判定失败\n  实际:   stopped=%v reason=%q\n  期望:   stopped=false reason=completed",
			outer.StoppedEarly, outer.StopReason)
	}
	if outer.DurationNanos < 0 || outer.TraceID != root.TraceID() {
		t.Errorf("[outer timing/trace] 判定失败\n  实际:   duration=%d trace=%s\n  期望:   duration>=0 trace=%s",
			outer.DurationNanos, outer.TraceID, root.TraceID())
	}
	// 组级结果与成员明细对账：每个成员都能按 span_id 找到导出明细，且未缺失。
	var inner export.GroupSummaryView
	innerFound := false
	for _, gv := range d.groups {
		if gv.GroupSpanID != outer.GroupSpanID {
			inner, innerFound = gv, true
		}
	}
	if !innerFound {
		t.Fatal("导出中找不到内层子组的组级结果")
	}
	subGroupLinked := false
	for _, m := range outer.Members {
		if m.MissingReason != "" {
			t.Errorf("全采样+足量队列下成员 %s 不应缺失: %s", m.SpanID, m.MissingReason)
		}
		if _, ok := d.spans[m.SpanID]; !ok {
			t.Errorf("成员 %s (%s) 找不到导出明细", m.SpanID, m.Name)
		}
		if m.SpanID == inner.GroupSpanID {
			subGroupLinked = true
		}
	}
	if !subGroupLinked {
		t.Error("内层子组的组 Span 未作为外层成员登记，嵌套对账断裂")
	}
	if inner.MembersTotal != 1 || inner.TraceID != outer.TraceID {
		t.Errorf("[inner summary] 判定失败\n  实际:   total=%d trace=%s\n  期望:   total=1 trace=%s",
			inner.MembersTotal, inner.TraceID, outer.TraceID)
	}
	// 组级属性导出前必须按现有规则脱敏。
	if outer.Attributes["token"] != logging.RedactedPlaceholder {
		t.Errorf("[attribute masking] 判定失败\n  输入:   token=secret-token\n  实际:   %#v\n  期望:   %s",
			outer.Attributes["token"], logging.RedactedPlaceholder)
	}
	if outer.Attributes["batch.size"] != float64(3) {
		t.Errorf("非敏感属性应原样保留: %#v", outer.Attributes["batch.size"])
	}
	if bytes.Contains(buf.Bytes(), []byte("secret-token")) {
		t.Error("导出数据中残留原始敏感值 secret-token")
	}
}

// 业务场景：扇出过程中被整体停掉——未结束的成员及时收到信号，
// 各自记下停止原因，组级结果标记提前终止。
func TestIntegrationGroupStoppedDuringFanout(t *testing.T) {
	var buf bytes.Buffer
	p := newTestPipeline(t, &buf, 1, 10000)

	ctx, root := p.StartSpan(context.Background(), "scatter", false)
	g := group.New(ctx, group.WithName("fanout"))
	const n = 8
	started := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		g.Go("worker", func(ctx context.Context, s trace.Span) {
			started <- struct{}{}
			<-ctx.Done() // 模拟进行中的工作
		})
	}
	for i := 0; i < n; i++ {
		<-started
	}
	g.Stop() // 扇出进行中整体停掉
	sum := g.Wait()
	root.End("")
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	if !sum.StoppedEarly || sum.StopReason != trace.ReasonCanceled {
		t.Errorf("[stopped early] 判定失败\n  输入:   扇出中 g.Stop()\n  实际:   stopped=%v reason=%q\n  期望:   stopped=true reason=canceled",
			sum.StoppedEarly, sum.StopReason)
	}
	if sum.ReasonCounts[trace.ReasonParentCanceled] != n {
		t.Errorf("[member stop reasons] 判定失败\n  实际:   parent_canceled=%d\n  期望:   %d",
			sum.ReasonCounts[trace.ReasonParentCanceled], n)
	}

	d := decodeOutput(t, buf.Bytes())
	if len(d.groups) != 1 {
		t.Fatalf("[exactly one summary] 判定失败\n  实际:   %d 条\n  期望:   1", len(d.groups))
	}
	gv := d.groups[0]
	if !gv.StoppedEarly || gv.StopReason != trace.ReasonCanceled {
		t.Errorf("[exported stop fields] 判定失败\n  实际:   stopped=%v reason=%q", gv.StoppedEarly, gv.StopReason)
	}
	// 每个成员既在组级结果里记了停止原因，也能对上导出的成员明细。
	for _, m := range gv.Members {
		if m.EndReason != trace.ReasonParentCanceled {
			t.Errorf("成员 %s 停止原因错误: %s", m.SpanID, m.EndReason)
		}
		sv, ok := d.spans[m.SpanID]
		if !ok {
			t.Errorf("成员 %s 找不到导出明细", m.SpanID)
			continue
		}
		if sv.EndReason != trace.ReasonParentCanceled {
			t.Errorf("成员 %s 导出明细停止原因错误: %s", m.SpanID, sv.EndReason)
		}
	}
}

// 业务场景：采样率调到 0——成员明细不导出，但组级结果必须仍在，
// 并把每个成员如实记为 sampled_out，否则丢弃量无处对账。
func TestIntegrationGroupCoverageSampledOut(t *testing.T) {
	var buf bytes.Buffer
	p := newTestPipeline(t, &buf, 0, 10000) // rate=0：全不采样

	ctx, root := p.StartSpan(context.Background(), "unsampled.job", false)
	g := group.New(ctx, group.WithName("unsampled-group"))
	for i := 0; i < 3; i++ {
		g.Go("task", func(ctx context.Context, s trace.Span) {})
	}
	sum := g.Wait()
	root.End("")
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	if sum.MissingCount != 3 || sum.MissingReasons[group.MissingSampledOut] != 3 {
		t.Errorf("[sampled_out accounting] 判定失败\n  输入:   rate=0, 3 成员\n  实际:   missing=%d reasons=%v\n  期望:   missing=3 sampled_out=3",
			sum.MissingCount, sum.MissingReasons)
	}

	d := decodeOutput(t, buf.Bytes())
	if len(d.spans) != 0 {
		t.Errorf("未采样链路不应导出任何 span，实际 %d 条", len(d.spans))
	}
	if len(d.groups) != 1 {
		t.Fatalf("[summary survives sampling] 判定失败\n  实际:   %d 条 group_summary\n  期望:   1（未采样也要留下组级对账结果）",
			len(d.groups))
	}
	gv := d.groups[0]
	if gv.MembersTotal != 3 || gv.MissingCount != 3 || gv.MissingReasons[group.MissingSampledOut] != 3 {
		t.Errorf("[exported coverage] 判定失败\n  实际:   total=%d missing=%d reasons=%v",
			gv.MembersTotal, gv.MissingCount, gv.MissingReasons)
	}
	for _, m := range gv.Members {
		if m.MissingReason != group.MissingSampledOut {
			t.Errorf("成员 %s 未纳入原因错误: %q，期望 sampled_out", m.SpanID, m.MissingReason)
		}
	}
	if gv.SamplingState != "drop" {
		t.Errorf("组级结果应标记链路采样状态为 drop，实际 %q", gv.SamplingState)
	}
}

// 业务场景：队列积压丢数据——组级结果必须分清“实际纳入”与
// “队列没放下”的成员，且自身通过可靠投递幸存下来完成对冲。
func TestIntegrationGroupCoverageQueueBacklog(t *testing.T) {
	var buf bytes.Buffer
	const capN = 8
	p, err := obs.NewPipeline(obs.Config{
		Output:        &buf,
		QueueCapacity: capN,
		BatchSize:     64,
		FlushInterval: time.Hour, // 关闭定时刷新，让积压确定性地发生
		DropPolicy:    export.DropNewest,
		SampleRate:    1,
		MaskRules:     logging.DefaultRules(),
		IDSource:      trace.NewCounterIDSource(),
	})
	if err != nil {
		t.Fatalf("NewPipeline: %v", err)
	}

	ctx, root := p.StartSpan(context.Background(), "burst.job", false)
	g := group.New(ctx, group.WithName("backlog-group"))
	const n = 10
	for i := 0; i < n; i++ {
		g.Go("task", func(ctx context.Context, s trace.Span) {})
	}
	// 队列满后组级汇总的可靠投递会短暂阻塞，把 Wait 放到独立协程。
	done := make(chan group.Summary, 1)
	go func() { done <- g.Wait() }()
	// 等全部 10 条成员明细投递完毕（8 入队 + 2 丢弃），再 Flush 腾出空位。
	deadline := time.Now().Add(5 * time.Second)
	for {
		st := p.Exporter().Snapshot()
		if st.Enqueued+st.DroppedNew >= n {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("成员投递未在限定时间内完成: %+v", st)
		}
		time.Sleep(time.Millisecond)
	}
	if err := p.Exporter().Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	sum := <-done
	root.End("")
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	if sum.MembersTotal != n || sum.MissingCount != 2 {
		t.Errorf("[backlog coverage] 判定失败\n  输入:   10 成员, 容量 8, DropNewest\n  实际:   total=%d missing=%d\n  期望:   total=10 missing=2",
			sum.MembersTotal, sum.MissingCount)
	}
	if sum.MissingReasons[group.MissingQueueDroppedNew] != 2 {
		t.Errorf("[queue_dropped_new accounting] 判定失败\n  实际:   %v\n  期望:   queue_dropped_new=2",
			sum.MissingReasons)
	}

	d := decodeOutput(t, buf.Bytes())
	if len(d.groups) != 1 {
		t.Fatalf("[summary survives backlog] 判定失败\n  实际:   %d 条 group_summary\n  期望:   1（队列积压不得丢掉组级对账结果）",
			len(d.groups))
	}
	gv := d.groups[0]
	// 组级结果与导出明细逐条对账：缺失的成员不在导出里，纳入的必须在。
	missing := 0
	for _, m := range gv.Members {
		_, exported := d.spans[m.SpanID]
		if m.MissingReason == group.MissingQueueDroppedNew {
			missing++
			if exported {
				t.Errorf("被队列丢弃的成员 %s 不应出现在导出里", m.SpanID)
			}
		} else if !exported {
			t.Errorf("已纳入的成员 %s 找不到导出明细", m.SpanID)
		}
	}
	if missing != 2 {
		t.Errorf("导出侧缺失成员数与组级结果不一致: %d，期望 2", missing)
	}
	// 收尾守恒：已接受数据（含组级结果）全部落盘，不丢失。
	st := p.Exporter().Snapshot()
	if st.Enqueued != st.Exported {
		t.Errorf("[shutdown no loss] 判定失败\n  实际:   enqueued=%d exported=%d", st.Enqueued, st.Exported)
	}
}
