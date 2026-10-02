package obs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/trace-observability-go/export"
	"github.com/xingjianluo/trace-observability-go/group"
	"github.com/xingjianluo/trace-observability-go/logging"
	"github.com/xingjianluo/trace-observability-go/obs"
	"github.com/xingjianluo/trace-observability-go/trace"
)

func newGroupPipeline(t *testing.T, buf *bytes.Buffer, rate float64, capacity int) *obs.Pipeline {
	t.Helper()
	p, err := obs.NewPipeline(obs.Config{
		Output:        buf,
		QueueCapacity: capacity,
		BatchSize:     64,
		FlushInterval: 50 * time.Millisecond,
		DropPolicy:    export.DropNewest,
		SampleRate:    rate,
		MaskRules:     logging.DefaultRules(),
		IDSource:      trace.NewCounterIDSource(),
	})
	if err != nil {
		t.Fatalf("NewPipeline: %v", err)
	}
	return p
}

// TestIntegrationNestedGroupsAndSingleSummary 覆盖：嵌套组归属、
// 汇总与成员明细靠 trace/group id 对齐、每个组恰好一条汇总。
func TestIntegrationNestedGroupsAndSingleSummary(t *testing.T) {
	var buf bytes.Buffer
	p := newGroupPipeline(t, &buf, 1, 10000)

	ctx, rootSpan := p.StartSpan(context.Background(), "biz", false)
	root := p.StartGroup(ctx, "outer")
	root.Go("outer-member", func(ctx context.Context, s trace.Span) {})

	sub := root.SubGroup("inner")
	sub.Go("inner-member", func(ctx context.Context, s trace.Span) {})

	subSum := sub.Wait()
	rootSum := root.Wait()
	rootSpan.End("")
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	d := decodeOutput(t, buf.Bytes())
	// 恰好两条 group 记录，且 SpanID 与返回汇总一致。
	if len(d.groups) != 2 {
		t.Fatalf("组汇总数量错误\n输入: outer+inner\n实际导出 group: %d\n期望: 2\n原始: %s",
			len(d.groups), d.raw)
	}
	outer, ok1 := d.groups[rootSum.GroupSpanID]
	inner, ok2 := d.groups[subSum.GroupSpanID]
	if !ok1 || !ok2 {
		t.Fatalf("导出的组汇总与 Wait 返回值对不上\nouter=%v inner=%v\n导出: %#v", ok1, ok2, d.groups)
	}

	// inner 挂在 outer 之下，trace 相同。
	if inner.ParentGroup != outer.GroupSpanID {
		t.Errorf("子组父级链错误\n实际 parent_group=%q\n期望 %q", inner.ParentGroup, outer.GroupSpanID)
	}
	if inner.TraceID != outer.TraceID {
		t.Errorf("嵌套组 trace 不一致: %s vs %s", inner.TraceID, outer.TraceID)
	}
	// outer 的直接成员：outer-member + inner 的组 Span。
	if outer.Coverage.Total != 2 {
		t.Errorf("outer 成员总数错误\n实际: %d\n期望: 2\n判定依据: 子组组 Span 是父组直接成员",
			outer.Coverage.Total)
	}
	if inner.Coverage.Total != 1 || inner.SuccessCount != 1 {
		t.Errorf("inner 成员统计错误: total=%d success=%d", inner.Coverage.Total, inner.SuccessCount)
	}
	// 全采样、大队列：全部明细都应 observed。
	if outer.Coverage.Observed != outer.Coverage.Total ||
		inner.Coverage.Observed != inner.Coverage.Total {
		t.Errorf("全采样下覆盖应完整: outer=%+v inner=%+v", outer.Coverage, inner.Coverage)
	}

	// 明细对齐：inner 的组 Span 作为 outer 成员，能在 spans 中找到且 group_span_id 指向 outer。
	var innerGroupSpan export.SpanView
	found := false
	for _, sp := range d.spans {
		if sp.SpanID == inner.GroupSpanID {
			innerGroupSpan = sp
			found = true
		}
	}
	if !found {
		t.Fatal("找不到子组组 Span 明细，违反“不允许找不到归属的成员”")
	}
	if innerGroupSpan.GroupSpanID != outer.GroupSpanID {
		t.Errorf("子组组 Span 归属标记错误: %q 期望 %q",
			innerGroupSpan.GroupSpanID, outer.GroupSpanID)
	}

	// 每个成员 span 的 group_span_id 必须能在 groups 中找到归属。
	for _, sp := range d.spans {
		if sp.GroupSpanID == "" {
			continue // 根 biz Span 不在任何组内，允许为空
		}
		if _, ok := d.groups[sp.GroupSpanID]; !ok {
			t.Errorf("成员 Span %s(%s) 的 group_span_id=%s 找不到归属汇总",
				sp.Name, sp.SpanID, sp.GroupSpanID)
		}
	}
}

// TestIntegrationGroupFanoutCanceled 覆盖：扇出过程中整体停止，
// 未结束成员收到信号并记 parent_canceled，汇总标记 stopped。
func TestIntegrationGroupFanoutCanceled(t *testing.T) {
	var buf bytes.Buffer
	p := newGroupPipeline(t, &buf, 1, 10000)

	ctx, rootSpan := p.StartSpan(context.Background(), "biz", false)
	g := p.StartGroup(ctx, "fanout")
	g.Go("done", func(ctx context.Context, s trace.Span) {})
	g.Go("blocked", func(ctx context.Context, s trace.Span) { <-ctx.Done() })
	time.Sleep(30 * time.Millisecond) // 确保 blocked 已挂起
	g.Stop(group.StopReasonStopped)
	sum := g.Wait()
	rootSpan.End("")
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	d := decodeOutput(t, buf.Bytes())
	gv, ok := d.groups[sum.GroupSpanID]
	if !ok {
		t.Fatalf("导出中找不到组汇总\n实际 groups: %#v", d.groups)
	}
	if !gv.Stopped || gv.StopReason != group.StopReasonStopped {
		t.Errorf("组停止标记错误\n实际 stopped=%v reason=%q", gv.Stopped, gv.StopReason)
	}
	if gv.Coverage.Total != 2 || gv.SuccessCount != 1 || gv.CanceledCount != 1 {
		t.Errorf("停止时成员分布错误\n实际 total=%d ok=%d canceled=%d\n期望 2/1/1",
			gv.Coverage.Total, gv.SuccessCount, gv.CanceledCount)
	}
	if gv.OutcomeCounts[group.OutcomeParentCanceled] != 1 {
		t.Errorf("被停成员应记 parent_canceled，实际分布: %#v", gv.OutcomeCounts)
	}
}

// TestIntegrationSummaryEmittedOnceConcurrent 覆盖：成员并发结束、
// 重复 Wait 时汇总只导出一条。
func TestIntegrationSummaryEmittedOnceConcurrent(t *testing.T) {
	var buf bytes.Buffer
	p := newGroupPipeline(t, &buf, 1, 10000)
	ctx, rootSpan := p.StartSpan(context.Background(), "biz", false)
	g := p.StartGroup(ctx, "concurrent")
	const n = 50
	for i := 0; i < n; i++ {
		g.Go("m", func(ctx context.Context, s trace.Span) {})
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); g.Wait() }()
	}
	sum := g.Wait()
	wg.Wait()
	rootSpan.End("")
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	d := decodeOutput(t, buf.Bytes())
	if len(d.groups) != 1 {
		t.Fatalf("并发 Wait 下汇总应只出现一次\n实际 group 记录数: %d\n期望: 1", len(d.groups))
	}
	gv := d.groups[sum.GroupSpanID]
	if gv.Coverage.Total != n || gv.Coverage.Observed != n {
		t.Errorf("并发成员覆盖错误: total=%d observed=%d 期望 %d",
			gv.Coverage.Total, gv.Coverage.Observed, n)
	}
	// 明细 spanID 不重复。
	seen := map[string]bool{}
	for _, m := range gv.Members {
		if seen[m.SpanID] {
			t.Fatalf("成员被重复计入: %s", m.SpanID)
		}
		seen[m.SpanID] = true
	}
	if len(gv.Members) != n {
		t.Errorf("成员明细数错误: %d 期望 %d", len(gv.Members), n)
	}
}

// 防止未使用导入（json 在后续用例使用）。
var _ = json.Marshal
var _ = strings.Contains

// TestIntegrationSamplingAndBackpressureCoverage 覆盖：
// 低采样率时成员明细区分为 observed/sampled_out；队列积压时采样命中的
// 成员明细因背压没入队，计入 queue_dropped；汇总不默认所有成员都在。
func TestIntegrationSamplingAndBackpressureCoverage(t *testing.T) {
	// 场景 A：rate=0，全链路不采样，成员全部计入 sampled_out。
	var buf bytes.Buffer
	p := newGroupPipeline(t, &buf, 0, 10000)
	g := p.StartGroup(context.Background(), "dropped-trace")
	const n = 20
	for i := 0; i < n; i++ {
		g.Go("m", func(ctx context.Context, s trace.Span) {})
	}
	sum := g.Wait()
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sum.Coverage.Total != n {
		t.Fatalf("采样不影响成员总数\n输入: %d 成员\n实际 total=%d\n期望 %d",
			n, sum.Coverage.Total, n)
	}
	if sum.Coverage.SampledOut != n {
		t.Errorf("rate=0 时成员应全部 sampled_out\n实际 coverage=%+v\n期望 sampled_out=%d",
			sum.Coverage, n)
	}
	if sum.Coverage.Observed != 0 || sum.Coverage.QueueDropped != 0 {
		t.Errorf("rate=0 不应有 observed/queue_dropped，实际: %+v", sum.Coverage)
	}
	if sum.Coverage.Observed+sum.Coverage.SampledOut+sum.Coverage.QueueDropped+
		sum.Coverage.Closed+sum.Coverage.Unknown != sum.Coverage.Total {
		t.Errorf("覆盖分类之和必须等于成员总数（不能含糊带过）: %+v", sum.Coverage)
	}
	// rate=0 时不应有任何 span/group 明细导出。
	d := decodeOutput(t, buf.Bytes())
	if len(d.spans) != 0 || len(d.groups) != 0 {
		t.Errorf("rate=0 不应导出 span/group，实际 spans=%d groups=%d",
			len(d.spans), len(d.groups))
	}

	// 场景 B：全采样 + 极小队列制造积压，部分成员明细计入 queue_dropped。
	// 用独立计数器让 span 序列确定：汇总在成员之后投递，队列满时被丢，
	// 因此从内存 Summary 校验覆盖（汇总本身是否导出不影响覆盖结论）。
	var buf2 bytes.Buffer
	const capN = 2
	p2 := newGroupPipeline(t, &buf2, 1, capN)
	ctx2, root2 := p2.StartSpan(context.Background(), "biz", false)
	g2 := p2.StartGroup(ctx2, "pressured")
	const m2 = 40
	for i := 0; i < m2; i++ {
		g2.Go("m", func(ctx context.Context, s trace.Span) {})
	}
	sum2 := g2.Wait()
	root2.End("")
	// 不依赖汇总是否成功导出：覆盖统计来自内存中的唯一 Summary。
	stBefore := p2.Exporter().Snapshot()
	if err := p2.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = stBefore
	if sum2.Coverage.Total != m2 {
		t.Fatalf("成员总数错误: %d 期望 %d", sum2.Coverage.Total, m2)
	}
	if sum2.Coverage.QueueDropped == 0 {
		t.Errorf("小队列突发下应有成员因背压没纳入统计\n输入: %d 成员 容量 %d\n实际 coverage=%+v",
			m2, capN, sum2.Coverage)
	}
	if sum2.Coverage.Observed+sum2.Coverage.QueueDropped != m2 {
		t.Errorf("全采样下 observed+queue_dropped 应等于总数: %+v", sum2.Coverage)
	}
	if sum2.Coverage.Observed >= m2 {
		t.Errorf("队列容量仅 %d 却统计全部 observed，背压统计失真: %+v", capN, sum2.Coverage)
	}
}

// TestIntegrationGroupSummaryMasking 覆盖：组汇总携带成员属性时，
// 脱敏规则照常生效，导出中不残留原始敏感内容；同样输入重复跑结论一致。
func TestIntegrationGroupSummaryMasking(t *testing.T) {
	run := func() (string, export.GroupView) {
		var buf bytes.Buffer
		p := newGroupPipeline(t, &buf, 1, 10000)
		g := p.StartGroup(context.Background(), "sensitive-group")
		g.SetAttribute("token", "tok_group_secret_value")
		g.SetAttribute("phone", "13800138000")
		g.SetAttribute("region", "cn-north")
		g.Go("member", func(ctx context.Context, s trace.Span) {
			p.Logger().Info(ctx, "doing", map[string]any{"api_key": "AKIA-RAW-KEY"})
		})
		sum := g.Wait()
		if err := p.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		d := decodeOutput(t, buf.Bytes())
		gv, ok := d.groups[sum.GroupSpanID]
		if !ok {
			t.Fatalf("汇总未导出: groups=%#v", d.groups)
		}
		return buf.String(), gv
	}

	raw1, gv1 := run()
	raw2, gv2 := run()
	_ = raw1
	_ = raw2

	// 汇总属性中敏感字段已脱敏。
	if gv1.Attributes["token"] != logging.RedactedPlaceholder {
		t.Errorf("组属性 token 未脱敏\n实际: %q", gv1.Attributes["token"])
	}
	if gv1.Attributes["phone"] == "13800138000" {
		t.Errorf("组属性 phone 未脱敏: %q", gv1.Attributes["phone"])
	}
	if gv1.Attributes["region"] != "cn-north" {
		t.Errorf("非敏感属性不应被改动: %q", gv1.Attributes["region"])
	}
	// 原始敏感值不得出现在任何导出记录中。
	for _, secret := range []string{"tok_group_secret_value", "13800138000", "AKIA-RAW-KEY"} {
		if strings.Contains(raw1, secret) {
			t.Errorf("导出中残留原始敏感值 %q", secret)
		}
	}
	// 可复现：相同输入两次跑，汇总关键字段一致（时间戳除外）。
	if gv1.GroupSpanID != gv2.GroupSpanID ||
		gv1.Coverage.Total != gv2.Coverage.Total ||
		gv1.SuccessCount != gv2.SuccessCount ||
		gv1.OutcomeCounts["completed"] != gv2.OutcomeCounts["completed"] {
		t.Errorf("相同输入重复跑结论不一致\n第一次: %+v\n第二次: %+v", gv1, gv2)
	}
	if len(gv1.Members) != len(gv2.Members) ||
		gv1.Members[0].Name != gv2.Members[0].Name ||
		gv1.Members[0].Outcome != gv2.Members[0].Outcome {
		t.Errorf("成员明细两次不一致，不可复现")
	}
}

// TestIntegrationFailFastGroupExport 端到端验证 FailFast 组的汇总是一条
// stopped 记录，成员结果分布与归属在导出中自洽。
func TestIntegrationFailFastGroupExport(t *testing.T) {
	var buf bytes.Buffer
	p := newGroupPipeline(t, &buf, 1, 10000)
	ctx, rootSpan := p.StartSpan(context.Background(), "biz", false)
	g := p.StartGroup(ctx, "ff-biz", group.WithFailFast())
	g.Go("bad", func(ctx context.Context, s trace.Span) { s.End(trace.ReasonError) })
	g.Go("waiter", func(ctx context.Context, s trace.Span) { <-ctx.Done() })
	sum := g.Wait()
	rootSpan.End("")
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	d := decodeOutput(t, buf.Bytes())
	gv, ok := d.groups[sum.GroupSpanID]
	if !ok {
		t.Fatalf("FailFast 组汇总未导出: %#v", d.groups)
	}
	if !gv.FailFast || gv.StopReason != group.StopReasonFailFast {
		t.Errorf("FailFast 标记错误: fail_fast=%v stop=%q", gv.FailFast, gv.StopReason)
	}
	if gv.ErrorCount != 1 || gv.CanceledCount != 1 {
		t.Errorf("FailFast 成员分布错误: error=%d canceled=%d", gv.ErrorCount, gv.CanceledCount)
	}
	// 全部成员的 group_span_id 都应指向该组。
	for _, sp := range d.spans {
		if sp.Name == "bad" || sp.Name == "waiter" {
			if sp.GroupSpanID != gv.GroupSpanID {
				t.Errorf("成员 %s 归属错误: %q", sp.Name, sp.GroupSpanID)
			}
		}
	}
}

// TestIntegrationExternalDeadlineGroup 端到端验证外部 deadline 到期时，
// 组汇总与成员都稳定判定为外部超时。
func TestIntegrationExternalDeadlineGroup(t *testing.T) {
	var buf bytes.Buffer
	p := newGroupPipeline(t, &buf, 1, 10000)
	dctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	g := p.StartGroup(dctx, "deadline-biz")
	g.Go("slow", func(ctx context.Context, s trace.Span) { <-ctx.Done() })
	sum := g.Wait()
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sum.StopReason != group.StopReasonExternal || !sum.Stopped {
		t.Errorf("外部 deadline 停止原因错误: stopped=%v reason=%q", sum.Stopped, sum.StopReason)
	}
	if sum.EndReason != trace.ReasonDeadlineExceeded {
		t.Errorf("组应记 deadline_exceeded，实际 %q", sum.EndReason)
	}
	if sum.OutcomeCounts[group.OutcomeDeadlineExceeded] != 1 {
		t.Errorf("成员应记 deadline_exceeded，分布: %#v", sum.OutcomeCounts)
	}
	d := decodeOutput(t, buf.Bytes())
	gv, ok := d.groups[sum.GroupSpanID]
	if !ok {
		t.Fatal("外部 deadline 组汇总未导出")
	}
	if gv.EndReason != trace.ReasonDeadlineExceeded {
		t.Errorf("导出的组终止原因错误: %q", gv.EndReason)
	}
}
