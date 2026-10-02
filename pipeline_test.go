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
	"github.com/xingjianluo/trace-observability-go/logging"
	"github.com/xingjianluo/trace-observability-go/obs"
	"github.com/xingjianluo/trace-observability-go/trace"
)

type decoded struct {
	spans  map[string]export.SpanView
	logs   []logging.Entry
	groups map[string]export.GroupView
	raw    string
}

func decodeOutput(t *testing.T, data []byte) decoded {
	t.Helper()
	d := decoded{
		spans:  map[string]export.SpanView{},
		groups: map[string]export.GroupView{},
	}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		var env []struct {
			Kind string          `json:"kind"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal([]byte(line), &env); err != nil {
			t.Fatalf("输出行不是批次 JSON 数组\n输入: %q\n错误: %v", line, err)
		}
		for _, rec := range env {
			switch rec.Kind {
			case "span":
				var v export.SpanView
				if err := json.Unmarshal(rec.Data, &v); err != nil {
					t.Fatalf("span JSON 解析失败: %v", err)
				}
				d.spans[v.SpanID] = v
			case "group":
				var v export.GroupView
				if err := json.Unmarshal(rec.Data, &v); err != nil {
					t.Fatalf("group JSON 解析失败: %v", err)
				}
				d.groups[v.GroupSpanID] = v
			case "log":
				var e logging.Entry
				if err := json.Unmarshal(rec.Data, &e); err != nil {
					t.Fatalf("log JSON 解析失败: %v", err)
				}
				d.logs = append(d.logs, e)
			}
		}
	}
	d.raw = string(data)
	return d
}

func newTestPipeline(t *testing.T, buf *bytes.Buffer, rate float64, capacity int) *obs.Pipeline {
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

func TestIntegrationConcurrentAttribution(t *testing.T) {
	var buf bytes.Buffer
	p := newTestPipeline(t, &buf, 1, 10000)
	const children = 25

	ctx, root := p.StartSpan(context.Background(), "parent", false)
	g := trace.NewGroup(ctx)
	for i := 0; i < children; i++ {
		g.Go("child", func(ctx context.Context, s trace.Span) {
			p.Logger().Info(ctx, "work", map[string]any{"token": "secret-token", "i": 1})
		})
	}
	g.Wait()
	root.End("")
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	d := decodeOutput(t, buf.Bytes())
	gotSpans := len(d.spans)
	if gotSpans != children+1 {
		t.Errorf("Span 数量错误\n输入: 1 父 + %d 子\n实际导出 span: %d\n期望: %d\n判定依据: 并发子 Span 不得遗漏/重复",
			children, gotSpans, children+1)
	}
	rootV, ok := d.spans[root.SpanID()]
	if !ok || rootV.ParentSpanID != "" {
		t.Errorf("根 Span 缺失或父 ID 非空: %#v", rootV)
	}
	childCount := 0
	for _, v := range d.spans {
		if v.Name == "child" {
			childCount++
			if v.TraceID != root.TraceID() || v.ParentSpanID != root.SpanID() || v.Depth != 1 {
				t.Errorf("子 Span 错挂\n输入: 子操作应归属 root\n实际: trace=%s parent=%s depth=%d\n期望: trace=%s parent=%s depth=1",
					v.TraceID, v.ParentSpanID, v.Depth, root.TraceID(), root.SpanID())
			}
			if v.EndReason != trace.ReasonCompleted {
				t.Errorf("子 Span 终止原因错误: %s", v.EndReason)
			}
		}
	}
	if childCount != children {
		t.Errorf("子 Span 计数错误: %d 期望 %d", childCount, children)
	}
	if len(d.logs) != children {
		t.Errorf("日志计数错误\n输入: %d 条\n实际: %d", children, len(d.logs))
	}
	for _, e := range d.logs {
		if e.ParentSpanID != root.SpanID() || e.TraceID != root.TraceID() || e.Depth != 1 {
			t.Errorf("日志串号: trace=%s parent=%s depth=%d", e.TraceID, e.ParentSpanID, e.Depth)
		}
		if e.Fields["token"] != logging.RedactedPlaceholder {
			t.Errorf("日志未脱敏: %#v", e.Fields)
		}
	}
	if strings.Contains(d.raw, "secret-token") {
		t.Errorf("导出数据中残留原始敏感值 secret-token")
	}
}

func TestIntegrationParentCancelPropagates(t *testing.T) {
	var buf bytes.Buffer
	p := newTestPipeline(t, &buf, 1, 10000)

	pctx, cancel := context.WithCancel(context.Background())
	ctx, root := p.StartSpan(pctx, "cancellable", false)
	g := trace.NewGroup(ctx)
	g.Go("blocked", func(ctx context.Context, s trace.Span) {
		p.Logger().Info(ctx, "waiting", nil)
		<-ctx.Done()
	})
	time.Sleep(30 * time.Millisecond)
	cancel()
	g.Wait()
	root.End("")
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	d := decodeOutput(t, buf.Bytes())
	var blocked *export.SpanView
	for _, v := range d.spans {
		vv := v
		if vv.Name == "blocked" {
			blocked = &vv
		}
	}
	if blocked == nil {
		t.Fatal("未找到 blocked 子 Span\n实际导出的 spans:\n" + spanNames(d.spans))
	}
	if blocked.EndReason != trace.ReasonParentCanceled {
		t.Errorf("终止原因错误\n输入: 父 ctx cancel()\n实际: %q\n期望: %q\n判定依据: 父取消必须同步传播并明确记录",
			blocked.EndReason, trace.ReasonParentCanceled)
	}
	if blocked.ParentSpanID != root.SpanID() || blocked.TraceID != root.TraceID() {
		t.Errorf("被取消的子 Span 归属错误: %#v", blocked)
	}
}

func TestIntegrationSamplingStableAndExplained(t *testing.T) {
	// 用独立计数器让 traceID 序列确定：每个根 trace 依次为 0001,0002,...
	run := func(rate float64) (map[string]string, []string) {
		var buf bytes.Buffer
		p := newTestPipeline(t, &buf, rate, 10000)
		var decisions []string
		for i := 0; i < 20; i++ {
			ctx, s := p.StartSpan(context.Background(), "t", false)
			_ = ctx
			decisions = append(decisions, s.Record().Attributes["sampling.decision_reason"])
			s.End("")
		}
		_ = p.Shutdown(context.Background())
		d := decodeOutput(t, buf.Bytes())
		out := map[string]string{}
		for _, v := range d.spans {
			out[v.TraceID] = v.Attributes["sampling.decision_reason"]
		}
		return out, decisions
	}

	exported1, _ := run(0.3)
	exported2, _ := run(0.3)
	if len(exported1) == 0 || len(exported1) != len(exported2) {
		t.Fatalf("采样结果不稳定\n输入: 同序列 20 个 trace, rate=0.3, 跑两次\n实际: 第一次导出 %d, 第二次 %d",
			len(exported1), len(exported2))
	}
	for k, v := range exported1 {
		if exported2[k] != v {
			t.Errorf("同一 traceID 两次决策不一致\ntrace=%s\n第一次: %s\n第二次: %s", k, v, exported2[k])
		}
	}
	for traceID, reason := range exported1 {
		if !strings.Contains(reason, "mix64(") && !strings.Contains(reason, "rate=") {
			t.Errorf("采样理由不可解释 trace=%s: %s", traceID, reason)
		}
	}

	// rate=0 不导出任何 span（日志也应被采样逻辑跳过）。
	var buf bytes.Buffer
	p := newTestPipeline(t, &buf, 0, 10000)
	ctx, s := p.StartSpan(context.Background(), "t", false)
	p.Logger().Info(ctx, "dropped", nil)
	s.End("")
	_ = p.Shutdown(context.Background())
	if strings.TrimSpace(buf.String()) != "" {
		t.Errorf("rate=0 时不应有任何导出\n实际: %s", buf.String())
	}
}

func TestIntegrationDropObservableAndShutdownComplete(t *testing.T) {
	var buf bytes.Buffer
	const capN = 8
	p := newTestPipeline(t, &buf, 1, capN)

	ctx, root := p.StartSpan(context.Background(), "burst", false)
	for i := 0; i < capN*10; i++ {
		p.Logger().Debug(ctx, "x", map[string]any{"i": i})
	}
	root.End("")
	stBefore := p.Exporter().Snapshot()
	if stBefore.DroppedNew == 0 {
		t.Fatalf("应有丢弃可观测\n输入: %d 条, 容量 %d\n实际 dropped_new=%d",
			capN*10, capN, stBefore.DroppedNew)
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	st := p.Exporter().Snapshot()
	if st.Enqueued != st.Exported {
		t.Errorf("收尾丢失已接受数据\n输入: enqueued=%d\n实际 exported=%d", st.Enqueued, st.Exported)
	}
	d := decodeOutput(t, buf.Bytes())
	if len(d.logs)+len(d.spans) != int(st.Exported) {
		t.Errorf("落盘记录数与 Exported 不符: %d vs %d", len(d.logs)+len(d.spans), st.Exported)
	}
}

func TestIntegrationConcurrentMixedLoadNoRaces(t *testing.T) {
	var buf bytes.Buffer
	p := newTestPipeline(t, &buf, 0.8, 64)
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx, root := p.StartSpan(context.Background(), "mixed", false)
			g := trace.NewGroup(ctx)
			for j := 0; j < 4; j++ {
				g.Go("sub", func(ctx context.Context, s trace.Span) {
					p.Logger().Info(ctx, "m", map[string]any{"password": "pw" + itoa(id*4+j), "id": j})
				})
			}
			g.Wait()
			root.End("")
		}(i)
	}
	wg.Wait()
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	// 所有 password 都必须脱敏，导出中不得出现 pw0..pw47 原始值。
	for i := 0; i < 48; i++ {
		if strings.Contains(out, "pw"+itoa(i)) {
			t.Fatalf("导出中残留原始密码 pw%d", i)
		}
	}
	if !strings.Contains(out, logging.RedactedPlaceholder) {
		t.Fatal("导出中未出现脱敏占位符，password 字段可能未被脱敏")
	}
}

func spanNames(m map[string]export.SpanView) string {
	var b strings.Builder
	for _, v := range m {
		b.WriteString("  - ")
		b.WriteString(v.Name)
		b.WriteString(" ")
		b.WriteString(v.EndReason)
		b.WriteString("\n")
	}
	return b.String()
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
