package logging

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/xingjianluo/trace-observability-go/trace"
)

type memSink struct {
	mu      sync.Mutex
	entries []Entry
}

func (m *memSink) OnLog(e Entry) {
	m.mu.Lock()
	m.entries = append(m.entries, e)
	m.mu.Unlock()
}

func (m *memSink) all() []Entry {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Entry, len(m.entries))
	copy(out, m.entries)
	return out
}

func TestLogCarriesTraceCorrelation(t *testing.T) {
	trace.SetIDSource(trace.NewCounterIDSource())
	sink := &memSink{}
	l := NewLogger(sink, nil)

	ctx, s := trace.StartSpan(context.Background(), "op")
	l.Info(ctx, "hello", map[string]any{"k": "v"})
	s.End("")

	got := sink.all()
	if len(got) != 1 {
		t.Fatalf("日志条数不符\n输入: 1 条 Info\n实际: %d 条\n判定依据: 每条 Emit 恰好产生一条 Entry", len(got))
	}
	e := got[0]
	if e.TraceID != s.TraceID() || e.SpanID != s.SpanID() ||
		e.ParentSpanID != s.ParentSpanID() || e.Depth != s.Depth() {
		t.Errorf("关联字段错误\n输入: span(trace=%s,id=%s,depth=%d)\n实际: trace=%s span=%s parent=%s depth=%d\n判定依据: 日志必须携带所属 Span 的关联标识",
			s.TraceID(), s.SpanID(), s.Depth(), e.TraceID, e.SpanID, e.ParentSpanID, e.Depth)
	}
}

func TestMaskingNoRawValueLeak(t *testing.T) {
	rules := DefaultRules()
	m := NewMasker(rules)
	sink := &memSink{}
	l := NewLogger(sink, m)

	secrets := map[string]string{
		"password": "supersecret",
		"token":    "tok_live_abcdef",
		"api_key":  "AKIA1234567890",
		"email":    "alice@example.com",
	}
	fields := map[string]any{
		"password": secrets["password"],
		"token":    secrets["token"],
		"api_key":  secrets["api_key"],
		"email":    secrets["email"],
		"nested":   map[string]any{"phone": "13800138000", "ok": "keep"},
		"list":     []any{map[string]any{"secret": "ssss"}, 3},
		"amount":   99.5,
	}
	orig := map[string]any{"password": "supersecret"}
	_ = orig
	l.Info(context.Background(), "secure", fields)

	e := sink.all()[0]
	for name, raw := range secrets {
		if strings.Contains(flatten(e.Fields), raw) {
			t.Errorf("脱敏后残留原始值\n输入: 字段 %s=%q\n实际输出: %#v\n判定依据: 脱敏后任何位置都不得出现原始敏感值",
				name, raw, e.Fields)
		}
	}
	if e.Fields["amount"] != 99.5 {
		t.Errorf("非敏感字段被改动\n输入: amount=99.5\n实际: %#v", e.Fields["amount"])
	}
	nested := e.Fields["nested"].(map[string]any)
	if nested["ok"] != "keep" {
		t.Errorf("嵌套非敏感字段被误脱敏: %#v", nested)
	}
	if nested["phone"] == "13800138000" {
		t.Errorf("嵌套敏感字段未脱敏: %#v", nested["phone"])
	}
	// 原始输入 map 不得被修改
	if fields["token"] != secrets["token"] {
		t.Errorf("脱敏修改了原始 map\n输入: token=%q\n实际: %q\n判定依据: MaskFields 必须复制后处理",
			secrets["token"], fields["token"])
	}
}

func TestPartialAndHashMasks(t *testing.T) {
	m := NewMasker(DefaultRules())
	out := m.MaskFields(map[string]any{
		"password": "supersecret",
		"user":     map[string]any{"id": "user-777"},
		"shortpw":  "abc",
	})
	got := out["password"].(string)
	if !strings.Contains(got, "*") || strings.Contains(got, "supersecret") {
		t.Errorf("部分打码错误\n输入: password=supersecret\n实际: %q\n期望: 含掩码且不含原值", got)
	}
	// user.id 配置的是点号字面键；直接验证 MaskFields 对 "user.id" 键做哈希
	flat := m.MaskFields(map[string]any{"user.id": "user-777"})
	h, ok := flat["user.id"].(string)
	if !ok || !strings.HasPrefix(h, "sha256:") {
		t.Errorf("哈希脱敏错误\n输入: user.id=user-777\n实际: %#v\n期望: sha256: 前缀", flat["user.id"])
	}
	// 相同输入+盐必须稳定
	flat2 := m.MaskFields(map[string]any{"user.id": "user-777"})
	if flat["user.id"] != flat2["user.id"] {
		t.Errorf("哈希脱敏不稳定: %q vs %q", flat["user.id"], flat2["user.id"])
	}
	_ = out
}

func TestConcurrentLogsNoCrossTalk(t *testing.T) {
	trace.SetIDSource(trace.NewCounterIDSource())
	sink := &memSink{}
	l := NewLogger(sink, NewMasker(DefaultRules()))

	const n = 32
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx, s := trace.StartSpan(context.Background(), "worker")
			defer s.End("")
			l.Info(ctx, "msg", map[string]any{
				"token": "tok-" + pad(id),
				"wid":   id,
			})
		}(i)
	}
	wg.Wait()

	entries := sink.all()
	if len(entries) != n {
		t.Fatalf("并发日志条数错误\n输入: %d 个协程各 1 条\n实际: %d 条", n, len(entries))
	}
	seen := map[string]bool{}
	for _, e := range entries {
		if seen[e.SpanID] {
			t.Errorf("日志串号/重复\n实际: span_id=%s 出现多次\n判定依据: 每个协程独立 Span，日志必须一一对应", e.SpanID)
		}
		seen[e.SpanID] = true
		if e.Fields["token"] != RedactedPlaceholder {
			t.Errorf("并发日志脱敏失败 span=%s token=%#v", e.SpanID, e.Fields["token"])
		}
		wid := int(e.Fields["wid"].(int))
		if wid < 0 || wid >= n {
			t.Errorf("日志字段串号 span=%s wid=%v", e.SpanID, wid)
		}
	}
}

func TestSampledOutLogsDropped(t *testing.T) {
	sink := &memSink{}
	l := NewLogger(sink, nil)
	ctx, s := trace.StartSpan(context.Background(), "dropped",
		trace.WithFinalSampling(trace.SamplingDrop))
	l.Info(ctx, "should not appear", nil)
	s.End("")
	if len(sink.all()) != 0 {
		t.Errorf("采样丢弃链路仍产生日志\n输入: sampling=drop\n实际: %d 条\n判定依据: drop 链路日志必须被跳过", len(sink.all()))
	}
}

func flatten(v any) string {
	switch x := v.(type) {
	case map[string]any:
		var b strings.Builder
		for _, vv := range x {
			b.WriteString(flatten(vv))
			b.WriteByte('|')
		}
		return b.String()
	case []any:
		var b strings.Builder
		for _, vv := range x {
			b.WriteString(flatten(vv))
			b.WriteByte('|')
		}
		return b.String()
	case string:
		return x
	default:
		return ""
	}
}

func pad(i int) string {
	s := ""
	for len(s)+len(itoa(i)) < 8 {
		s += "0"
	}
	return s + itoa(i)
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
