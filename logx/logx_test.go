package logx_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/xingjianluo/trace-observability-go/logx"
	"github.com/xingjianluo/trace-observability-go/tracing"
)

type collectingSink struct {
	mu      sync.Mutex
	entries []logx.Entry
}

func (c *collectingSink) Write(e logx.Entry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = append(c.entries, e)
}

func (c *collectingSink) all() []logx.Entry {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]logx.Entry, len(c.entries))
	copy(out, c.entries)
	return out
}

func TestLogCarriesCorrelationAndDepth(t *testing.T) {
	sink := &collectingSink{}
	lg := logx.New(sink, nil, logx.Config{})
	tr := tracing.New(tracing.RecorderFunc(func(tracing.SpanData) {}),
		tracing.WithIDs(tracing.NewCounterIDs(7, 0)))

	ctx, root := tr.Start(context.Background(), "root")
	_, child := tr.Start(ctx, "child")

	lg.Info(child.Context(), "hi", logx.F("k", "v"))
	got := sink.all()[0]
	sc := child.SpanContext()
	if got.TraceID != sc.TraceID.Hex() || got.SpanID != sc.SpanID.Hex() {
		t.Fatalf("\ninput : log inside child span\nactual: trace=%s span=%s\nrule  : ids must equal child %s/%s",
			got.TraceID, got.SpanID, sc.TraceID.Hex(), sc.SpanID.Hex())
	}
	if got.ParentID != root.SpanContext().SpanID.Hex() {
		t.Fatalf("\ninput : child depth 1\nactual: parent=%s\nrule  : parent id equals root %s",
			got.ParentID, root.SpanContext().SpanID.Hex())
	}
	if got.Depth != 1 {
		t.Fatalf("\ninput : child depth 1\nactual: %d\nrule  : depth = 1", got.Depth)
	}
}

func TestRedactionLeavesNoRawSensitiveValue(t *testing.T) {
	sink := &collectingSink{}
	r, err := logx.NewRedactor([]logx.RedactRule{
		{Keys: []string{"password", "api_key"}},
		{KeySuffix: "token"},
		{ValuePattern: `Bearer [A-Za-z0-9\.\-]+`},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	lg := logx.New(sink, r, logx.Config{})

	secret := "super-secret-value-42"
	tr := tracing.New(tracing.RecorderFunc(func(tracing.SpanData) {}))
	ctx, sp := tr.Start(context.Background(), "op")
	lg.Info(ctx, "login",
		logx.F("user", "bob"),
		logx.F("password", secret),
		logx.F("api_key", "sk-live-999"),
		logx.F("sessionToken", "tok-xyz"),
		logx.F("authHeader", "Bearer eyJhbGciOi.JIUzI1NiJ9.payload"),
		logx.F("nested", map[string]any{
			"refresh_token": "rt-1",
			"note":          "token shown as Bearer abc.def in text",
			"ok":            1,
		}),
		logx.F("list", []any{"Bearer aaa.bbb", "plain"}),
	)
	sp.End()

	var buf bytes.Buffer
	for _, e := range sink.all() {
		buf.Write(logx.AppendJSON(nil, e))
	}
	out := buf.String()

	rawFragments := []string{secret, "sk-live-999", "tok-xyz", "eyJhbGciOi",
		"rt-1", "aaa.bbb", "abc.def"}
	for _, frag := range rawFragments {
		if strings.Contains(out, frag) {
			t.Fatalf("\ninput : redacted entry containing %q\nactual: %s\nrule  : raw sensitive fragment must never reach sink",
				frag, out)
		}
	}
	if !strings.Contains(out, logx.DefaultMask) {
		t.Fatalf("\ninput : sensitive fields\nactual: %s\nrule  : mask token must be present", out)
	}
	if !strings.Contains(out, `"user"`) || !strings.Contains(out, "bob") {
		t.Fatalf("\ninput : non-sensitive field user=bob\nactual: %s\nrule  : non-sensitive values pass through", out)
	}
}

func TestRedactionDoesNotMutateInput(t *testing.T) {
	r, _ := logx.NewRedactor([]logx.RedactRule{{Key: "password"}}, "")
	in := logx.Entry{Fields: []logx.Field{{Key: "password", Value: "raw"}}}
	out := r.Apply(in)
	if in.Fields[0].Value != "raw" {
		t.Fatalf("\ninput : Apply(entry)\nactual: original mutated to %v\nrule  : input entry stays untouched",
			in.Fields[0].Value)
	}
	if out.Fields[0].Value != logx.DefaultMask {
		t.Fatalf("\ninput : Apply(entry)\nactual: %v\nrule  : copy is redacted", out.Fields[0].Value)
	}
}

func TestConcurrentLogsDoNotCrossWires(t *testing.T) {
	sink := &collectingSink{}
	lg := logx.New(sink, nil, logx.Config{})
	tr := tracing.New(tracing.RecorderFunc(func(tracing.SpanData) {}),
		tracing.WithIDs(tracing.NewCounterIDs(0, 0)))

	const n = 100
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, sp := tr.Start(context.Background(), fmt.Sprintf("op-%d", i))
			// Each log entry must carry the span under which it was
			// emitted. The field marker equals the span's own id.
			sc := sp.SpanContext()
			lg.Info(ctx, "msg", logx.F("marker", sc.SpanID.Hex()))
			sp.End()
		}(i)
	}
	wg.Wait()

	got := sink.all()
	if len(got) != n {
		t.Fatalf("\ninput : %d goroutines, 1 log each\nactual: %d entries\nrule  : no lost or duplicated logs",
			n, len(got))
	}
	seen := map[string]bool{}
	for _, e := range got {
		var marker string
		for _, f := range e.Fields {
			if f.Key == "marker" {
				marker, _ = f.Value.(string)
			}
		}
		if marker == "" || marker != e.SpanID {
			t.Fatalf("\ninput : marker set to own span id\nactual: span=%s marker=%s\nrule  : concurrent logs must not cross wires",
				e.SpanID, marker)
		}
		if seen[e.SpanID] {
			t.Fatalf("\ninput : unique span per goroutine\nactual: repeated span id %s\nrule  : no double counting", e.SpanID)
		}
		seen[e.SpanID] = true
	}
}

func TestMinLevelFiltering(t *testing.T) {
	sink := &collectingSink{}
	lg := logx.New(sink, nil, logx.Config{MinLevel: logx.LevelWarn})
	lg.Debug(context.Background(), "d")
	lg.Info(context.Background(), "i")
	lg.Warn(context.Background(), "w")
	if len(sink.all()) != 1 || sink.all()[0].Message != "w" {
		msgs := []string{}
		for _, e := range sink.all() {
			msgs = append(msgs, e.Message)
		}
		t.Fatalf("\ninput : MinLevel=Warn, debug/info/warn\nactual: %v\nrule  : only warn emitted", msgs)
	}
}

func TestJSONOutputIsValidJSONL(t *testing.T) {
	sink := &collectingSink{}
	lg := logx.New(sink, nil, logx.Config{})
	tr := tracing.New(tracing.RecorderFunc(func(tracing.SpanData) {}))
	ctx, _ := tr.Start(context.Background(), "op")
	lg.Info(ctx, "hello", logx.F("n", 3), logx.F("b", true))

	var buf bytes.Buffer
	for _, e := range sink.all() {
		buf.Write(logx.AppendJSON(nil, e))
	}
	line := bytes.TrimRight(buf.Bytes(), "\n")
	var v map[string]any
	if err := json.Unmarshal(line, &v); err != nil {
		t.Fatalf("\ninput : AppendJSON output\nactual: %q\nrule  : must be valid JSON: %v", line, err)
	}
	if v["level"] != "info" {
		t.Fatalf("\ninput : Info entry\nactual: level=%v\nrule  : level serialized as string", v["level"])
	}
}
