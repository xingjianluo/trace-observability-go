package pipeline_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xingjianluo/trace-observability-go/export"
	"github.com/xingjianluo/trace-observability-go/logx"
	"github.com/xingjianluo/trace-observability-go/pipeline"
	"github.com/xingjianluo/trace-observability-go/sampler"
	"github.com/xingjianluo/trace-observability-go/tracing"
)

// decodeJSONL splits the exporter buffer into parsed records.
func decodeJSONL(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var v map[string]any
		if err := json.Unmarshal(line, &v); err != nil {
			t.Fatalf("\ninput : exported line\nactual: %s\nrule  : valid JSON: %v", line, err)
		}
		out = append(out, v)
	}
	return out
}

func TestEndToEndFanoutAndCancellation(t *testing.T) {
	var buf bytes.Buffer
	p := pipeline.New(export.NewFileExporter(&buf), pipeline.Config{
		IDs:           tracing.NewCounterIDs(0, 0),
		QueueCapacity: 64,
		Policy:        export.PolicyBlock,
		Batch:         export.BatcherConfig{MaxBatchSize: 8, FlushInterval: 20 * time.Millisecond},
		Log:           logx.Config{MinLevel: logx.LevelDebug},
	})

	pctx, cancel := context.WithCancel(context.Background())
	ctx, root := p.Tracer().Start(pctx, "root")

	var joins []<-chan struct{}
	// Cancellation branches: the goroutine blocks on ctx.Done() and
	// ends its own span only after observing cancellation, so the
	// recorded reason is deterministically parent_cancelled.
	started := make(chan struct{}, 3)
	for i := 0; i < 3; i++ {
		i := i
		cctx, ccancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			cctx2, sp := p.Tracer().Start(cctx, fmt.Sprintf("cancel-%d", i))
			started <- struct{}{}
			<-cctx2.Done()
			p.Logger().Warn(cctx2, "stopping")
			sp.End()
		}()
		<-started
		ccancel()
		joins = append(joins, done)
	}
	for i := 3; i < 10; i++ {
		i := i
		joins = append(joins, p.Tracer().Go(ctx, fmt.Sprintf("ok-%d", i),
			func(ctx context.Context, sp tracing.Span) {
				p.Logger().Info(ctx, "working", logx.F("i", i))
			}))
	}
	for _, j := range joins {
		<-j
	}
	root.End()
	cancel()

	ctxS, c := context.WithTimeout(context.Background(), 2*time.Second)
	defer c()
	if err := p.Shutdown(ctxS); err != nil {
		t.Fatal(err)
	}

	recs := decodeJSONL(t, &buf)
	var spans, logs int
	traceIDs := map[string]int{}
	reasonCount := map[string]int{}
	parentLinks := map[string]string{} // span -> parent
	spanNames := map[string]map[string]any{}
	for _, rec := range recs {
		payload := rec["payload"].(map[string]any)
		switch rec["kind"] {
		case "span":
			spans++
			tid := payload["trace_id"].(string)
			traceIDs[tid]++
			reasonCount[payload["end_reason"].(string)]++
			parentLinks[payload["span_id"].(string)] = payload["parent_span_id"].(string)
			spanNames[payload["name"].(string)] = payload
		case "log":
			logs++
			if payload["trace_id"] == nil || payload["trace_id"] == "" {
				t.Fatalf("\ninput : every log emitted under a span\nactual: %v\nrule  : trace_id present", payload)
			}
			traceIDs[payload["trace_id"].(string)]++
		}
	}
	if spans != 11 { // root + 10 children
		t.Fatalf("\ninput : 11 spans\nactual: %d spans in %d records\nrule  : every span exported once",
			spans, len(recs))
	}
	if len(traceIDs) != 1 {
		t.Fatalf("\ninput : one fanout trace\nactual: trace ids %v\nrule  : all records share one trace", keys(traceIDs))
	}
	if reasonCount["parent_cancelled"] != 3 {
		t.Fatalf("\ninput : 3 cancelled branches\nactual: reasons=%v\nrule  : exactly 3 parent_cancelled",
			reasonCount)
	}
	if reasonCount["finished"] != 8 {
		t.Fatalf("\ninput : 7 normal children + root\nactual: reasons=%v\nrule  : 8 finished", reasonCount)
	}
	// Every child's parent is the root span.
	rootSpan := spanNames["root"]
	for name, sp := range spanNames {
		if name == "root" {
			continue
		}
		if sp["parent_span_id"] != rootSpan["span_id"] {
			t.Fatalf("\ninput : child %s\nactual: parent=%s\nrule  : parent is root %s",
				name, sp["parent_span_id"], rootSpan["span_id"])
		}
	}
	if p.Queue().Stats().Dropped != 0 {
		t.Fatalf("\ninput : block policy, adequate capacity\nactual: dropped=%d\nrule  : no drops",
			p.Queue().Stats().Dropped)
	}
}

func TestBackpressureDropsAreObservable(t *testing.T) {
	var buf bytes.Buffer
	// Capacity 1 plus a blocking exporter guarantees overflow while
	// the test hammers the queue with DropNewest.
	gate := make(chan struct{})
	ex := gateExporter{w: export.NewFileExporter(&buf), gate: gate}
	p := pipeline.New(ex, pipeline.Config{
		QueueCapacity: 1,
		Policy:        export.PolicyDropNewest,
		Batch:         export.BatcherConfig{MaxBatchSize: 1, FlushInterval: time.Hour},
	})

	tr := p.Tracer()
	drops := p.Queue().Drops()
	var seenDrops atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, sp := tr.Start(context.Background(), fmt.Sprintf("s%d", i))
			sp.End() // RecordSpan -> Enqueue, overflow may drop
		}(i)
	}
	obsDone := make(chan struct{})
	go func() {
		defer close(obsDone)
		for ev := range drops {
			if ev.Reason == export.DropQueueFull {
				seenDrops.Add(1)
			}
		}
	}()
	wg.Wait()

	stats := p.Queue().Stats()
	close(gate)
	ctxS, c := context.WithTimeout(context.Background(), 2*time.Second)
	defer c()
	_ = p.Shutdown(ctxS)

	if stats.Dropped == 0 {
		t.Fatalf("\ninput : capacity=1, blocking exporter, 200 spans\nactual: dropped=0\nrule  : overflow must happen and be counted")
	}
	if stats.Accepted+stats.Dropped != 200 {
		t.Fatalf("\ninput : 200 offers\nactual: accepted=%d dropped=%d\nrule  : every offer is accounted for",
			stats.Accepted, stats.Dropped)
	}
	// The drop channel is buffered; when observed events catch up
	// to the counter we know every notification has been delivered.
	deadline := time.Now().Add(time.Second)
	for seenDrops.Load() < stats.Dropped && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if seenDrops.Load() == 0 {
		t.Fatalf("\ninput : %d dropped items\nactual: observed drop events=%d\nrule  : drops are observable with reasons",
			stats.Dropped, seenDrops.Load())
	}
}

type gateExporter struct {
	w    *export.FileExporter
	gate chan struct{}
}

func (g gateExporter) Export(ctx context.Context, batch []export.Envelope) error {
	select {
	case <-g.gate:
	case <-ctx.Done():
		return ctx.Err()
	}
	return g.w.Export(ctx, batch)
}

func TestSamplingDecisionIsStable(t *testing.T) {
	count := func(ratio float64) int {
		var buf bytes.Buffer
		p := pipeline.New(export.NewFileExporter(&buf), pipeline.Config{
			IDs:           tracing.NewCounterIDs(0, 0),
			QueueCapacity: 256,
			Sampler:       sampler.NewRatio(ratio),
			Batch:         export.BatcherConfig{FlushInterval: 10 * time.Millisecond},
		})
		for i := 0; i < 40; i++ {
			_, sp := p.Tracer().Start(context.Background(), "root")
			sp.End()
		}
		ctxS, c := context.WithTimeout(context.Background(), time.Second)
		defer c()
		if err := p.Shutdown(ctxS); err != nil {
			t.Fatal(err)
		}
		return len(decodeJSONL(t, &buf))
	}
	if got := count(0); got != 0 {
		t.Fatalf("\ninput : ratio=0, 40 roots\nactual: %d exported\nrule  : nothing sampled", got)
	}
	if got := count(1); got != 40 {
		t.Fatalf("\ninput : ratio=1, 40 roots\nactual: %d exported\nrule  : everything sampled", got)
	}
	// Determinism: the same CounterIDs seed reproduces the same count.
	a, b := count(0.5), count(0.5)
	if a != b {
		t.Fatalf("\ninput : identical seeds, ratio=0.5\nactual: %d vs %d\nrule  : sampling is reproducible", a, b)
	}
}

func TestRedactionEndToEndNoLeak(t *testing.T) {
	var buf bytes.Buffer
	r, err := logx.NewRedactor([]logx.RedactRule{
		{KeySuffix: "password"},
		{ValuePattern: `AKIA[0-9A-Z]{8}`},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	p := pipeline.New(export.NewFileExporter(&buf), pipeline.Config{
		QueueCapacity: 8,
		Redactor:      r,
		Batch:         export.BatcherConfig{FlushInterval: 10 * time.Millisecond},
		Log:           logx.Config{},
	})
	ctx, sp := p.Tracer().Start(context.Background(), "op")
	p.Logger().Info(ctx, "call",
		logx.F("db_password", "hunter2"),
		logx.F("key", "AKIAABCD1234"),
		logx.F("user", "alice"))
	sp.End()
	ctxS, c := context.WithTimeout(context.Background(), time.Second)
	defer c()
	if err := p.Shutdown(ctxS); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, leak := range []string{"hunter2", "AKIAABCD1234"} {
		if strings.Contains(out, leak) {
			t.Fatalf("\ninput : sensitive %q logged\nactual: %s\nrule  : no raw value survives into export", leak, out)
		}
	}
}

func TestShutdownRejectsNewDataAndFlushesAccepted(t *testing.T) {
	var buf bytes.Buffer
	p := pipeline.New(export.NewFileExporter(&buf), pipeline.Config{
		QueueCapacity: 4,
		Batch:         export.BatcherConfig{MaxBatchSize: 2, FlushInterval: time.Hour},
	})
	for i := 0; i < 4; i++ {
		_, sp := p.Tracer().Start(context.Background(), fmt.Sprintf("s%d", i))
		sp.End()
	}
	ctxS, c := context.WithTimeout(context.Background(), time.Second)
	defer c()
	if err := p.Shutdown(ctxS); err != nil {
		t.Fatal(err)
	}
	// After shutdown, new spans are still built but their envelopes
	// are rejected (closed), never silently accepted.
	_, sp := p.Tracer().Start(context.Background(), "late")
	sp.End()
	recs := decodeJSONL(t, &buf)
	if len(recs) != 4 {
		t.Fatalf("\ninput : 4 accepted spans, then shutdown, then 1 late\nactual: %d exported\nrule  : accepted flushed; late rejected",
			len(recs))
	}
}

func TestExportFailureReported(t *testing.T) {
	p := pipeline.New(export.ExporterFunc(func(context.Context, []export.Envelope) error {
		return errors.New("disk full")
	}), pipeline.Config{
		QueueCapacity: 2,
		Batch: export.BatcherConfig{
			MaxBatchSize: 1, FlushInterval: time.Hour,
			MaxRetries: 1, RetryBase: time.Millisecond, ExportTimeout: time.Second,
		},
	})
	_, sp := p.Tracer().Start(context.Background(), "s")
	sp.End()
	ctxS, c := context.WithTimeout(context.Background(), 2*time.Second)
	defer c()
	err := p.Shutdown(ctxS)
	if err == nil {
		t.Fatalf("\ninput : exporter always failing\nactual: nil error\nrule  : shutdown surfaces the export error")
	}
	if p.Batcher().Stats().Failed == 0 {
		t.Fatalf("\ninput : permanently failing export\nactual: failed=%d\nrule  : failed envelopes counted",
			p.Batcher().Stats().Failed)
	}
}

func keys(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
