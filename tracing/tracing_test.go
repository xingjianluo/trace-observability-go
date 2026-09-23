package tracing_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/xingjianluo/trace-observability-go/tracing"
)

// rec is a concurrency-safe in-memory Recorder used by the tests.
type rec struct {
	mu    sync.Mutex
	spans []tracing.SpanData
}

func (r *rec) RecordSpan(d tracing.SpanData) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.spans = append(r.spans, d)
}

func (r *rec) snapshot() []tracing.SpanData {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]tracing.SpanData, len(r.spans))
	copy(out, r.spans)
	return out
}

func (r *rec) byID(id tracing.SpanID) (tracing.SpanData, bool) {
	for _, s := range r.snapshot() {
		if s.SpanID == id {
			return s, true
		}
	}
	return tracing.SpanData{}, false
}

// fatalf prints input, actual result and the rule being checked.
func fatalf(t *testing.T, input string, actual any, rule string, args ...any) {
	t.Helper()
	t.Fatalf("\ninput : %s\nactual: %v\nrule  : %s", input, actual, fmt.Sprintf(rule, args...))
}

func TestContextPropagationParentChild(t *testing.T) {
	r := &rec{}
	tr := tracing.New(r, tracing.WithIDs(tracing.NewCounterIDs(0, 0)))

	ctx, root := tr.Start(context.Background(), "root")
	ctx1, child1 := tr.Start(ctx, "child-1")
	_, grand := tr.Start(ctx1, "grand-1")
	_, child2 := tr.Start(ctx, "child-2")

	grand.End()
	child2.End()
	child1.End()
	root.End()

	got := r.snapshot()
	if len(got) != 4 {
		fatalf(t, "4 started spans", len(got), "all spans recorded exactly once")
	}
	byName := map[string]tracing.SpanData{}
	for _, s := range got {
		byName[s.Name] = s
	}
	// One trace, correct parent links, correct depths.
	trace := byName["root"].TraceID
	cases := []struct {
		name   string
		parent string
		depth  int
		reason tracing.EndReason
	}{
		{"root", "", 0, tracing.ReasonFinished},
		{"child-1", "root", 1, tracing.ReasonFinished},
		{"child-2", "root", 1, tracing.ReasonFinished},
		{"grand-1", "child-1", 2, tracing.ReasonFinished},
	}
	for _, c := range cases {
		s := byName[c.name]
		if s.TraceID != trace {
			fatalf(t, c.name, s.TraceID.Hex(), "must share trace id %s", trace.Hex())
		}
		if s.Depth != c.depth {
			fatalf(t, c.name, s.Depth, "depth must be %d", c.depth)
		}
		if s.EndReason != c.reason {
			fatalf(t, c.name, s.EndReason, "normal End records %s", c.reason)
		}
		if c.parent != "" {
			if s.ParentSpanID != byName[c.parent].SpanID {
				fatalf(t, c.name, s.ParentSpanID.Hex(),
					"parent must be %s (%s)", c.parent, byName[c.parent].SpanID.Hex())
			}
		} else if s.ParentSpanID != (tracing.SpanID{}) {
			fatalf(t, c.name, s.ParentSpanID.Hex(), "root must have zero parent id")
		}
	}
}

func TestConcurrentFanoutAttribution(t *testing.T) {
	r := &rec{}
	tr := tracing.New(r, tracing.WithIDs(tracing.NewCounterIDs(0, 0)))

	const n = 50
	ctx, root := tr.Start(context.Background(), "root")

	joins := make([]<-chan struct{}, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("child-%02d", i)
		joins[i] = tr.Go(ctx, name, func(ctx context.Context, sp tracing.Span) {
			// Each child starts its own grandchild to stress
			// multi-level concurrent linking.
			g := tr.Go(ctx, "grand", func(ctx context.Context, sp tracing.Span) {
				sp.SetAttribute("i", i)
			})
			<-g
			sp.SetAttribute("i", i)
		})
	}
	for _, j := range joins {
		<-j
	}
	root.End()

	got := r.snapshot()
	want := 1 + 2*n
	if len(got) != want {
		fatalf(t, fmt.Sprintf("1 root + %d children + %d grandchildren", n, n),
			len(got), "no span may be lost or double counted")
	}

	rootSpan := findByName(got, "root")
	idCount := map[tracing.SpanID]int{}
	for _, s := range got {
		idCount[s.SpanID]++
	}
	for id, c := range idCount {
		if c != 1 {
			fatalf(t, "span "+id.Hex(), c, "every span id occurs exactly once")
		}
		if findSpan(got, id).TraceID != rootSpan.TraceID {
			fatalf(t, id.Hex(), findSpan(got, id).TraceID.Hex(),
				"every concurrent span belongs to the root trace %s", rootSpan.TraceID.Hex())
		}
	}
	children, grands := 0, 0
	for _, s := range got {
		switch s.Name {
		case "root":
			if s.ParentSpanID != (tracing.SpanID{}) {
				fatalf(t, "root", s.ParentSpanID.Hex(), "root has no parent")
			}
		case "grand":
			grands++
			if s.Depth != 2 {
				fatalf(t, s.Name, s.Depth, "grandchild depth = 2")
			}
			parent, ok := r.byID(s.ParentSpanID)
			if !ok || parent.Name != "child-"+fmt.Sprintf("%02d", s.Attributes["i"].(int)) {
				// Grandchild's parent is the child started in the same
				// loop iteration; attribute i proves no cross-wiring.
				pi, ok := r.byID(s.ParentSpanID)
				fatalf(t, "grand attr.i="+fmt.Sprint(s.Attributes["i"]),
					fmt.Sprintf("parent=%q ok=%v", pi.Name, ok),
					"grandchild must hang under the same iteration's child")
			}
		default:
			children++
			if s.Depth != 1 {
				fatalf(t, s.Name, s.Depth, "child depth = 1")
			}
			if s.ParentSpanID != rootSpan.SpanID {
				fatalf(t, s.Name, s.ParentSpanID.Hex(),
					"child parent must be root %s", rootSpan.SpanID.Hex())
			}
		}
	}
	if children != n || grands != n {
		fatalf(t, fmt.Sprintf("%d children, %d grands", n, n),
			fmt.Sprintf("%d children, %d grands", children, grands),
			"counts must match fanout")
	}
}

func TestParentCancellationPropagates(t *testing.T) {
	r := &rec{}
	tr := tracing.New(r, tracing.WithIDs(tracing.NewCounterIDs(0, 0)))

	parentCtx, cancel := context.WithCancel(context.Background())
	ctx, root := tr.Start(parentCtx, "root")

	cancelledChildEnded := make(chan struct{})
	j1 := tr.Go(ctx, "cancelled-child", func(ctx context.Context, sp tracing.Span) {
		<-ctx.Done() // simulate cooperative work noticing cancellation
		<-sp.Done()  // span must be ended by propagated cancellation
		close(cancelledChildEnded)
	})
	j2 := tr.Go(ctx, "self-finishing-child", func(ctx context.Context, sp tracing.Span) {
		// returns immediately; Go ends the span before cancellation
	})

	cancel() // parent operation cancelled
	<-j1
	<-j2
	root.End()

	got := r.snapshot()
	byName := map[string]tracing.SpanData{}
	for _, s := range got {
		byName[s.Name] = s
	}
	cc, ok := byName["cancelled-child"]
	if !ok {
		fatalf(t, "cancelled-child present", r.snapshot(), "span must be recorded")
	}
	if cc.EndReason != tracing.ReasonParentCancel {
		fatalf(t, "cancel() while child alive", cc.EndReason,
			"child terminated by parent cancellation records %s", tracing.ReasonParentCancel)
	}
	sf := byName["self-finishing-child"]
	// The child's fn returned before cancel(); End was scheduled by
	// Go's defer. The watcher teardown and cancel() can race, so the
	// deterministic contract is: the span ends exactly once with a
	// valid reason, never double-recorded or orphaned.
	if sf.EndReason != tracing.ReasonFinished && sf.EndReason != tracing.ReasonParentCancel {
		fatalf(t, "child fn returned before cancel()", sf.EndReason,
			"reason is %q or %q", tracing.ReasonFinished, tracing.ReasonParentCancel)
	}
	// The root itself observed the caller context cancellation too,
	// but explicit End won the race on purpose; either finished or
	// context_cancelled is a valid root outcome, never a crash/double
	// record.
	if len(got) != 3 {
		fatalf(t, "3 spans", len(got), "cancellation records each span exactly once")
	}
}

func TestEndIsIdempotent(t *testing.T) {
	r := &rec{}
	tr := tracing.New(r)
	_, sp := tr.Start(context.Background(), "x")
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); sp.End() }()
	}
	wg.Wait()
	if len(r.snapshot()) != 1 {
		fatalf(t, "20 concurrent End calls", len(r.snapshot()), "exactly one record produced")
	}
}

func TestDeterministicCounterIDs(t *testing.T) {
	a := tracing.NewCounterIDs(0, 0)
	b := tracing.NewCounterIDs(0, 0)
	if a.NewTraceID() != b.NewTraceID() || a.NewSpanID() != b.NewSpanID() {
		fatalf(t, "two fresh CounterIDs(0,0)", "differ",
			"deterministic ids must reproduce across instances")
	}
	tr := tracing.New(recNop(), tracing.WithIDs(tracing.NewCounterIDs(0, 0)))
	_, sp := tr.Start(context.Background(), "r")
	sc := sp.SpanContext()
	wantTrace := "00000000000000000000000000000001"
	wantSpan := "0000000000000001"
	if sc.TraceID.Hex() != wantTrace || sc.SpanID.Hex() != wantSpan {
		fatalf(t, "CounterIDs(0,0) first root", sc,
			"trace=%s span=%s", wantTrace, wantSpan)
	}
}

type nopRec struct{}

func (nopRec) RecordSpan(tracing.SpanData) {}
func recNop() tracing.Recorder             { return nopRec{} }

func findByName(ss []tracing.SpanData, name string) tracing.SpanData {
	for _, s := range ss {
		if s.Name == name {
			return s
		}
	}
	return tracing.SpanData{}
}

func findSpan(ss []tracing.SpanData, id tracing.SpanID) tracing.SpanData {
	for _, s := range ss {
		if s.SpanID == id {
			return s
		}
	}
	return tracing.SpanData{}
}

func TestGoChildEndedAfterParentCancel(t *testing.T) {
	r := &rec{}
	tr := tracing.New(r, tracing.WithIDs(tracing.NewCounterIDs(0, 0)))

	parentCtx, cancel := context.WithCancel(context.Background())
	ctx, root := tr.Start(parentCtx, "root")

	started := make(chan struct{})
	done := tr.Go(ctx, "child", func(ctx context.Context, sp tracing.Span) {
		// Signal that the child goroutine is alive, then wait for
		// cancellation. Go's deferred End races the internal
		// watcher, but the recorded reason must still be
		// parent_cancelled because the context is visibly cancelled.
		close(started)
		<-ctx.Done()
	})
	<-started
	cancel()
	<-done
	root.End()

	var child tracing.SpanData
	for _, sp := range r.snapshot() {
		if sp.Name == "child" {
			child = sp
		}
	}
	if child.EndReason != tracing.ReasonParentCancel {
		t.Fatalf("\ninput : Go child alive when parent cancel()\nactual: %s\nrule  : reason parent_cancelled even if deferred End wins watcher race",
			child.EndReason)
	}
}
