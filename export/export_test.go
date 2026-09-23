package export_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xingjianluo/trace-observability-go/export"
)

func env(i int) export.Envelope {
	return export.Envelope{Kind: export.KindLog, Timestamp: int64(i), Payload: i}
}

func TestDropNewestCountsAndReportsReasons(t *testing.T) {
	q := export.NewQueue(2, export.PolicyDropNewest)
	for i := 0; i < 5; i++ {
		if err := q.Enqueue(context.Background(), env(i)); err != nil {
			t.Fatal(err)
		}
	}
	stats := q.Stats()
	if stats.Accepted != 2 || stats.Dropped != 3 {
		t.Fatalf("\ninput : capacity=2, 5 offers, DropNewest\nactual: accepted=%d dropped=%d\nrule  : 2 accepted, 3 dropped",
			stats.Accepted, stats.Dropped)
	}
	gotDrops := 0
drain:
	for {
		select {
		case ev := <-q.Drops():
			if ev.Reason != export.DropQueueFull {
				t.Fatalf("\ninput : overflow drop\nactual: reason=%s\nrule  : reason=%q", ev.Reason, export.DropQueueFull)
			}
			gotDrops++
		default:
			break drain
		}
	}
	if gotDrops != 3 {
		t.Fatalf("\ninput : 3 overflows\nactual: %d drop events\nrule  : every drop is observable", gotDrops)
	}
	// The two OLDEST offered items are what remains: newest are
	// dropped, not oldest.
	var vals []int
	for len(vals) < 2 {
		e := <-q.C()
		vals = append(vals, e.Payload.(int))
	}
	if vals[0] != 0 || vals[1] != 1 {
		t.Fatalf("\ninput : drop newest policy\nactual: buffered %v\nrule  : items 0 and 1 survive", vals)
	}
}

func TestBlockPolicyBackpressuresThenAccepts(t *testing.T) {
	q := export.NewQueue(1, export.PolicyBlock)
	if err := q.Enqueue(context.Background(), env(0)); err != nil {
		t.Fatal(err)
	}
	accepted := make(chan error, 1)
	go func() { accepted <- q.Enqueue(context.Background(), env(1)) }()

	select {
	case err := <-accepted:
		t.Fatalf("\ninput : queue full, second offer\nactual: returned %v immediately\nrule  : producer must block (backpressure)", err)
	case <-time.After(30 * time.Millisecond):
	}

	<-q.C() // drain one
	select {
	case err := <-accepted:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatalf("\ninput : space freed\nactual: producer still blocked\nrule  : blocked offer completes once space is available")
	}
	if s := q.Stats(); s.Accepted != 2 || s.Dropped != 0 {
		t.Fatalf("\ninput : 2 accepted after drain\nactual: %+v\nrule  : no drops under block policy", s)
	}
}

func TestEnqueueAfterClose(t *testing.T) {
	q := export.NewQueue(1, export.PolicyDropNewest)
	q.Close()
	err := q.Enqueue(context.Background(), env(1))
	if !errors.Is(err, export.ErrQueueClosed) {
		t.Fatalf("\ninput : enqueue after Close\nactual: %v\nrule  : ErrQueueClosed", err)
	}
	if q.Stats().Dropped != 1 {
		t.Fatalf("\ninput : rejected close enqueue\nactual: dropped=%d\nrule  : close-drop is observable", q.Stats().Dropped)
	}
}

type blockingExporter struct {
	mu      sync.Mutex
	batches [][]export.Envelope
	gate    chan struct{}
}

func (b *blockingExporter) Export(ctx context.Context, batch []export.Envelope) error {
	<-b.gate
	b.mu.Lock()
	defer b.mu.Unlock()
	cp := make([]export.Envelope, len(batch))
	copy(cp, batch)
	b.batches = append(b.batches, cp)
	return nil
}

func TestGracefulShutdownLosesNothing(t *testing.T) {
	q := export.NewQueue(4, export.PolicyBlock)
	ex := &blockingExporter{gate: make(chan struct{})}
	b := export.NewBatcher(q, ex, export.BatcherConfig{
		MaxBatchSize: 100, FlushInterval: time.Hour, ExportTimeout: time.Second})

	ctx := context.Background()
	done := make(chan struct{})
	go func() { _ = b.Run(ctx); close(done) }()

	const n = 20
	for i := 0; i < n; i++ {
		if err := q.Enqueue(ctx, env(i)); err != nil {
			t.Fatal(err)
		}
	}
	q.Close()

	// Nothing must be exported until gate opens, but shutdown must
	// eventually deliver ALL n items.
	select {
	case <-done:
		t.Fatalf("\ninput : exporter blocked\nactual: Run returned\nrule  : Run waits for slow exporter, does not drop")
	case <-time.After(30 * time.Millisecond):
	}
	close(ex.gate)
	<-done

	var nExported int
	ex.mu.Lock()
	for _, batch := range ex.batches {
		nExported += len(batch)
	}
	ex.mu.Unlock()
	if nExported != n {
		t.Fatalf("\ninput : %d accepted items, slow exporter, shutdown\nactual: %d exported\nrule  : zero data loss on graceful shutdown",
			n, nExported)
	}
}

type failExporter struct {
	fails   int32
	calls   int32
	batches int32
}

func (f *failExporter) Export(ctx context.Context, batch []export.Envelope) error {
	n := atomic.AddInt32(&f.calls, 1)
	if n <= 2 {
		atomic.AddInt32(&f.fails, 1)
		return errors.New("boom")
	}
	atomic.AddInt32(&f.batches, 1)
	return nil
}

func TestRetryThenSuccess(t *testing.T) {
	q := export.NewQueue(4, export.PolicyBlock)
	ex := &failExporter{}
	b := export.NewBatcher(q, ex, export.BatcherConfig{
		MaxBatchSize: 1, FlushInterval: time.Hour, MaxRetries: 3, RetryBase: time.Millisecond})
	go func() { _ = b.Run(context.Background()) }()
	if err := q.Enqueue(context.Background(), env(1)); err != nil {
		t.Fatal(err)
	}
	q.Close()
	time.Sleep(200 * time.Millisecond)
	st := b.Stats()
	if st.Exported != 1 || st.Batches != 1 {
		t.Fatalf("\ninput : 2 failing then succeeding exporter\nactual: %+v\nrule  : batch eventually exported once", st)
	}
	if st.Retries != 2 {
		t.Fatalf("\ninput : 2 failures\nactual: retries=%d\nrule  : each failure counted", st.Retries)
	}
}

func TestFileExporterWritesJSONL(t *testing.T) {
	var buf bytes.Buffer
	ex := export.NewFileExporter(&buf)
	err := ex.Export(context.Background(), []export.Envelope{env(1), env(2)})
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	if len(lines) != 2 {
		t.Fatalf("\ninput : 2 envelopes\nactual: %d lines\nrule  : one JSON object per line", len(lines))
	}
	for i, line := range lines {
		var v map[string]any
		if err := json.Unmarshal(line, &v); err != nil {
			t.Fatalf("\ninput : line %d\nactual: %s\nrule  : valid JSON: %v", i, line, err)
		}
		if v["kind"] != "log" {
			t.Fatalf("\ninput : KindLog\nactual: %v\nrule  : kind=log", v["kind"])
		}
	}
}

func TestBlockedProducerReleasedOnClose(t *testing.T) {
	q := export.NewQueue(1, export.PolicyBlock)
	if err := q.Enqueue(context.Background(), env(0)); err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- q.Enqueue(context.Background(), env(1)) }()
	time.Sleep(20 * time.Millisecond)
	go func() {
		time.Sleep(20 * time.Millisecond)
		q.Close()
	}()
	select {
	case err := <-errCh:
		if !errors.Is(err, export.ErrQueueClosed) {
			t.Fatalf("\ninput : close while producer blocked\nactual: %v\nrule  : ErrQueueClosed, no panic", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked producer not released on Close")
	}
}

var _ = fmt.Sprintf
