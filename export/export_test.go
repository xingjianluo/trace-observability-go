package export

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}
func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func recFor(i int) Record {
	return Record{Kind: KindLog, Data: json.RawMessage(string(append(append([]byte(`{"i":`), itoa(i)...), '}'))), Accepted: true}
}

func TestDropNewestPolicy(t *testing.T) {
	buf := &lockedBuffer{}
	e := New(buf, WithCapacity(4), WithBatchSize(4),
		WithFlushInterval(0), WithDropPolicy(DropNewest))
	for i := 0; i < 10; i++ {
		if err := e.Enqueue(context.Background(), recFor(i)); err != nil {
			t.Fatalf("Enqueue 错误: %v", err)
		}
	}
	st := e.Snapshot()
	if st.Enqueued != 4 || st.DroppedNew != 6 {
		t.Errorf("DropNewest 计数错误\n输入: 10 条, 容量 4\n实际: enqueued=%d dropped_new=%d\n期望: 4/6",
			st.Enqueued, st.DroppedNew)
	}
	if err := e.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	var exported []map[string]json.RawMessage
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var batch []map[string]json.RawMessage
		if err := json.Unmarshal([]byte(line), &batch); err != nil {
			t.Fatalf("输出不是 JSON 数组行: %v", err)
		}
		exported = append(exported, batch...)
	}
	if len(exported) != 4 {
		t.Errorf("导出条数错误\n输入: 4 条入队\n实际导出: %d\n期望: 4", len(exported))
	}
	if e.Snapshot().Exported != 4 {
		t.Errorf("Exported 计数错误: %d", e.Snapshot().Exported)
	}
}

func TestDropOldestPolicy(t *testing.T) {
	buf := &lockedBuffer{}
	e := New(buf, WithCapacity(4), WithBatchSize(8),
		WithFlushInterval(0), WithDropPolicy(DropOldest))
	for i := 0; i < 10; i++ {
		_ = e.Enqueue(context.Background(), recFor(i))
	}
	st := e.Snapshot()
	if st.Enqueued != 10 || st.DroppedOld != 6 {
		t.Errorf("DropOldest 计数错误\n输入: 10 条, 容量 4\n实际: enqueued=%d dropped_old=%d\n期望: 10/6",
			st.Enqueued, st.DroppedOld)
	}
	_ = e.Shutdown(context.Background())
	var indices []int
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var batch []struct {
			Data struct {
				I int `json:"i"`
			} `json:"data"`
		}
		if err := json.Unmarshal([]byte(line), &batch); err != nil {
			t.Fatalf("解析批次: %v", err)
		}
		for _, r := range batch {
			indices = append(indices, r.Data.I)
		}
	}
	want := []int{6, 7, 8, 9}
	if !eqInts(indices, want) {
		t.Errorf("DropOldest 保留集合错误\n输入: 0..9, 容量 4\n实际: %v\n期望: %v\n判定依据: 应淘汰最旧保留最新",
			indices, want)
	}
}

func TestBlockCallerBackpressure(t *testing.T) {
	buf := &lockedBuffer{}
	// flushInterval=0 => worker 不主动 flush，队列满后 Enqueue 必须阻塞（背压）。
	e := New(buf, WithCapacity(2), WithBatchSize(2),
		WithFlushInterval(0), WithDropPolicy(BlockCaller))
	if err := e.Enqueue(context.Background(), recFor(1)); err != nil {
		t.Fatal(err)
	}
	if err := e.Enqueue(context.Background(), recFor(2)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- e.Enqueue(context.Background(), recFor(3)) }()
	select {
	case err := <-done:
		t.Fatalf("队列满时应阻塞形成背压，却立即返回: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	// Flush 腾出空位后，阻塞的 Enqueue 应成功。
	if err := e.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("背压解除后 Enqueue 应成功: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Flush 后背压未解除")
	}
	// ctx 取消时阻塞的 Enqueue 应返回 ctx 错误
	e2 := New(&lockedBuffer{}, WithCapacity(1), WithFlushInterval(0), WithDropPolicy(BlockCaller))
	_ = e2.Enqueue(context.Background(), recFor(0))
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- e2.Enqueue(ctx, recFor(1)) }()
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ctx 取消后应返回 context.Canceled，实际: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ctx 取消未唤醒阻塞的 Enqueue")
	}
	_ = e.Shutdown(context.Background())
	_ = e2.Shutdown(context.Background())
}

func TestEnqueueAfterShutdown(t *testing.T) {
	e := New(io.Discard, WithFlushInterval(0))
	if err := e.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	err := e.Enqueue(context.Background(), recFor(0))
	if !errors.Is(err, ErrClosed) {
		t.Errorf("Shutdown 后入队应返回 ErrClosed\n实际: %v\n期望: %v", err, ErrClosed)
	}
}

func TestShutdownLosesNoAcceptedData(t *testing.T) {
	buf := &lockedBuffer{}
	e := New(buf, WithCapacity(1000), WithBatchSize(17),
		WithFlushInterval(time.Hour)) // 关闭定时刷新，验证 Shutdown 排空
	const n = 500
	for i := 0; i < n; i++ {
		if err := e.Enqueue(context.Background(), recFor(i)); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	if err := e.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	count := 0
	dec := json.NewDecoder(strings.NewReader(buf.String()))
	for dec.More() {
		var batch []Record
		if err := dec.Decode(&batch); err != nil {
			t.Fatalf("解析批次: %v", err)
		}
		count += len(batch)
	}
	if count != n {
		t.Errorf("Shutdown 丢失了已接受数据\n输入: %d 条入队\n实际导出: %d\n判定依据: 正常收尾必须不丢数据", n, count)
	}
	if st := e.Snapshot(); st.Exported != n {
		t.Errorf("Exported=%d 期望 %d", st.Exported, n)
	}
}

func TestConcurrentEnqueueWithRace(t *testing.T) {
	buf := &lockedBuffer{}
	e := New(buf, WithCapacity(256), WithBatchSize(64), WithFlushInterval(10*time.Millisecond))
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = e.Enqueue(context.Background(), recFor(id*100+j))
			}
		}(i)
	}
	wg.Wait()
	if err := e.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	st := e.Snapshot()
	if st.Enqueued != st.Exported {
		t.Errorf("计数不平衡\n输入: 1600 次入队尝试, 容量 256, DropNewest\n实际: enqueued=%d exported=%d droppedNew=%d droppedOld=%d\n判定依据: Shutdown 后 enqueued==exported；尝试数=%d=enqueued+dropped",
			st.Enqueued, st.Exported, st.DroppedNew, st.DroppedOld,
			st.Enqueued+st.DroppedNew+st.DroppedOld)
	}
	if st.Enqueued+st.DroppedNew+st.DroppedOld != 1600 {
		t.Errorf("尝试数守恒失败\n实际: enqueued+dropped=%d\n期望: 1600",
			st.Enqueued+st.DroppedNew+st.DroppedOld)
	}
}

func eqInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func itoa(i int) []byte {
	if i == 0 {
		return []byte("0")
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return b
}
