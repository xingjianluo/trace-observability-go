package export

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"time"
)

// DropPolicy 定义有界队列满时的背压策略。
type DropPolicy int

const (
	// DropNewest 丢弃最新到达的数据（保护已入队数据）。
	DropNewest DropPolicy = iota
	// DropOldest 丢弃队列中最旧的数据，腾出空间给新数据。
	DropOldest
	// BlockCaller 阻塞调用方直到有空位（真正的背压，不丢数据）。
	BlockCaller
)

// Stats 是导出器的可观测计数（全部为单调累计值）。
type Stats struct {
	Enqueued   uint64 `json:"enqueued"`
	DroppedNew uint64 `json:"dropped_new"`
	DroppedOld uint64 `json:"dropped_old"`
	Exported   uint64 `json:"exported"`
	Batches    uint64 `json:"batches"`
}

// Exporter 是有界队列 + 批量消费的导出器。
// 输出格式为 JSON Lines：每个批次是一个 JSON 数组，独占一行。
type Exporter struct {
	mu       sync.Mutex
	cond     *sync.Cond
	queue    []Record
	capacity int
	policy   DropPolicy
	w        io.Writer
	batch    int
	interval time.Duration
	closed   bool
	workerWg sync.WaitGroup
	stop     chan struct{}
	stopped  chan struct{}
	stats    Stats
	writeMu  sync.Mutex
}

// Option 配置 Exporter。
type Option func(*exporterConfig)

type exporterConfig struct {
	capacity      int
	batchSize     int
	flushInterval time.Duration
	policy        DropPolicy
}

// WithCapacity 设置有界队列容量（默认 1024）。
func WithCapacity(n int) Option {
	return func(c *exporterConfig) {
		if n > 0 {
			c.capacity = n
		}
	}
}

// WithBatchSize 设置导出批次大小（默认 64）。
func WithBatchSize(n int) Option {
	return func(c *exporterConfig) {
		if n > 0 {
			c.batchSize = n
		}
	}
}

// WithFlushInterval 设置定期刷新间隔（默认 200ms，<=0 关闭定时）。
func WithFlushInterval(d time.Duration) Option {
	return func(c *exporterConfig) { c.flushInterval = d }
}

// WithDropPolicy 设置队列满时的背压/丢弃策略（默认 DropNewest）。
func WithDropPolicy(p DropPolicy) Option {
	return func(c *exporterConfig) { c.policy = p }
}

// New 创建导出器并启动后台消费协程。
// w 为批次输出目标，每批以 JSON 数组形式独占一行写出。
func New(w io.Writer, opts ...Option) *Exporter {
	cfg := exporterConfig{
		capacity:      1024,
		batchSize:     64,
		flushInterval: 200 * time.Millisecond,
		policy:        DropNewest,
	}
	for _, o := range opts {
		o(&cfg)
	}
	e := &Exporter{
		queue:    make([]Record, 0, cfg.capacity),
		capacity: cfg.capacity,
		policy:   cfg.policy,
		w:        w,
		batch:    cfg.batchSize,
		interval: cfg.flushInterval,
		stop:     make(chan struct{}),
		stopped:  make(chan struct{}),
	}
	e.cond = sync.NewCond(&e.mu)
	e.workerWg.Add(1)
	go e.worker()
	return e
}

// ErrClosed 在 Shutdown 之后再调用 Enqueue 时返回。
var ErrClosed = errors.New("export: exporter closed")

// MissingReason 常量描述一条“未被纳入统计”的记录的原因，
// 由 Submit 的回执携带，供业务组做覆盖统计。
const (
	MissingDroppedNew   = "queue_dropped_new"
	MissingDroppedOld   = "queue_dropped_old"
	MissingExporterGone = "exporter_closed"
)

// Result 是 Submit 的投递回执，调用方据此区分
// “实际进入队列”与“没能纳入（及为什么没能纳入）”。
type Result struct {
	Accepted      bool   // 是否真正进入队列
	MissingReason string // 未纳入原因；Accepted=true 时为空
}

// Submit 将一条记录放入有界队列并返回明确回执。语义同 Enqueue，
// 区别在于丢弃/关闭时通过 Result 告知具体原因，而不是静默成功。
//   - DropNewest：满时丢弃本条，回执 {Accepted:false, MissingReason:MissingDroppedNew}；
//   - DropOldest：满时丢弃队首旧条，本条入队；被丢弃旧条的原因记录在 DroppedOld 计数中；
//   - BlockCaller：阻塞直到有空位（Accepted=true）；ctx 取消返回 ctx 错误；
//   - Shutdown 之后：返回 ErrClosed，回执 MissingReason=MissingExporterGone。
func (e *Exporter) Submit(ctx context.Context, r Record) (Result, error) {
	e.mu.Lock()
	for len(e.queue) >= e.capacity && !e.closed {
		if e.policy == BlockCaller {
			if err := ctx.Err(); err != nil {
				e.mu.Unlock()
				return Result{Accepted: false}, err
			}
			// cond.Wait 期间释放锁；用 watcher 在 ctx 取消或 stop 时唤醒。
			// e.waitLocked 返回后 watcher 一定会退出，不会泄漏协程。
			if err := e.waitLocked(ctx); err != nil {
				e.mu.Unlock()
				return Result{Accepted: false}, err
			}
			continue
		}
		if e.policy == DropOldest {
			e.queue = e.queue[1:]
			e.stats.DroppedOld++
			break
		}
		e.stats.DroppedNew++
		e.mu.Unlock()
		return Result{Accepted: false, MissingReason: MissingDroppedNew}, nil
	}
	if e.closed {
		e.mu.Unlock()
		return Result{Accepted: false, MissingReason: MissingExporterGone}, ErrClosed
	}
	e.queue = append(e.queue, r)
	e.stats.Enqueued++
	e.mu.Unlock()
	e.cond.Signal()
	return Result{Accepted: true}, nil
}

// Enqueue 将一条记录放入有界队列。
//   - DropNewest：满时丢弃本条，DroppedNew +1；
//   - DropOldest：满时丢弃队首旧条，DroppedOld +1，本条入队；
//   - BlockCaller：阻塞直到有空位；Shutdown 时返回 ErrClosed。
func (e *Exporter) Enqueue(ctx context.Context, r Record) error {
	_, err := e.Submit(ctx, r)
	return err
}

// waitLocked 必须在持有 e.mu 时调用；它阻塞直到队列有空位、
// ctx 取消或导出器关闭。watcher 协程在三种情况下都会被回收。
func (e *Exporter) waitLocked(ctx context.Context) error {
	wake := make(chan struct{})
	var watcher sync.WaitGroup
	watcher.Add(1)
	go func() {
		defer watcher.Done()
		select {
		case <-ctx.Done():
			e.cond.Broadcast()
		case <-e.stop:
			e.cond.Broadcast()
		case <-wake:
		}
	}()
	e.cond.Wait()
	close(wake)
	e.mu.Unlock()
	watcher.Wait()
	e.mu.Lock()
	if e.closed {
		return ErrClosed
	}
	return ctx.Err()
}

// Snapshot 返回计数快照。
func (e *Exporter) Snapshot() Stats {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stats
}

func (e *Exporter) takeBatchLocked(max int) []Record {
	n := len(e.queue)
	if n == 0 {
		return nil
	}
	if n > max {
		n = max
	}
	batch := make([]Record, n)
	copy(batch, e.queue[:n])
	e.queue = e.queue[n:]
	return batch
}

func (e *Exporter) writeBatch(batch []Record) error {
	if len(batch) == 0 {
		return nil
	}
	data, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	_, err = e.w.Write(data)
	return err
}

func (e *Exporter) worker() {
	defer e.workerWg.Done()
	var timerCh <-chan time.Time
	var timer *time.Timer
	if e.interval > 0 {
		timer = time.NewTimer(e.interval)
		timerCh = timer.C
	}
	for {
		select {
		case <-e.stop:
			e.drainAndFlush()
			close(e.stopped)
			return
		case <-timerCh:
			e.mu.Lock()
			batch := e.takeBatchLocked(e.batch)
			e.mu.Unlock()
			if len(batch) > 0 {
				_ = e.writeBatch(batch)
				e.mu.Lock()
				e.stats.Exported += uint64(len(batch))
				e.stats.Batches++
				e.mu.Unlock()
			}
			timer.Reset(e.interval)
		}
	}
}

// drainAndFlush 排空队列中所有记录，分批同步写出（Shutdown 收尾用）。
func (e *Exporter) drainAndFlush() {
	for {
		e.mu.Lock()
		batch := e.takeBatchLocked(e.batch)
		e.mu.Unlock()
		if len(batch) == 0 {
			return
		}
		_ = e.writeBatch(batch)
		e.mu.Lock()
		e.stats.Exported += uint64(len(batch))
		e.stats.Batches++
		e.cond.Broadcast() // 唤醒可能在 BlockCaller 上等待的调用方
		e.mu.Unlock()
	}
}

// Flush 同步导出当前队列中的全部记录（不关闭导出器）。
func (e *Exporter) Flush(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		e.mu.Lock()
		n := len(e.queue)
		e.mu.Unlock()
		if n == 0 {
			return nil
		}
		e.mu.Lock()
		batch := e.takeBatchLocked(n)
		e.mu.Unlock()
		if err := e.writeBatch(batch); err != nil {
			return err
		}
		e.mu.Lock()
		e.stats.Exported += uint64(len(batch))
		e.stats.Batches++
		e.cond.Signal() // 腾出空位，唤醒 BlockCaller 的等待者
		e.mu.Unlock()
	}
}

// Shutdown 停止接收、排空队列并关闭后台协程；成功返回后不丢数据。
func (e *Exporter) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	close(e.stop)
	e.cond.Broadcast()
	e.mu.Unlock()

	done := make(chan struct{})
	go func() {
		e.workerWg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
