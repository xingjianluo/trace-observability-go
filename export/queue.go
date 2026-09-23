// Package export provides the local export path: a bounded in-memory
// queue with explicit backpressure/drop policies, a batching worker
// and pluggable exporters. No network or external service is used.
package export

import (
	"context"
	"sync"
	"sync/atomic"
)

// Kind identifies the payload type of an Envelope.
type Kind string

const (
	KindSpan Kind = "span"
	KindLog  Kind = "log"
)

// Envelope is one item offered to the pipeline. Payload is a
// tracing.SpanData or logx.Entry; it is typed as any to keep the
// export package free of import cycles.
type Envelope struct {
	Kind      Kind `json:"kind"`
	Timestamp int64 `json:"timestamp_ns"`
	Payload   any  `json:"payload"`
}

// DropReason says why an item was rejected.
type DropReason string

const (
	DropQueueFull   DropReason = "queue_full"
	DropQueueClosed DropReason = "queue_closed"
)

// ErrQueueClosed is returned by Enqueue after Close.
type queueClosedErr struct{}

func (queueClosedErr) Error() string { return "export: queue closed" }

// ErrQueueClosed is returned by Enqueue after Close.
var ErrQueueClosed error = queueClosedErr{}

// Policy selects what happens when the bounded queue is full.
type Policy int

const (
	// PolicyBlock blocks until space is available or the queue is
	// closed; it guarantees no accepted data is lost.
	PolicyBlock Policy = iota
	// PolicyDropNewest rejects the incoming item and counts it.
	PolicyDropNewest
)

// Queue is a bounded, concurrency-safe queue.
type Queue struct {
	ch     chan Envelope
	policy Policy

	mu     sync.RWMutex
	closed bool

	accepted atomic.Int64
	dropped  atomic.Int64

	dropCh chan DropEvent
	closeCh chan struct{}
}

// NewQueue creates a queue of the given capacity (must be > 0).
func NewQueue(capacity int, policy Policy) *Queue {
	if capacity <= 0 {
		capacity = 1
	}
	return &Queue{
		ch:     make(chan Envelope, capacity),
		policy: policy,
		// Buffer a few drop notifications; if the observer is slow
		// drops are still reflected by DroppedTotal.
		dropCh:  make(chan DropEvent, 16),
		closeCh: make(chan struct{}),
	}
}

// Enqueue puts an item in the queue.
//
//   - PolicyBlock: blocks until space is available, ctx is done, or
//     the queue is closed. A queued item counts as accepted and is
//     guaranteed to be drained on graceful shutdown.
//   - PolicyDropNewest: never blocks on a full queue; the offered
//     item is counted under DropQueueFull and a DropEvent is emitted.
func (q *Queue) Enqueue(ctx context.Context, e Envelope) error {
	if q.isClosed() {
		q.countDrop(e.Kind, DropQueueClosed)
		return ErrQueueClosed
	}
	if q.policy == PolicyDropNewest {
		select {
		case q.ch <- e:
			q.accepted.Add(1)
			return nil
		default:
			q.countDrop(e.Kind, DropQueueFull)
			return nil
		}
	}
	select {
	case q.ch <- e:
		q.accepted.Add(1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-q.closeCh:
		q.countDrop(e.Kind, DropQueueClosed)
		return ErrQueueClosed
	}
}

// countDrop bumps the counter and best-effort notifies observers.
func (q *Queue) countDrop(k Kind, r DropReason) {
	q.dropped.Add(1)
	select {
	case q.dropCh <- DropEvent{Kind: k, Reason: r}:
	default:
	}
}

// Close marks the queue closed; blocked producers are released with
// ErrQueueClosed. Buffered items remain readable until drained.
// Close is idempotent.
func (q *Queue) Close() {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.closed = true
	close(q.ch)
	close(q.closeCh)
	q.mu.Unlock()
}

func (q *Queue) isClosed() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.closed
}

// C returns the receive channel consumed by the batcher.
func (q *Queue) C() <-chan Envelope { return q.ch }

// AcceptedTotal returns the cumulative number of enqueued items.
func (q *Queue) AcceptedTotal() int64 { return q.accepted.Load() }

// DroppedTotal returns the cumulative number of dropped items.
func (q *Queue) DroppedTotal() int64 { return q.dropped.Load() }

// DropEvent records one observable loss.
type DropEvent struct {
	Kind   Kind       `json:"kind"`
	Reason DropReason `json:"reason"`
}

// Drops returns the channel on which drop events are reported.
func (q *Queue) Drops() <-chan DropEvent { return q.dropCh }
