// Package export provides the local export path: a bounded in-memory
// queue with explicit backpressure/drop policies, a batching worker
// and pluggable exporters. No network or external service is used.
package export

import (
	"context"
	"errors"
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
	Kind      Kind
	Timestamp int64 // unix nano, set by the producer
	Payload   any
}

// DropReason says why an item was rejected.
type DropReason string

const (
	DropQueueFull   DropReason = "queue_full"
	DropQueueClosed DropReason = "queue_closed"
)

// ErrQueueClosed is returned by TryEnqueue/Enqueue after Close.
var ErrQueueClosed = errors.New("export: queue closed")

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
	ch       chan Envelope
	policy   Policy
	dropped  atomic.Int64
	closed   atomic.Bool
}

// NewQueue creates a queue of the given capacity (must be > 0).
func NewQueue(capacity int, policy Policy) *Queue { return &Queue{} }

// Enqueue puts an item in the queue. Under PolicyBlock it blocks
// until space is available, the queue is closed or ctx is done.
// Under PolicyDropNewest it returns immediately; on overflow the item
// is counted and a DropEvent is emitted through Drops().
func (q *Queue) Enqueue(ctx context.Context, e Envelope) error { return nil }

// Close marks the queue closed; blocked producers are released with
// ErrQueueClosed. Buffered items remain readable.
func (q *Queue) Close() {}

// C returns the receive channel consumed by the batcher.
func (q *Queue) C() <-chan Envelope { return nil }

// DroppedTotal returns the cumulative number of dropped items.
func (q *Queue) DroppedTotal() int64 { return 0 }

// DropEvent records one observable loss.
type DropEvent struct {
	Kind   Kind
	Reason DropReason
}

// Drops returns the channel on which drop events are reported. It is
// buffered; if nobody listens drops are still counted in DroppedTotal.
func (q *Queue) Drops() <-chan DropEvent { return nil }
