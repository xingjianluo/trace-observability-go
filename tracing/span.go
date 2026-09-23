package tracing

import (
	"context"
	"sync"
	"time"
)

// Kind classifies a span's role in a trace.
type Kind int

const (
	KindInternal Kind = iota
	KindServer
	KindClient
	KindProducer
	KindConsumer
)

func (k Kind) String() string {
	switch k {
	case KindServer:
		return "server"
	case KindClient:
		return "client"
	case KindProducer:
		return "producer"
	case KindConsumer:
		return "consumer"
	default:
		return "internal"
	}
}

// EndReason explains why a span terminated. It is recorded verbatim
// when a parent's cancellation propagates to a child.
type EndReason string

const (
	ReasonFinished     EndReason = "finished"
	ReasonParentCancel EndReason = "parent_cancelled"
	ReasonContextDone  EndReason = "context_cancelled"
	ReasonDropped      EndReason = "dropped"
)

// Event is a timestamped annotation attached to a span.
type Event struct {
	Time    time.Time
	Name    string
	Payload map[string]any
}

// SpanData is the immutable snapshot exported after a span ends.
type SpanData struct {
	TraceID      TraceID         `json:"trace_id"`
	SpanID       SpanID          `json:"span_id"`
	ParentSpanID SpanID          `json:"parent_span_id"`
	Name         string          `json:"name"`
	Kind         Kind            `json:"kind"`
	Start        time.Time       `json:"start"`
	End          time.Time       `json:"end"`
	EndReason    EndReason       `json:"end_reason"`
	Attributes   map[string]any  `json:"attributes,omitempty"`
	Events       []Event         `json:"events,omitempty"`
	StatusError  string          `json:"status_error,omitempty"`
	Depth        int             `json:"depth"`
}

// Span is an in-progress operation. All methods are safe for
// concurrent use. End must be called exactly once; further calls are
// ignored.
type Span interface {
	SpanContext() SpanContext
	ParentID() SpanID
	SetAttribute(key string, value any)
	AddEvent(name string, payload map[string]any)
	RecordError(err error)
	End()
	// Done returns the channel closed when the span ends, either by
	// an explicit End or by a propagated cancellation.
	Done() <-chan struct{}
	// Context returns the derived context carrying this span.
	Context() context.Context
	// Depth returns the distance from the trace root (root = 0).
	Depth() int
}

// SpanOption configures a span at creation.
type SpanOption func(*spanConfig)

type spanConfig struct {
	kind  Kind
	start time.Time
}

// SpanKind sets the span kind.
func SpanKind(k Kind) SpanOption {
	return func(c *spanConfig) { c.kind = k }
}

// SpanStart overrides the start time (tests).
func SpanStart(t time.Time) SpanOption {
	return func(c *spanConfig) { c.start = t }
}

// span is the concrete Span implementation.
type span struct {
	// Immutable identity / metadata.
	sc      SpanContext
	parent  SpanID
	name    string
	kind    Kind
	depth   int
	startAt time.Time
	ctx     context.Context
	now       func() time.Time
	onEnd     func(SpanData)
	callerCtx context.Context
	bornRoot  bool

	// Mutable state guarded by mu.
	mu        sync.Mutex
	attrs     map[string]any
	events    []Event
	statusErr string

	// Lifecycle.
	endOnce sync.Once
	done    chan struct{}
}

func newSpan(sc SpanContext, parent SpanID, name string, kind Kind, depth int,
	startAt time.Time, ctx context.Context, now func() time.Time,
	onEnd func(SpanData), isRoot bool) *span {
	s := &span{
		sc:        sc,
		parent:    parent,
		name:      name,
		kind:      kind,
		depth:     depth,
		startAt:   startAt,
		ctx:       withSpan(ctx, nil),
		now:       now,
		onEnd:     onEnd,
		done:      make(chan struct{}),
		callerCtx: ctx,
		bornRoot:  isRoot,
	}
	s.ctx = withSpan(ctx, s)
	return s
}

func (s *span) SpanContext() SpanContext { return s.sc }
func (s *span) ParentID() SpanID         { return s.parent }

func (s *span) SetAttribute(key string, value any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attrs == nil {
		s.attrs = make(map[string]any)
	}
	s.attrs[key] = value
}

func (s *span) AddEvent(name string, payload map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, Event{
		Time:    s.now(),
		Name:    name,
		Payload: cloneMap(payload),
	})
}

func (s *span) RecordError(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	s.statusErr = err.Error()
	s.events = append(s.events, Event{
		Time:    s.now(),
		Name:    "exception",
		Payload: map[string]any{"message": err.Error()},
	})
	s.mu.Unlock()
}

func (s *span) End() { s.finish(ReasonFinished) }

// finish ends the span with the given reason. It is safe to call from
// multiple goroutines: exactly one caller wins and produces the
// exported snapshot. When an explicit finish races an already-visible
// cancellation of the caller context, cancellation wins, because the
// operation's real termination cause is the cancellation.
func (s *span) finish(reason EndReason) {
	s.endOnce.Do(func() {
		if reason == ReasonFinished && s.callerCtx != nil && s.callerCtx.Err() != nil {
			if s.bornRoot {
				reason = ReasonContextDone
			} else {
				reason = ReasonParentCancel
			}
		}
		s.mu.Lock()
		data := SpanData{
			TraceID:      s.sc.TraceID,
			SpanID:       s.sc.SpanID,
			ParentSpanID: s.parent,
			Name:         s.name,
			Kind:         s.kind,
			Start:        s.startAt,
			End:          s.now(),
			EndReason:    reason,
			Attributes:   cloneMap(s.attrs),
			Events:       append([]Event(nil), s.events...),
			StatusError:  s.statusErr,
			Depth:        s.depth,
		}
		s.mu.Unlock()
		close(s.done)
		if s.onEnd != nil {
			s.onEnd(data)
		}
	})
}

func (s *span) Done() <-chan struct{}    { return s.done }
func (s *span) Context() context.Context { return s.ctx }
func (s *span) Depth() int                  { return s.depth }

// watchCancellation ends the span when the caller-supplied context
// is cancelled. The concrete reason (parent_cancelled vs
// context_cancelled vs an explicit End winning the race) is decided
// centrally in finish, so the watcher only has to trigger it. The
// watcher exits promptly when the span ends normally.
func (s *span) watchCancellation(parentCtx context.Context) {
	select {
	case <-parentCtx.Done():
		s.finish(ReasonFinished)
	case <-s.done:
	}
}

func cloneMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

type jsonSpanData struct {
	TraceID      string        `json:"trace_id"`
	SpanID       string        `json:"span_id"`
	ParentSpanID string        `json:"parent_span_id"`
	Name         string        `json:"name"`
	Kind         string        `json:"kind"`
	Start        string        `json:"start"`
	End          string        `json:"end"`
	EndReason    EndReason     `json:"end_reason"`
	Attributes   map[string]any `json:"attributes,omitempty"`
	Events       []jsonEvent   `json:"events,omitempty"`
	StatusError  string        `json:"status_error,omitempty"`
	Depth        int           `json:"depth"`
}

type jsonEvent struct {
	Time    string         `json:"time"`
	Name    string         `json:"name"`
	Payload map[string]any `json:"payload,omitempty"`
}

const rfc3339NanoUTC = "2006-01-02T15:04:05.000000000Z"

// MarshalJSON renders a span with hex ids, string kind and UTC
// RFC3339Nano timestamps for stable, readable JSONL.
func (d SpanData) MarshalJSON() ([]byte, error) {
	j := jsonSpanData{
		TraceID:      d.TraceID.Hex(),
		SpanID:       d.SpanID.Hex(),
		Name:         d.Name,
		Kind:         d.Kind.String(),
		Start:        d.Start.UTC().Format(rfc3339NanoUTC),
		End:          d.End.UTC().Format(rfc3339NanoUTC),
		EndReason:    d.EndReason,
		Attributes:   d.Attributes,
		StatusError:  d.StatusError,
		Depth:        d.Depth,
	}
	if pid := d.ParentSpanID; pid != (SpanID{}) {
		j.ParentSpanID = pid.Hex()
	}
	for _, ev := range d.Events {
		j.Events = append(j.Events, jsonEvent{
			Time:    ev.Time.UTC().Format(rfc3339NanoUTC),
			Name:    ev.Name,
			Payload: ev.Payload,
		})
	}
	return jsonMarshal(j)
}
