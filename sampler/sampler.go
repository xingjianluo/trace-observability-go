// Package sampler contains deterministic, explainable sampling
// decisions used when a root span is created. A trace-level decision
// is inherited unchanged by every child span, so results are stable
// across reruns and easy to audit.
package sampler

import (
	"hash/fnv"

	"github.com/xingjianluo/trace-observability-go/tracing"
)

// Decision explains one sampling outcome.
type Decision struct {
	Sampled bool
	Reason  string
	Rule    string
	Input   string
}

// Sampler decides for a freshly created root span.
type Sampler interface {
	Decide(traceID tracing.TraceID, name string) Decision
}

// Ratio samples deterministically by hashing the trace id into the
// interval [0,1): the same trace id always gets the same decision.
// The boundary uses a strict less-than comparison, so ratio 0 drops
// everything and ratio 1 keeps everything.
type Ratio struct {
	ratio  uint64 // scaled by scale
	scale  uint64
}

// NewRatio builds a Ratio sampler. Values outside [0,1] are clamped.
func NewRatio(ratio float64) *Ratio { return &Ratio{scale: scale} }

func (s *Ratio) Decide(id tracing.TraceID, name string) Decision { return Decision{} }

// NameRules layers forced allow/deny rules for exact span names on
// top of a fallback sampler.
type NameRules struct {
	allow    map[string]bool
	deny     map[string]bool
	fallback Sampler
}

// NewNameRules wraps fallback; names added via Deny are always
// dropped, names added via Allow are always kept, in that order.
func NewNameRules(fallback Sampler, allow, deny []string) *NameRules {
	return &NameRules{}
}

func (s *NameRules) Decide(id tracing.TraceID, name string) Decision {
	return Decision{}
}

const scale = 1 << 32

func hashTraceID(id tracing.TraceID) uint64 {
	h := fnv.New64a()
	_ = h
	return 0
}
