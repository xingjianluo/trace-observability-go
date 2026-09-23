// Package sampler contains deterministic, explainable sampling
// decisions used when a root span is created. A trace-level decision
// is inherited unchanged by every child span, so results are stable
// across reruns and easy to audit.
package sampler

import (
	"fmt"
	"hash/fnv"

	"github.com/xingjianluo/trace-observability-go/tracing"
)

// scale maps the unit interval to integer math.
const scale uint64 = 1 << 32

// Decision explains one sampling outcome.
type Decision struct {
	Sampled bool   `json:"sampled"`
	Reason  string `json:"reason"` // human-readable explanation
	Rule    string `json:"rule"`   // machine-readable rule id
	Input   string `json:"input"`  // what the decision was computed from
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
	threshold uint64
	ratio     float64
}

// NewRatio builds a Ratio sampler. Values outside [0,1] are clamped.
func NewRatio(ratio float64) *Ratio {
	if ratio < 0 {
		ratio = 0
	}
	if ratio > 1 {
		ratio = 1
	}
	return &Ratio{threshold: uint64(ratio * float64(scale)), ratio: ratio}
}

// Rule implements the shared rule id used in Decision.Rule.
func (s *Ratio) RuleID() string { return fmt.Sprintf("ratio:%g", s.ratio) }

func (s *Ratio) Decide(id tracing.TraceID, name string) Decision {
	h := hashTraceID(id)
	// Uniform in [0, scale): use the low 32 bits of the 64-bit
	// FNV hash after modulo scale (scale is a power of two, so this
	// is a clean bit mask).
	bucket := h % scale
	sampled := bucket < s.threshold
	reason := fmt.Sprintf(
		"trace=%s fnv1a64%%2^32=%d threshold=%d (ratio=%g): %s",
		id.Hex(), bucket, s.threshold, s.ratio,
		map[bool]string{true: "keep", false: "drop"}[sampled])
	return Decision{
		Sampled: sampled,
		Reason:  reason,
		Rule:    s.RuleID(),
		Input:   id.Hex(),
	}
}

// NameRules layers forced allow/deny rules for exact span names on
// top of a fallback sampler. Deny is checked before allow so an
// accidental listing in both is a safe drop.
type NameRules struct {
	allow    map[string]struct{}
	deny     map[string]struct{}
	fallback Sampler
}

// NewNameRules wraps fallback; names added via deny are always
// dropped, names added via allow are always kept (deny wins on
// conflict).
func NewNameRules(fallback Sampler, allow, deny []string) *NameRules {
	nr := &NameRules{
		allow:    make(map[string]struct{}, len(allow)),
		deny:     make(map[string]struct{}, len(deny)),
		fallback: fallback,
	}
	for _, n := range allow {
		nr.allow[n] = struct{}{}
	}
	for _, n := range deny {
		nr.deny[n] = struct{}{}
	}
	return nr
}

func (s *NameRules) Decide(id tracing.TraceID, name string) Decision {
	if _, ok := s.deny[name]; ok {
		return Decision{
			Sampled: false,
			Reason:  fmt.Sprintf("span name %q matched explicit deny rule", name),
			Rule:    "name:deny",
			Input:   name,
		}
	}
	if _, ok := s.allow[name]; ok {
		return Decision{
			Sampled: true,
			Reason:  fmt.Sprintf("span name %q matched explicit allow rule", name),
			Rule:    "name:allow",
			Input:   name,
		}
	}
	d := s.fallback.Decide(id, name)
	d.Rule = "fallback:" + d.Rule
	return d
}

// hashTraceID returns the FNV-1a 64-bit hash of the raw trace id
// bytes. FNV is not cryptographic; we only need a stable, well-spread
// bucket assignment.
func hashTraceID(id tracing.TraceID) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(id[:])
	return h.Sum64()
}

// AlwaysOn and AlwaysOff are trivial samplers for tests/config.
type always struct{ keep bool }

func (a always) Decide(id tracing.TraceID, name string) Decision {
	if a.keep {
		return Decision{Sampled: true, Reason: "always-on sampler", Rule: "always_on", Input: id.Hex()}
	}
	return Decision{Sampled: false, Reason: "always-off sampler", Rule: "always_off", Input: id.Hex()}
}

// AlwaysOn keeps every trace.
func AlwaysOn() Sampler { return always{keep: true} }

// AlwaysOff drops every trace.
func AlwaysOff() Sampler { return always{keep: false} }
