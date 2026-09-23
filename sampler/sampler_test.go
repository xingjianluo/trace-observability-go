package sampler_test

import (
	"fmt"
	"testing"

	"github.com/xingjianluo/trace-observability-go/sampler"
	"github.com/xingjianluo/trace-observability-go/tracing"
)

func traceIDFromHex(t *testing.T, h string) tracing.TraceID {
	t.Helper()
	var id tracing.TraceID
	if len(h) != 32 {
		t.Fatalf("bad hex len %d", len(h))
	}
	for i := 0; i < 16; i++ {
		var b byte
		for _, c := range []byte(h[2*i : 2*i+2]) {
			b <<= 4
			switch {
			case c >= '0' && c <= '9':
				b |= c - '0'
			case c >= 'a' && c <= 'f':
				b |= c - 'a' + 10
			}
		}
		id[i] = b
	}
	return id
}

func TestRatioDeterministicBoundaries(t *testing.T) {
	// Same id must always produce the same decision, across
	// instances and calls.
	id1 := traceIDFromHex(t, "01010101010101010101010101010101")
	id2 := traceIDFromHex(t, "02020202020202020200202020202020")
	ratio := sampler.NewRatio(0.5)
	for i := 0; i < 5; i++ {
		d1 := sampler.NewRatio(0.5).Decide(id1, "op")
		d2 := ratio.Decide(id2, "op")
		if got := ratio.Decide(id1, "op"); got.Sampled != d1.Sampled {
			t.Fatalf("\ninput : ratio=0.5 id=%s call#%d\nactual: %v then %v\nrule  : identical input yields identical decision",
				id1.Hex(), i, d1.Sampled, got.Sampled)
		}
		_ = d2
	}
	// 0 drops all, 1 keeps all, deterministically.
	for _, tc := range []struct {
		r    float64
		keep bool
	}{{0, false}, {1, true}} {
		for i := 0; i < 1000; i++ {
			id := traceIDFromHex(t, fmt.Sprintf("%032d", i+1))
			if d := sampler.NewRatio(tc.r).Decide(id, "x"); d.Sampled != tc.keep {
				t.Fatalf("\ninput : ratio=%v id=%s\nactual: sampled=%v (%s)\nrule  : ratio 0 = all drop, ratio 1 = all keep",
					tc.r, id.Hex(), d.Sampled, d.Reason)
			}
		}
	}
}

func TestRatioDistribution(t *testing.T) {
	ratio := sampler.NewRatio(0.5)
	kept := 0
	const total = 4000
	for i := 0; i < total; i++ {
		id := traceIDFromHex(t, fmt.Sprintf("%032d", i+1))
		if ratio.Decide(id, "x").Sampled {
			kept++
		}
	}
	// FNV over sequential ids should land close to 50%.
	frac := float64(kept) / total
	if frac < 0.4 || frac > 0.6 {
		t.Fatalf("\ninput : ratio=0.5 n=%d\nactual: kept=%d (%.2f)\nrule  : observed fraction within [0.4,0.6]",
			total, kept, frac)
	}
}

func TestDecisionIsExplainable(t *testing.T) {
	d := sampler.NewRatio(0.25).Decide(traceIDFromHex(t, "abababababababababababababababab"), "op")
	if d.Rule == "" || d.Reason == "" || d.Input == "" {
		t.Fatalf("\ninput : ratio decision\nactual: %+v\nrule  : Decision must carry rule, reason and input", d)
	}
}

func TestNameRulesDenyWinsOverAllow(t *testing.T) {
	rules := sampler.NewNameRules(sampler.AlwaysOn(),
		[]string{"important"}, []string{"important", "noise"})
	d := rules.Decide(traceIDFromHex(t, "00000000000000000000000000000001"), "important")
	if d.Sampled {
		t.Fatalf("\ninput : name in both allow and deny\nactual: sampled=%v rule=%s\nrule  : deny wins on conflict",
			d.Sampled, d.Rule)
	}
	if d := rules.Decide(traceIDFromHex(t, "00000000000000000000000000000001"), "noise"); d.Sampled || d.Rule != "name:deny" {
		t.Fatalf("\ninput : deny-listed name\nactual: %+v\nrule  : deny rule applies", d)
	}
	if d := rules.Decide(traceIDFromHex(t, "00000000000000000000000000000001"), "important-other"); !d.Sampled {
		t.Fatalf("\ninput : fallback always-on, non-listed name\nactual: sampled=%v\nrule  : fallback decides", d.Sampled)
	}
}
