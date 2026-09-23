package sampling

import (
	"context"
	"strings"
	"sync"
	"testing"
)

func TestDecisionIsStable(t *testing.T) {
	s := NewThresholdSampler(0.5)
	d1 := s.Decide(context.Background(), "trace-42", false)
	d2 := s.Decide(context.Background(), "trace-42", false)
	if d1.Accept != d2.Accept || d1.HashValue != d2.HashValue {
		t.Errorf("同一采样键决策不稳定\n输入: key=trace-42 rate=0.5\n实际: %#v vs %#v\n判定依据: 同键同哈希必须同结论", d1, d2)
	}
	if d1.Reason == "" || !strings.Contains(d1.Reason, "mix64(") {
		t.Errorf("决策理由不可解释\n输入: key=trace-42\n实际: %q\n期望: 包含哈希与阈值说明", d1.Reason)
	}
}

func TestRateBoundaries(t *testing.T) {
	cases := []struct {
		rate   float64
		accept bool
		reason string
	}{
		{0, false, "rate=0"},
		{1, true, "rate=1"},
	}
	for _, c := range cases {
		s := NewThresholdSampler(c.rate)
		for _, key := range []string{"a", "b", "trace-xyz"} {
			d := s.Decide(context.Background(), key, false)
			if d.Accept != c.accept || !strings.Contains(d.Reason, c.reason) {
				t.Errorf("边界判定错误\n输入: rate=%v key=%s\n实际: accept=%v reason=%q\n期望: accept=%v reason~%q\n判定依据: rate 边界恒定结论",
					c.rate, key, d.Accept, d.Reason, c.accept, c.reason)
			}
		}
	}
}

func TestForcedAccept(t *testing.T) {
	s := NewThresholdSampler(0) // 本应全部丢弃
	d := s.Decide(context.Background(), "err-trace", true)
	if !d.Accept || !d.Forced || !strings.Contains(d.Reason, "forced") {
		t.Errorf("强制采样未生效\n输入: rate=0 forced=true\n实际: %#v\n期望: Accept=true 且 reason 含 forced", d)
	}
}

func TestApproximateRateDistribution(t *testing.T) {
	s := NewThresholdSampler(0.5)
	accept := 0
	const n = 4096
	for i := 0; i < n; i++ {
		d := s.Decide(context.Background(), "svc/order/"+keyFor(i)+"/user/"+keyFor(n-i), false)
		if d.Accept {
			accept++
		}
	}
	frac := float64(accept) / n
	if frac < 0.4 || frac > 0.6 {
		t.Errorf("采样比率偏差过大\n输入: rate=0.5 keys=%d\n实际: accept=%d frac=%.3f\n期望: frac∈[0.4,0.6]\n判定依据: FNV 哈希应近似均匀",
			n, accept, frac)
	}
}

func TestSaltChangesSetButStable(t *testing.T) {
	a := NewThresholdSamplerWithSalt(0.5, "salt-a")
	b := NewThresholdSamplerWithSalt(0.5, "salt-b")
	differ := false
	for i := 0; i < 64; i++ {
		k := keyFor(i)
		da := a.Decide(context.Background(), k, false)
		db := b.Decide(context.Background(), k, false)
		if da.Accept != db.Accept {
			differ = true
		}
		// 各自仍稳定
		if a.Decide(context.Background(), k, false).Accept != da.Accept {
			t.Fatalf("盐采样自身不稳定 key=%s", k)
		}
	}
	if !differ {
		t.Errorf("盐未改变采样集合\n输入: 64 个键, salt-a vs salt-b\n实际: 决策完全相同\n期望: 至少有一个不同\n判定依据: 盐应扰动哈希但保持稳定")
	}
}

func TestConcurrentDecisionsSafe(t *testing.T) {
	s := NewThresholdSampler(0.5)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				d := s.Decide(context.Background(), keyFor(id*1000+j), false)
				_ = d.Accept
			}
		}(i)
	}
	wg.Wait()
}

func keyFor(i int) string {
	const digits = "0123456789abcdef"
	b := make([]byte, 8)
	x := i
	for k := 7; k >= 0; k-- {
		b[k] = digits[x&0xf]
		x >>= 4
	}
	return string(b)
}
