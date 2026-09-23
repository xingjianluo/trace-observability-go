package sampling

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"
)

// Decision 是一次采样判定的结果。
type Decision struct {
	Accept      bool   // 是否采样（导出）
	Stable      bool   // 决策是否对同一采样键稳定
	Reason      string // 人类可读的判定依据
	Threshold   uint64 // 判定阈值（rate 映射到 [0,2^64)）
	HashValue   uint64 // 采样键的 FNV-1a 哈希值
	Rate        float64
	SamplingKey string // 实际参与哈希的键
	Forced      bool
}

func (d Decision) String() string { return d.Reason }

// Sampler 对某个追踪键给出确定性的采样决策。
type Sampler interface {
	Decide(ctx context.Context, key string, forced bool) Decision
}

// ThresholdSampler 是基于 FNV-1a 哈希与阈值比率的确定性采样器。
// 同一个采样键永远得到同一个结论，不依赖随机数，因此可复现、可解释。
type ThresholdSampler struct {
	rate      float64
	threshold uint64
	salt      string
	full      bool
}

// NewThresholdSampler 创建阈值采样器；rate 取值 [0,1]。
// rate<=0 表示全不采样，rate>=1 表示全采样。
func NewThresholdSampler(rate float64) *ThresholdSampler {
	return NewThresholdSamplerWithSalt(rate, "")
}

// NewThresholdSamplerWithSalt 允许指定盐，使同一逻辑键在不同部署间
// 得到不同但各自稳定的采样集合。
func NewThresholdSamplerWithSalt(rate float64, salt string) *ThresholdSampler {
	if math.IsNaN(rate) {
		rate = 0
	}
	if rate < 0 {
		rate = 0
	}
	if rate > 1 {
		rate = 1
	}
	// 语义：accept 当且仅当 hash/2^64 < rate，即 hash < rate*2^64。
	// rate=1 时阈值在数学上是 2^64（超出 uint64 表示范围），用 threshold=0
	// 配合 full 标志处理：full=true 时任意 hash 都接受。
	var threshold uint64
	if rate < 1 {
		threshold = uint64(rate * math.Ldexp(1, 64))
	}
	return &ThresholdSampler{rate: rate, threshold: threshold, salt: salt, full: rate >= 1}
}

// Rate 返回配置的采样比率。
func (s *ThresholdSampler) Rate() float64 { return s.rate }

// HashKey 返回采样键的稳定 64 位哈希。
//
// 仅用 FNV-1a 时，等长、仅末尾不同的键（如连续 TraceID）雪崩性差，
// 高位几乎不变，会导致按阈值比较时采样聚集。因此在 FNV-1a 之上再做
// 一次 splitmix64 雪崩混合（公开常数，非加密、确定性、零分配），
// 使输出在 [0,2^64) 上近似均匀，同时保持完全可复现。
func HashKey(key, salt string) uint64 {
	h := fnv.New64a()
	if salt != "" {
		_, _ = h.Write([]byte(salt))
		_, _ = h.Write([]byte{':'})
	}
	_, _ = h.Write([]byte(key))
	return mix64(h.Sum64())
}

// mix64 是 splitmix64 的最终化（avalanche）混合函数。
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// Decide 依据采样键的哈希值稳定地决定是否采样。
func (s *ThresholdSampler) Decide(_ context.Context, key string, forced bool) Decision {
	fullKey := key
	if s.salt != "" {
		fullKey = s.salt + ":" + key
	}
	hv := HashKey(key, s.salt)
	d := Decision{
		Stable:      true,
		Threshold:   s.threshold,
		HashValue:   hv,
		Rate:        s.rate,
		SamplingKey: fullKey,
		Forced:      forced,
	}
	switch {
	case forced:
		d.Accept = true
		d.Reason = fmt.Sprintf("forced accept (sampling key=%q hash=0x%016x rate=%.4f)", fullKey, hv, s.rate)
	case s.rate <= 0:
		d.Accept = false
		d.Reason = fmt.Sprintf("rate=0 => drop (sampling key=%q hash=0x%016x)", fullKey, hv)
	case s.rate >= 1 || s.full:
		d.Accept = true
		d.Reason = fmt.Sprintf("rate=1 => accept (sampling key=%q hash=0x%016x)", fullKey, hv)
	default:
		// s.full（rate=1）已在上面的 case 覆盖；此处一定 0<rate<1。
		d.Accept = hv < s.threshold
		cmp := "<"
		if !d.Accept {
			cmp = ">="
		}
		d.Reason = fmt.Sprintf("mix64(fnv1a(%q))=0x%016x %s threshold=0x%016x (rate=%.4f) => %s",
			fullKey, hv, cmp, s.threshold, s.rate, map[bool]string{true: "accept", false: "drop"}[d.Accept])
	}
	return d
}
