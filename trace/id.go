package trace

import (
	"sync"
	"sync/atomic"
)

// IDSource 负责生成稳定、可复现的 TraceID / SpanID。
type IDSource interface {
	NewTraceID() string
	NewSpanID() string
}

// CounterIDSource 使用进程内单调递增计数器生成定长十六进制 ID。
// 结果完全确定，便于测试复现与按序核对。
type CounterIDSource struct {
	traceSeq atomic.Uint64
	spanSeq  atomic.Uint64
}

// NewCounterIDSource 创建从 1 开始计数的 ID 源。
func NewCounterIDSource() *CounterIDSource {
	s := &CounterIDSource{}
	s.traceSeq.Store(0)
	s.spanSeq.Store(0)
	return s
}

// NewTraceID 返回形如 "0000000000000001" 的 TraceID。
func (s *CounterIDSource) NewTraceID() string {
	return formatID(s.traceSeq.Add(1))
}

// NewSpanID 返回形如 "0000000000000001" 的 SpanID。
func (s *CounterIDSource) NewSpanID() string {
	return formatID(s.spanSeq.Add(1))
}

func formatID(n uint64) string {
	const hexdigits = "0123456789abcdef"
	var b [16]byte
	for i := 15; i >= 0; i-- {
		b[i] = hexdigits[n&0xf]
		n >>= 4
	}
	return string(b[:])
}

var (
	idMu     sync.RWMutex
	idSource IDSource = NewCounterIDSource()
)

// DefaultIDSource 返回当前进程级默认 ID 源。
func DefaultIDSource() IDSource {
	idMu.RLock()
	defer idMu.RUnlock()
	return idSource
}

// SetIDSource 安装进程级 ID 源（主要供测试注入确定性序列）。
// 传入 nil 会恢复为默认计数器源。
func SetIDSource(src IDSource) {
	idMu.Lock()
	defer idMu.Unlock()
	if src == nil {
		src = NewCounterIDSource()
	}
	idSource = src
}
