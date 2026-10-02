package group

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xingjianluo/trace-observability-go/trace"
)

// ReasonFailFast 是组级终止原因：因为打开了 fail-fast 且有成员失败，
// 组主动停掉了其余成员。
const ReasonFailFast = "fail_fast"

// Option 配置业务组。
type Option func(*options)

type options struct {
	name       string
	failFast   bool
	attributes map[string]any
}

// WithName 设置组名（同时作为组 Span 名，默认 "group"）。
func WithName(name string) Option {
	return func(o *options) {
		if name != "" {
			o.name = name
		}
	}
}

// WithFailFast 打开“一个成员失败就整体停下”：任一成员以 error 结束时，
// 立即取消组上下文，其余尚未结束的成员收到取消信号。
func WithFailFast() Option {
	return func(o *options) { o.failFast = true }
}

// WithAttribute 为组级汇总附加自定义属性（导出前由装配层脱敏）。
func WithAttribute(key string, value any) Option {
	return func(o *options) {
		if o.attributes == nil {
			o.attributes = map[string]any{}
		}
		o.attributes[key] = value
	}
}

// Group 把一批并发操作（可以再嵌套子组）当作一个整体管理：
//   - 取消信号从任意发起层（最外层、中间层、成员自己的调用方）向下传播；
//   - 每个成员恰好统计一次（并发结束、重复结束、panic 都不会重复计入）；
//   - Wait 返回时产出且仅产出一条组级汇总 Summary。
type Group struct {
	span        trace.Span
	ctx         context.Context
	parentCtx   context.Context
	delivery    DeliverySink
	summarySink SummarySink
	failFast    bool
	attributes  map[string]any

	wg sync.WaitGroup

	mu       sync.Mutex
	order    []string
	members  map[string]MemberResult
	pending  map[string]bool
	stopWhy  string // fail-fast 触发时提前记录的组级终止原因
	finished bool
	summary  Summary

	waitOnce sync.Once
}

// New 基于父上下文创建业务组，组本身对应一个 Span（占一个层级，
// 成员自动挂到组 Span 下）。父上下文中若已有业务组，新组会作为
// 该父组的直接成员登记，实现嵌套。
func New(parent context.Context, opts ...Option) *Group {
	if parent == nil {
		parent = context.Background()
	}
	o := options{name: "group"}
	for _, fn := range opts {
		fn(&o)
	}
	gctx, gs := trace.StartSpan(parent, o.name)
	g := &Group{
		span:        gs,
		ctx:         gctx,
		parentCtx:   parent,
		delivery:    deliverySinkFromContext(parent),
		summarySink: summarySinkFromContext(parent),
		failFast:    o.failFast,
		attributes:  o.attributes,
		members:     map[string]MemberResult{},
		pending:     map[string]bool{},
	}
	// 成员（包括成员内部再派生的子孙 Span）取到的当前组都是本 G。
	g.ctx = contextWithGroup(g.ctx, g)
	// 若父上下文中已有业务组，组 Span 作为父组的一个直接成员登记，
	// 父组因此会等本组收尾，且不会与“普通成员”重复计入。
	if pg := groupFromContext(parent); pg != nil {
		pg.Track(gs)
	}
	return g
}

// Context 返回组的共享上下文；成员派生自己的子操作时必须用它。
func (g *Group) Context() context.Context { return g.ctx }

// Span 返回组自身对应的 Span。
func (g *Group) Span() trace.Span { return g.span }

// SetAttribute 追加一个组级属性（只能在 Wait 之前调用）。
func (g *Group) SetAttribute(key string, value any) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.finished {
		return
	}
	if g.attributes == nil {
		g.attributes = map[string]any{}
	}
	g.attributes[key] = value
}

// Go 派生一个成员 Span 并在独立协程中执行 fn。Span 在调用方协程创建，
// 保证登记/投递回执注册先于成员结束发生。fn 返回后若 Span 未结束会
// 自动结束；panic 会被转为 error 终止，而不会炸掉进程或被重复统计。
func (g *Group) Go(name string, fn func(ctx context.Context, s trace.Span)) {
	ctx, s := trace.StartSpan(g.ctx, name)
	g.register(s.SpanID())
	if g.delivery != nil {
		g.delivery.Expect(s.SpanID())
	}
	trace.OnEndAttached(s, func(rec trace.SpanRecord) {
		g.endMember(rec)
	})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				// panic 与“停止/超时几乎同时发生”时，先写原因再以 error 结束；
				// End 幂等，只有第一次结论生效，不会出现前后不一致的双重归属。
				s.SetAttribute("panic", fmt.Sprintf("%v", r))
				s.SetAttribute("panic.stack", string(debug.Stack()))
				if !s.Ended() {
					s.End(trace.ReasonError)
				}
				return
			}
			if !s.Ended() {
				s.End("")
			}
		}()
		fn(ctx, s)
	}()
}

// Track 把一个已经存在的 Span（可能由其他协程、其他层级创建）登记为
// 直接成员。重复登记、或 Span 已经结束，都安全：成员仍只统计一次。
func (g *Group) Track(s trace.Span) {
	if s == nil {
		return
	}
	if !g.register(s.SpanID()) {
		return
	}
	if g.delivery != nil {
		g.delivery.Expect(s.SpanID())
	}
	var fired atomic.Bool
	trace.OnEndAttached(s, func(rec trace.SpanRecord) {
		if fired.CompareAndSwap(false, true) {
			g.endMember(rec)
		}
	})
	// 处理“登记时 Span 已经结束”的竞态：附加观察者因此成为空操作，
	// 这里补一次汇总；若观察者已经抢先执行，fired 会阻止重复。
	if s.Ended() && fired.CompareAndSwap(false, true) {
		g.endMember(s.Record())
	}
}

// Sub 在本组上下文内创建嵌套子组（自动登记为本组直接成员）。
func (g *Group) Sub(opts ...Option) *Group {
	return New(g.ctx, opts...)
}

// Stop 请求停止整个组：尚未结束的成员立即收到取消信号。
// 可重复调用；它不会代替 Wait，成员仍会各自结束并被汇总一次。
func (g *Group) Stop() {
	g.span.Cancel()
}

// Wait 等待全部直接成员（含嵌套子组）结束，并返回且仅返回一次组级汇总。
func (g *Group) Wait() Summary {
	g.waitOnce.Do(func() {
		g.wg.Wait()
		g.finalize()
	})
	return g.summary
}

// register 登记一个直接成员；重复 SpanID 返回 false（绝不重复计数）。
func (g *Group) register(spanID string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.finished {
		return false
	}
	if _, exists := g.members[spanID]; exists || g.pending[spanID] {
		return false
	}
	g.pending[spanID] = true
	g.order = append(g.order, spanID)
	g.wg.Add(1)
	return true
}

// endMember 在某个成员 Span 的首次结束（含管线投递完成）之后恰好调用一次。
func (g *Group) endMember(rec trace.SpanRecord) {
	mr := MemberResult{
		SpanID:        rec.SpanID,
		Name:          rec.Name,
		Status:        rec.Status,
		EndReason:     rec.EndReason,
		SamplingState: rec.SamplingState,
	}
	if g.delivery != nil {
		if d, ok := g.delivery.TakeDelivery(rec.SpanID); ok && !d.Accepted {
			mr.MissingReason = d.MissingReason
		}
	} else {
		// 没有装配层（纯 trace 单测）时：未采样链路视为未纳入。
		if rec.SamplingState == trace.SamplingDrop {
			mr.MissingReason = MissingSampledOut
		}
	}

	triggerFailFast := false
	g.mu.Lock()
	if !g.pending[rec.SpanID] {
		// 重复结束 / 非本组登记的 Span：忽略，成员只汇总一次。
		g.mu.Unlock()
		return
	}
	delete(g.pending, rec.SpanID)
	g.members[rec.SpanID] = mr
	if g.failFast && rec.Status == "error" && g.stopWhy == "" {
		g.stopWhy = ReasonFailFast
		triggerFailFast = true
	}
	g.mu.Unlock()

	if triggerFailFast {
		// 取消组 Span 的上下文：成员 Span 均由它派生，会同步收到信号，
		// 结束时各自记录 parent_canceled。
		g.span.Cancel()
	}
	g.wg.Done()
}

// finalize 在所有成员结束后构造唯一汇总、交给装配层并结束组 Span。
func (g *Group) finalize() {
	end := time.Now()

	g.mu.Lock()
	statusCounts := map[string]int{}
	reasonCounts := map[string]int{}
	missingReasons := map[string]int{}
	missing := 0
	memberCopy := make(map[string]MemberResult, len(g.members))
	for _, id := range g.order {
		mr := g.members[id]
		memberCopy[id] = mr
		statusCounts[mr.Status]++
		reasonCounts[mr.EndReason]++
		if mr.MissingReason != "" {
			missing++
			missingReasons[mr.MissingReason]++
		}
	}
	stopReason := g.computeStopReasonLocked()
	attrs := make(map[string]any, len(g.attributes))
	for k, v := range g.attributes {
		attrs[k] = v
	}
	sum := Summary{
		TraceID:        g.span.TraceID(),
		GroupSpanID:    g.span.SpanID(),
		ParentSpanID:   g.span.ParentSpanID(),
		Name:           g.span.Name(),
		StartTime:      g.span.Record().StartTime,
		EndTime:        end,
		Duration:       end.Sub(g.span.Record().StartTime),
		MembersTotal:   len(g.order),
		StatusCounts:   statusCounts,
		ReasonCounts:   reasonCounts,
		Members:        memberCopy,
		MemberSpanIDs:  append([]string(nil), g.order...),
		StoppedEarly:   stopReason != "" && stopReason != trace.ReasonCompleted,
		StopReason:     stopReason,
		MissingCount:   missing,
		MissingReasons: missingReasons,
		Depth:          g.span.Depth(),
		SamplingState:  g.span.SamplingState(),
		Attributes:     attrs,
	}
	if sum.Duration < 0 {
		sum.Duration = 0
	}
	g.summary = sum
	g.finished = true
	g.mu.Unlock()

	// 先交汇总，再结束组 Span：父组（若有）的观察者只会在 span.End 时
	// 触发，那时本组的汇总已经定型，父子归属永远对齐。
	if g.summarySink != nil {
		g.summarySink.DeliverSummary(sum)
	}
	// 组 Span 以判定出的整体原因显式结束，避免与自动判定不一致。
	g.span.End(stopReason)
}

// computeStopReasonLocked 必须在持锁时调用。优先级：
// fail-fast 联动 > 超时 > 父级取消 > 本组自取消 > 正常完成。
func (g *Group) computeStopReasonLocked() string {
	if g.stopWhy != "" {
		return g.stopWhy
	}
	switch g.ctx.Err() {
	case context.DeadlineExceeded:
		return trace.ReasonDeadlineExceeded
	case context.Canceled:
		// 区分信号来自更外层父级，还是本组（含成员自己的调用方）发起。
		if g.parentCtx != nil && g.parentCtx.Err() != nil {
			return trace.ReasonParentCanceled
		}
		return trace.ReasonCanceled
	default:
		return trace.ReasonCompleted
	}
}
