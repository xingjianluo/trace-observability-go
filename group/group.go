package group

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/xingjianluo/trace-observability-go/trace"
)

// SpanStarter 抽象“创建一个 Span”的入口。
// 生产环境由 obs.Pipeline 实现（带采样决策与导出回调）；
// 直接使用 trace 包时可用 DefaultSpanStarter。
type SpanStarter interface {
	StartSpan(ctx context.Context, name string, forced bool, opts ...trace.SpanOption) (context.Context, trace.Span)
}

// EmitSummaryFunc 接收组结束时产出的唯一汇总（装配层负责脱敏与导出）。
type EmitSummaryFunc func(s Summary)

// Config 描述组的配置。
type Config struct {
	// Name 为组名（也是组 Span 的名字）。
	Name string
	// FailFast 为 true 时，任一成员 error/panic 立即取消全组；
	// 默认 false：单个成员失败不影响其他成员，失败只体现在汇总里。
	FailFast bool
	// Starter 负责创建组 Span 与成员 Span；nil 时使用 trace 直接启动。
	Starter SpanStarter
	// Emit 在 Wait/Close 时被调用且仅调用一次，用于导出汇总。
	Emit EmitSummaryFunc
}

// Option 配置组。
type Option func(*Config)

// WithFailFast 开启“一个失败就整体停下”。
func WithFailFast() Option {
	return func(c *Config) { c.FailFast = true }
}

// WithStarter 注入 Span 启动器（通常由 obs.Pipeline.StartGroup 完成）。
func WithStarter(s SpanStarter) Option {
	return func(c *Config) { c.Starter = s }
}

// WithSummaryEmitter 注入汇总出口。
func WithSummaryEmitter(f EmitSummaryFunc) Option {
	return func(c *Config) { c.Emit = f }
}

// Group 是一个可嵌套的业务并发组。
type Group struct {
	cfg      Config
	parent   context.Context
	groupCtx context.Context
	cancel   context.CancelCauseFunc
	span     trace.Span
	outCtx   context.Context // 携带本组观察者与 group id 的成员派生上下文
	done     chan struct{}   // 组生命周期结束（cancel 也会关闭它）

	mu         sync.Mutex
	cond       *sync.Cond
	slots      int    // 在途名额总数：Go 协程槽 + 未预占的内部 Span
	pending    int    // 已预占但成员 Span 尚未 StartSpan 消费的名额
	seq        int    // 成员登记序号
	closed     bool   // Wait/Close 已完成归零
	stopped    bool   // 组是否被提前停止
	stopReason string // 停止原因（StopReason* 常量）

	members []Member
	seen    map[string]bool // spanID -> 是否已结束，防重复计入
	// reserved 记录“由 Go/子组预占槽覆盖”的成员 SpanID；
	// 这些成员的 slot 在 fn 返回/子组收尾时统一释放，而不是在 Span End 时。
	reserved map[string]struct{}
	once     sync.Once
	summary  Summary

	attrs map[string]string
	start time.Time
}

// New 创建业务组：在 parent 下创建组 Span，并返回派生上下文（成员从该 ctx 派生）。
//
// 停止传播：parent 取消/超时会同步传到组与全部成员；组也可用 Stop 自行停止；
// FailFast 下任一成员 error/panic 会取消全组。三种来源都记录在汇总里。
func New(parent context.Context, name string, opts ...Option) *Group {
	cfg := Config{Name: name}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.Starter == nil {
		cfg.Starter = DefaultSpanStarter{}
	}

	g := &Group{
		cfg:      cfg,
		parent:   parent,
		seen:     map[string]bool{},
		reserved: map[string]struct{}{},
		attrs:    map[string]string{},
		start:    time.Now(),
		done:     make(chan struct{}),
	}
	g.cond = sync.NewCond(&g.mu)

	// 组自己的可取消上下文：Stop / fail-fast 通过它向下传播。
	// 用 WithCancelCause，并包装 cancel：组主动停止时若父 ctx 已取消
	// （例如 deadline 与 Stop 几乎同时），透传父的真实 cause，
	// 避免把成员的终止原因固化成普通 canceled。
	rawGroupCtx, rawCancel := context.WithCancelCause(parent)
	g.groupCtx = rawGroupCtx
	g.cancel = func(cause error) {
		if cause == nil {
			if pc := context.Cause(parent); pc != nil {
				cause = pc // 透传父级 deadline/cancel 真实原因
			}
		}
		rawCancel(cause)
	}

	// 创建组 Span。它继承 parent 中最近分组的观察者——因此嵌套时，
	// 子组的组 Span 恰好是父组的一个直接成员（向父组登记名额）。
	// 必须用 StartSpan 返回的 spanCtx（携带当前 Span 键）作为成员派生基础；
	// 仅用 span.Context() 会丢掉 spanKey，导致嵌套子组找不到父 Span。
	// 注入 g.parent 作为外部源：它是调用方传入的最外层 ctx，deadline/取消
	// 的真实原因不会被本组 Stop 的 cancel 覆盖，保证竞态下结论确定。
	groupSpanCtx := trace.ContextWithOrigin(g.groupCtx, g.parent)
	spanCtx, gs := cfg.Starter.StartSpan(groupSpanCtx, name, false)
	g.span = gs

	// 成员派生上下文：先放本组的 collector（覆盖继承来的父组观察者），
	// 再放最近分组 id。本组成员向本组登记；本组的组 Span 仍向父组登记。
	g.outCtx = trace.ContextWithGroupID(spanCtx, g.span.SpanID())
	g.outCtx = trace.ContextWithSpanObserver(g.outCtx, g)
	return g
}

// Context 返回组成员应使用的派生上下文（携带本组归属与取消传播）。
func (g *Group) Context() context.Context { return g.outCtx }

// Span 返回组自身的 Span。
func (g *Group) Span() trace.Span { return g.span }

// SetAttribute 设置组级属性（同时写入组 Span 与汇总，导出时脱敏）。
func (g *Group) SetAttribute(key, value string) {
	g.mu.Lock()
	g.attrs[key] = value
	g.mu.Unlock()
	g.span.SetAttribute(key, value)
}

// reserve 在调用方协程同步预占一个协程槽，保证“派生先于 Wait 归零”。
// 该槽一直占用到 fn 返回（fn 内派生的内部 Span 必已结束），
// 因此成员 Span 结束与内部 Span 启动之间不会出现零名额窗口。
func (g *Group) reserve() {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		// 与 sync.WaitGroup 同样的误用约束：归零结束后再派生属于编程错误。
		panic("group: Go/SubGroup called after Wait completed")
	}
	g.slots++
	g.pending++
	g.mu.Unlock()
}

// releaseSlot 释放一个预占槽（Go 协程 fn 已返回 / 子组已登记完成）。
func (g *Group) releaseSlot() {
	g.mu.Lock()
	g.slots--
	g.cond.Broadcast()
	g.mu.Unlock()
}

// Go 预占一个协程槽并在独立协程中启动成员 Span 执行 fn。
// fn 返回后成员 Span 自动结束；panic 会被捕获并记为 error，不会崩溃进程。
func (g *Group) Go(name string, fn func(ctx context.Context, s trace.Span)) {
	g.reserve()
	go func() {
		// fn 完全返回（含内部 Span 结束）后才释放协程槽。
		defer g.releaseSlot()
		ctx, s := g.cfg.Starter.StartSpan(g.outCtx, name, false)
		defer func() {
			if r := recover(); r != nil {
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

// SubGroup 在本组下创建嵌套子组；子组自身的组 Span 是本组的直接成员。
// 子组在自己的 Wait 时结束组 Span（即向本组释放名额），因此即便调用方
// 不显式等待子组，父组 Wait 也会等到子组完全收尾。
func (g *Group) SubGroup(name string, opts ...Option) *Group {
	// 子组默认沿用本组的 starter 与 emitter，保证采样/导出/汇总链路一致。
	opts = append([]Option{
		WithStarter(g.cfg.Starter),
		WithSummaryEmitter(g.cfg.Emit),
	}, opts...)
	g.reserve() // 子组的组 Span 将作为本组的一个直接成员，先预占名额
	sub := New(g.outCtx, name, opts...)
	// 子组 New 内 StartSpan 同步回调本组 OnSpanStart 消费 pending 并标记
	// reserved；预占槽在子组完全收尾（done 关闭，组 Span 已结束并已向本组
	// OnSpanEnd）后释放，因此父组 Wait 一定晚于子组全部成员收尾。
	go func() {
		<-sub.done
		g.releaseSlot()
	}()
	return sub
}

// Stop 显式停止整组：取消组上下文，未结束成员收到信号并记录停止原因。
// 可从最外层/中间层/成员协程内任意调用，幂等。
func (g *Group) Stop(reason string) {
	if reason == "" {
		reason = StopReasonStopped
	}
	g.markStopped(reason)
	g.cancel(nil)
}

// markStopped 只记录停止原因（先到先得），不触发取消。
func (g *Group) markStopped(reason string) {
	g.mu.Lock()
	if !g.stopped {
		g.stopped = true
		g.stopReason = reason
	}
	g.mu.Unlock()
}

// markFailFast 在 FailFast 模式下被成员失败触发：记录原因并取消全组。
func (g *Group) markFailFast() {
	if !g.cfg.FailFast {
		return
	}
	g.mu.Lock()
	first := !g.stopped
	if first {
		g.stopped = true
		g.stopReason = StopReasonFailFast
	}
	g.mu.Unlock()
	if first {
		g.cancel(nil)
	}
}

// Wait 等待全部直接成员（含嵌套子组）结束，产出并返回唯一汇总。
// 重复调用返回同一条汇总；汇总恰好产出一次（成员并发/重复结束都只计一次）。
func (g *Group) Wait() Summary {
	g.mu.Lock()
	for g.slots > 0 {
		g.cond.Wait()
	}
	// 允许在仍有成员协程存活时继续 fan-out（与 WaitGroup 同约束：
	// 新派生必须发生在本次 Wait 观察到归零之前）。归零后标记结束。
	g.closed = true
	g.mu.Unlock()

	g.once.Do(g.finish)
	return g.summary
}

// finish 在归零后恰好执行一次：判定组终止原因、结束组 Span、构建并发出汇总。
func (g *Group) finish() {
	close(g.done)
	end := time.Now()

	g.mu.Lock()
	stopped := g.stopped
	stopReason := g.stopReason
	g.mu.Unlock()

	// 停止原因的确定优先级：外部父级取消/超时 > 显式 Stop > fail-fast。
	// 这样“停止与超时几乎同时发生”也给出前后一致的唯一结论。
	if g.parent.Err() != nil {
		stopped = true
		stopReason = StopReasonExternal
	}

	// 组 Span 的终止原因。
	groupReason := trace.ReasonCompleted
	groupStatus := "ok"
	switch {
	case g.parent.Err() == context.DeadlineExceeded:
		groupReason = trace.ReasonDeadlineExceeded
		groupStatus = "canceled"
	case g.parent.Err() != nil:
		groupReason = trace.ReasonParentCanceled
		groupStatus = "canceled"
	case stopped:
		groupReason = trace.ReasonCanceled
		groupStatus = "canceled"
	}
	g.span.End(groupReason)

	// 回写最终停止标记，保证汇总字段与组 Span 结论一致。
	g.mu.Lock()
	g.stopped = stopped
	g.stopReason = stopReason
	g.mu.Unlock()

	sum := g.buildSummary(end, groupReason, groupStatus, stopReason)
	if g.cfg.Emit != nil {
		g.cfg.Emit(sum)
	}
}

// buildSummary 在持锁状态下从成员明细构建不可变汇总。
func (g *Group) buildSummary(end time.Time, groupReason, groupStatus, stopReason string) Summary {
	g.mu.Lock()
	defer g.mu.Unlock()

	counts := map[string]int{}
	members := make([]Member, len(g.members))
	copy(members, g.members)
	// 按登记顺序输出，保证相同输入下结论顺序可复现。
	sortMembers(members)

	cov := Coverage{Total: len(members)}
	var errN, okN, cancelN int
	for _, m := range members {
		counts[m.Outcome]++
		switch m.Outcome {
		case OutcomeCompleted:
			okN++
		case OutcomeError:
			errN++
		default:
			cancelN++
		}
		switch m.Delivery {
		case trace.DeliveryOutcomeAccepted:
			cov.Observed++
		case trace.DeliveryOutcomeNotSampled:
			cov.SampledOut++
		case trace.DeliveryOutcomeDroppedQueue:
			cov.QueueDropped++
		case trace.DeliveryOutcomeClosed:
			cov.Closed++
		default:
			cov.Unknown++
		}
	}

	attrs := make(map[string]string, len(g.attrs))
	for k, v := range g.attrs {
		attrs[k] = v
	}

	g.summary = Summary{
		TraceID:       g.span.TraceID(),
		GroupSpanID:   g.span.SpanID(),
		ParentSpanID:  g.span.ParentSpanID(),
		ParentGroup:   g.span.Record().Attributes["group_span_id"],
		Name:          g.cfg.Name,
		StartTime:     g.start,
		EndTime:       end,
		Duration:      end.Sub(g.start),
		FailFast:      g.cfg.FailFast,
		Stopped:       g.stopped,
		StopReason:    stopReason,
		GroupStatus:   groupStatus,
		EndReason:     groupReason,
		OutcomeCounts: counts,
		ErrorCount:    errN,
		SuccessCount:  okN,
		CanceledCount: cancelN,
		Coverage:      cov,
		Members:       members,
		Attributes:    attrs,
		SamplingState: g.span.SamplingState(),
	}
	return g.summary
}

// Close 等价于 Wait（便于 defer）。
func (g *Group) Close() Summary { return g.Wait() }
