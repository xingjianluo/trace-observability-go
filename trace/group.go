package trace

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
)

// Group 用于等待一组并发子操作，并保证子 Span 在 Wait 返回前全部结束。
// 组内所有子 Span 共享同一个派生上下文：父上下文取消时，全部子操作
// 会同步收到取消信号，不会出现孤立或错挂的追踪片段。
type Group struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	opts   []SpanOption
}

// NewGroup 基于父上下文创建并发组。
func NewGroup(parent context.Context, opts ...SpanOption) *Group {
	ctx, cancel := context.WithCancel(parent)
	return &Group{ctx: ctx, cancel: cancel, opts: opts}
}

// Context 返回该组的共享上下文。
func (g *Group) Context() context.Context { return g.ctx }

// Go 派生子 Span 并在独立协程中执行 fn；父取消会同步传播给子。
// fn 返回后若子 Span 尚未结束，会自动结束它，避免遗漏或重复计入。
func (g *Group) Go(name string, fn func(ctx context.Context, s Span)) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		ctx, s := StartSpan(g.ctx, name, g.opts...)
		defer func() {
			if r := recover(); r != nil {
				// 不让单个子协程的 panic 崩溃整个进程；
				// 记录原因与堆栈，该子 Span 以 error 结束。
				s.SetAttribute("panic", fmt.Sprintf("%v", r))
				s.SetAttribute("panic.stack", string(debug.Stack()))
			}
			if !s.Ended() {
				s.End("")
			}
		}()
		fn(ctx, s)
	}()
}

// Wait 等待全部子操作结束。组上下文本身不会被 Wait 取消。
func (g *Group) Wait() {
	g.wg.Wait()
}

// Cancel 取消整个组的共享上下文（通常由父操作在取消时调用）。
func (g *Group) Cancel() { g.cancel() }
