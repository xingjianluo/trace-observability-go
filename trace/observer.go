package trace

// OnEndAttached 在 Span 已创建之后追加一个“结束观察者”。
//
// 与 WithOnEnd 的区别：
//   - WithOnEnd 在创建时注入，并经由 inherit 机制自动向所有后代 Span 传播；
//   - OnEndAttached 在创建后登记，只对“当前这一个 Span”生效，
//     不会被它的子孙继承——用于业务组精确聚合自己登记的成员，
//     避免成员自己派生的内部子操作被误算成组成员。
//
// 观察者在 Span 真正结束（End 的首次生效调用）时，于装配层主回调之后
// 同步触发，且每个 Span 至多触发一次；Span 已结束后再登记为空操作。
func OnEndAttached(s Span, fn func(SpanRecord)) {
	if s == nil || fn == nil {
		return
	}
	type attacher interface {
		attachEndObserver(func(SpanRecord))
	}
	if a, ok := s.(attacher); ok {
		a.attachEndObserver(fn)
	}
}
