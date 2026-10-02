# trace-observability-go

一套**纯标准库、零外部依赖、本地可运行**的追踪与结构化日志管线。
目标是让并发场景下的**上下文关联、错误归属、数据脱敏、采样与导出**结果
可预期、可复现，并可在 `-race` 下稳定验证。

## 特性

- **跨协程 / 跨层级追踪传播**：子操作自动归属父级；父操作（或父 context）
  取消时，所有子操作被**同步取消**，并记录明确的终止原因
  （`completed` / `canceled` / `parent_canceled` / `deadline_exceeded` / `error`）。
  Span 结束幂等，不会出现孤立、错挂或重复计入的片段。
- **与追踪关联的结构化日志**：每条日志携带 `trace_id / span_id /
  parent_span_id / depth` 与进程内单调 `seq`；敏感字段按规则脱敏后才进入下游，
  原始输入 map 不会被修改，导出数据中不残留原始敏感值。
- **确定性采样**：基于 `mix64(FNV-1a(salt + ":" + key))` 与阈值比较，
  同一个 TraceID 永远得到同一结论，决策理由（哈希值、阈值、比率、forced）
  写入 Span 属性，可复现、可解释。
- **有界队列 + 批次导出**：容量有界，三种背压策略（丢新 / 丢旧 / 阻塞调用方）；
  `enqueued / dropped_new / dropped_old / exported / batches` 计数全部可观测；
  正常 `Shutdown` 时排空队列，**已接受数据不丢失**。
- **业务分组与嵌套汇总（`group` 包）**：把一批并发操作（可继续嵌套子组、
  成员可来自不同协程/层级）当成整体管理停止与收尾。支持外部取消、中间层
  停止、成员自身停止三向停止传播；默认失败隔离，可选 `WithFailFast()`
  “一个失败整体停”。每个组结束产出**恰好一条**组级汇总：成员总数、结果
  分布、是否被提前停及原因、耗时，以及“实际统计到 / 未采样 / 队列积压丢弃”
  的覆盖拆分；汇总与成员明细靠 `trace_id` + `group_span_id` 对齐。

## 包结构

| 包 | 职责 |
|----|------|
| `trace`    | Span、`context.Context` 传播、`Group` 并发派生、注入/提取（Carrier）、ID 源、Span 生命周期观察者 |
| `group`    | 业务组：可嵌套并发组、三向停止传播、失败策略、组级汇总与覆盖统计 |
| `sampling` | 确定性阈值采样器与可解释的 `Decision` |
| `logging`  | 关联追踪的结构化日志器、脱敏 `Masker` |
| `export`   | 有界队列、批次 JSON Lines 写出、背压策略与计数、Span/Group 视图 |
| `obs`      | 把上述组件装配为 `Pipeline`（含 `StartGroup`） |
| `cmd/demo` | 本地端到端示例 |

## 快速开始

```go
p, _ := obs.NewPipeline(obs.Config{
    Output:        os.Stdout,        // 任何 io.Writer（文件/内存缓冲）
    QueueCapacity: 1024,             // 有界队列
    BatchSize:     64,
    FlushInterval: 200 * time.Millisecond,
    DropPolicy:    export.DropNewest,
    SampleRate:    1.0,              // [0,1]；0=全不采样，1=全采样
    MaskRules:     logging.DefaultRules(),
    IDSource:      trace.NewCounterIDSource(), // 可选：确定性 ID（测试/复现）
})
defer p.Shutdown(context.Background())
log := p.Logger()

ctx, root := p.StartSpan(context.Background(), "order.checkout", false)

// 并发派生：必须用 StartSpan 返回的 ctx。
g := trace.NewGroup(ctx)
g.Go("payment.charge", func(ctx context.Context, s trace.Span) {
    log.Info(ctx, "charging", map[string]any{"phone": "13800138000"})
    // s 会在 fn 返回后自动结束；也可显式 s.End("...")
})
g.Wait()
root.End(trace.ReasonCompleted)

// 业务分组：一批并发操作 + 嵌套子组，收尾得到唯一组级汇总。
g2 := p.StartGroup(ctx, "order.fulfill")
g2.Go("reserve", func(ctx context.Context, s trace.Span) { /* ... */ })
sub := g2.SubGroup("shipment") // 或 group.WithFailFast() 开启一个失败整体停
sub.Go("carrier", func(ctx context.Context, s trace.Span) { /* ... */ })
sub.Wait()
summary := g2.Wait()            // group.Summary：成员分布/停止原因/coverage
_ = summary.Coverage.Observed   // 实际纳入统计的成员数
```

运行示例：

```bash
go run ./cmd/demo -out /tmp/demo.jsonl -rate 1.0 -capacity 256
```

输出为 **JSON Lines**：每行是一个批次（JSON 数组），数组元素形如
`{"kind":"span|log","data":{...},"trace_id":...,"span_id":...,"accepted":true}`。

## 上下文传播与生命周期语义

1. **Span 与 context 绑定**。`StartSpan` 从传入 `ctx` 读取父 Span，
   并返回一个**新的 ctx**。所有下游调用（含 `trace.NewGroup`）都必须使用这个
   返回的 ctx，子 Span 才能正确挂到父级。直接用 `span.Context()` 而绕开
   返回 ctx 派生兄弟/组，是最常见的错挂来源。
2. **跨协程传播靠 context**。`Group.Go` 内部在共享的派生 ctx 上 `StartSpan`，
   并在独立 goroutine 中执行；`SpanFromContext(ctx)` 在子协程内取回的
   一定是该子协程自己的 Span，日志因此不会串号。
3. **取消是同步传播的**。Span 的 ctx 由父 Span 的 ctx 派生
   （`context.WithCancel(parent.Context())`）。父 context 取消后：
   - 子 Span 的 `ctx.Done()` 立即关闭；
   - 子 Span 结束时若发现“自己未主动取消、而父 context 已取消”，
     记录终止原因为 `parent_canceled`；自己取消父仍存活则记为 `canceled`；
     超时记为 `deadline_exceeded`。
4. **结束幂等**。`End` 多次调用只有第一次生效（第一次的原因/状态胜出），
   结束回调只触发一次，避免重复计入。`Group.Go` 在 fn 返回后兜底结束 Span，
   fn 内 panic 会被记录到 Span 属性（`panic` / `panic.stack`）并以 `error` 结束，
   不会让单个子协程崩溃整个进程。
5. **装配选项随链路继承**。采样结论与“结束→导出”回调通过 context 自动向
   后代 Span 传播，因此 `Group.Go` 这类“拿不到父 Span 创建参数”的派生点
   也能被正确采样与导出。
6. **跨进程边界**。`trace.Inject(ctx, carrier)` / `trace.Extract(ctx, carrier)`
   使用三个头：`x-trace-traceid`、`x-trace-spanid`、`x-trace-sampled`。
   提取出的远端父级只携带标识（不导出、不可取消），本地子 Span 沿用其
   TraceID 并把 ParentSpanID 指向它。
7. **ID 可复现**。默认 `CounterIDSource` 用进程内单调计数器生成 16 位
   十六进制定长 ID；测试可 `trace.SetIDSource(trace.NewCounterIDSource())`
   重置序列，使输出逐条可比对。

## 业务分组与嵌套汇总

`group.Group` 在 Span 之上提供“业务组”语义：一次业务处理拆成多个阶段，
每个阶段还能派生自己的并发动作甚至子组，收尾时按组给出一条整体结论，
而不用手工拼一堆散落片段。

```go
g := p.StartGroup(ctx, "order.fulfill") // 返回的 *group.Group
g.Go("reserve", func(ctx context.Context, s trace.Span) { ... })

sub := g.SubGroup("shipment")           // 嵌套子组
sub.Go("call.carrier", func(ctx context.Context, s trace.Span) { ... })
summary := sub.Wait()                   // 子组先收尾
summary = g.Wait()                      // 父组收尾，产出唯一汇总
```

### 归属与嵌套

- **每个组拥有自己的组 Span**。成员（`g.Go` 的协程，以及成员 fn 内直接
  `StartSpan` 的后代 Span）都是该组的**直接成员**；成员内部再嵌套的
  `g.SubGroup` 则是一个独立组——它自己的组 Span 作为父组的一个直接成员，
  它的内部成员归它自己统计，不重复计入父组。
- 成员与组靠 `trace_id` 和 `group_span_id` 对齐：每个成员 Span/汇总都带
  最近组的 `group_span_id`，子组汇总带 `parent_group_span_id`。
  **不会出现找不到归属的成员，也不会把同一成员重复计入两个组。**
- 成员可以由不同协程、不同层级派生；`Go` 在调用方协程同步预占“协程槽”，
  槽位直到 fn 返回（内部 Span 都已结束）才释放，因此 `Wait` 不会在
  “成员 Span 已结束、内部 Span 尚未启动”的窗口里提前归零。

### 停止传播与异常

停止信号有三个来源，成员都会及时收到并各自记下**唯一且一致**的原因：

| 来源 | 组/成员的记录 |
|------|---------------|
| 最外层父 context 取消 / 超时 | 汇总 `stopped=true, stop_reason=external_cancel`；成员 `parent_canceled` / `deadline_exceeded` |
| 中间层（某组）`g.Stop()` | 该组及尚未结束的下游收到信号，成员记 `parent_canceled`；停止原因记 `stopped` |
| 组内成员自己的调用方 `s.Cancel()` | 仅该成员记 `canceled`，组整体不算被停，其他成员不受影响 |

- 终止原因用 `context.Cause` 沿取消链追溯，并以“最外层派生源 ctx”为权威
  （业务组把它显式注入，组自身的 `Stop` 取消不会污染该源），因此
  **停止与超时几乎同时发生时不会前后给出矛盾结论**：外部超时稳定判为
  `deadline_exceeded` + `external_cancel`。
- 成员 Span 的 `End` 幂等：并发结束、重复结束、panic 兜底结束都只有第一次
  结论生效，一个成员**不可能既算正常完成又算被停**。
- fn 内 panic 会被捕获（写入 `panic` / `panic.stack` 属性），该成员以
  `error` 结束并标记 `panic=true`，不会崩溃整个进程。

### 失败策略

- **默认失败隔离**：一个成员 `error`/panic 不影响其他成员，失败只体现在
  汇总的 `error_count` 与 `outcome_counts` 里，组整体 `stopped=false`。
- **FailFast**：`group.New(..., group.WithFailFast())`（或 `StartGroup`
  传入该 option）时，第一个 `error`/panic 成员立即取消全组，其余未结束
  成员记 `parent_canceled`，汇总 `stop_reason=fail_fast_member_error`。

### 组级汇总（恰好一条）

`Wait()`/`Close()` 返回 `group.Summary`，并通过 emitter（接管线时即导出
一条 `"group"` 记录）**恰好触发一次**——成员并发结束、重复 `Wait` 都不会
产生第二条。字段包括：

- `total` / `members`：直接成员数量与明细（按组内登记序 `start_seq` 排列，
  输出可复现），每个 span_id 只出现一次；
- `outcome_counts`、`success_count`、`error_count`、`canceled_count`：
  结果分布；
- `stopped`、`stop_reason`、`group_status`、`end_reason`：整体是否被提前停、
  因何而停、组 Span 的状态与终止原因；
- `duration_nanos`：组整体耗时；
- `coverage`：覆盖统计（见下）。

### 覆盖范围如何受采样与背压影响

汇总**不默认所有成员的明细都已落盘**。`coverage` 明确区分：

| 字段 | 含义 |
|------|------|
| `total` | 直接成员总数（与采样/队列无关，成员一个不漏地计数） |
| `observed` | 明细被下游实际**接受（入队）**的成员数 |
| `sampled_out` | 所在链路未被采样（rate=0 等），明细不会导出 |
| `queue_dropped` | 采样命中但有界队列积压（DropNewest/DropOldest）没能入队 |
| `closed` | 下游已关闭（如收尾之后）导致明细未被接受 |
| `unknown` | 未装配管线（直接用 trace/group 包）、没有投递结论 |

恒有 `observed + sampled_out + queue_dropped + closed + unknown == total`。

- **采样**：子 Span 继承根链路结论，整条 drop 链路既不导出成员 Span，
  也不导出组汇总（`Summary` 仍在内存中返回，`sampled_out` 照样可查）。
- **背压**：全采样 + 队列满时，没入队的成员明细计入 `queue_dropped`，
  丢弃原因可由导出器 `Snapshot()` 的 `dropped_new/dropped_old` 交叉核对。
- **收尾**：`Shutdown` 排空队列，已经入队的汇总与成员明细都不丢。
- **脱敏**：组属性（`g.SetAttribute`）与成员 Span 属性在入队前都按同一套
  `MaskRules` 脱敏（字符串 map 走 `MaskStringMap`），汇总不会因为带了
  成员属性而泄露原始敏感值。
- **可复现**：配合确定性 `CounterIDSource`，相同输入重复跑得到一致的
  ID、成员顺序、结果分布与覆盖结论。

## 采样策略

- 根 Span 以 `"trace:"+TraceID` 为采样键，判定 `mix64(fnv1a(salt:key)) < rate*2^64`。
  之所以在 FNV-1a 外再加 splitmix64 雪崩混合，是因为连续/高度相似的键
  （如顺序 TraceID）在纯 FNV-1a 下高位聚集，会导致阈值采样明显失真。
- **子 Span 不重新决策**，直接继承根链路的 `accept/drop`，保证一条追踪链路
  结论一致。被 `drop` 的链路既不导出 Span，也不产生日志。
- `StartSpan(ctx, name, forced=false)` 的 `forced=true` 用于错误等强制采样路径，
  即使 rate=0 也会 accept。
- 决策依据写入 Span 属性 `sampling.decision_reason`，例如：
  `mix64(fnv1a("trace:0000..0001"))=0x.. < threshold=0x.. (rate=0.3000) => accept`。
- 配置盐 `SampleSalt` 可在不同部署间得到不同但各自稳定的采样集合。
- `SampleRate` 取值：`0`=全不采样（合法），`1`=全采样，`(0,1)`=概率采样；
  `<0/NaN` 视为未设置，按全采样处理。

## 有界队列、背压与丢弃策略

`export.Exporter` 是“有界队列 + 单消费协程 + 批次写出”：

| 策略 | 队列满时行为 | 计数 |
|------|--------------|------|
| `DropNewest`（默认） | 丢弃**新到达**的记录，保护已入队数据 | `dropped_new++` |
| `DropOldest` | 丢弃队首**最旧**记录，为新记录腾位 | `dropped_old++` |
| `BlockCaller` | 阻塞调用方直到有空位（真正背压，不丢数据）；ctx 取消立即返回 ctx 错误 | 不丢 |

- `Exporter.Snapshot()` 返回单调累计的 `Stats{Enqueued, DroppedNew,
  DroppedOld, Exported, Batches}`，丢弃**数量与原因**因此可观测。
  守恒关系：`入队尝试数 = Enqueued + DroppedNew + DroppedOld`；
  收尾后 `Enqueued == Exported`。
- 批次由“达到 `BatchSize`”或“到达 `FlushInterval`”触发；
  `Flush(ctx)` 可同步排空（排空后会唤醒阻塞的生产者）。
- `Shutdown(ctx)`：停止接收（之后 `Enqueue` 返回 `ErrClosed`）→
  唤醒所有阻塞生产者 → 消费协程把队列**全部**分批写出 → 退出。
  成功返回后，所有**已接受**的数据都已落盘，不丢失。

## 脱敏配置

`logging.Masker` 按**字段名**（大小写不敏感）匹配，支持三种动作：

- `MaskFull`：整体替换为常量 `***REDACTED***`；
- `MaskPartial`：保留首 2、尾 1 字符，中间打码（长度 <5 整体打码）；
- `MaskHash`：`sha256:` + 带盐 SHA-256 摘要前 16 位，稳定且不可逆，可用于关联。

规则在**日志投递之前**应用，递归处理嵌套 `map` / `[]any`；
`MaskFields` 返回深拷贝，原始输入 map 与其中的原始敏感值保持不变。
`logging.DefaultRules()` 内置 `password/token/authorization/api_key/secret/
phone/email/id_card/bank_card/user.id` 等常见字段。自定义示例：

```go
masker := logging.NewMasker([]logging.Rule{
    {Field: "id_card", Action: logging.MaskPartial},
    {Field: "user.id", Action: logging.MaskHash, Salt: "per-deploy-secret"},
    {Field: "internal_note", Action: logging.MaskFull},
})
```

## 本地验证

```bash
# 全部测试，开启竞态检测
go test ./... -race -count=1

# 提高时序/竞态覆盖，重复多轮
go test ./... -race -count=10

# 静态检查
go vet ./...

# 端到端示例（含并发归属、父取消、队列积压丢弃、脱敏、收尾不丢）
go run ./cmd/demo -out /tmp/demo.jsonl
```

测试覆盖：

- `trace`：层级归属、并发 64 子 Span 归属与计数、父取消传播为
  `parent_canceled`、子取消不波及父、超时原因、End 幂等、注入/提取往返。
- `group`：嵌套组归属（子组组 Span 是父组直接成员、trace/父子组 id 对齐）、
  扇出过程中整体停（外部取消/显式 Stop/成员自停三向原因）、FailFast 与
  默认失败隔离、panic 成员计 error 且只计一次、成员并发结束 + 汇总恰好一条、
  停止与超时竞态下结论稳定一致。
- `sampling`：同键结论稳定、rate=0/1 边界、forced、近似比率分布、盐、并发安全。
- `logging`：关联字段、脱敏后无原始值残留（含嵌套）、原 map 不被修改、
  32 协程并发日志不串号、drop 链路不产生日志。
- `export`：三种背压策略、丢弃计数与守恒、阻塞可被 Flush/cancel 解除、
  Shutdown 后入队报错、500 条收尾零丢失、并发入队竞态。
- 根目录集成测试：端到端并发归属、父取消、采样稳定可解释、队列积压丢弃
  可观测、收尾完整、混合负载脱敏，以及分组专项——
  嵌套组 + 汇总仅一条、扇出被整体停、成员并发结束不重复、
  低采样叠加队列积压的 `coverage` 拆分（sampled_out / queue_dropped）、
  组汇总与成员属性脱敏、相同输入可复现。

所有失败断言都会打印**输入、实际结果、期望值与判定依据**，便于复盘。

## 设计边界（非目标）

- 不实现网络上报 / OTLP / 外部后端；导出目标是任意 `io.Writer`。
- 不做时钟同步、不保证跨进程全局唯一 ID（`CounterIDSource` 仅进程内确定）。
- 脱敏为字段名规则匹配，不做语义识别；具体规则由使用方配置。
