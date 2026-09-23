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

## 包结构

| 包 | 职责 |
|----|------|
| `trace`    | Span、`context.Context` 传播、`Group` 并发派生、注入/提取（Carrier）、ID 源 |
| `sampling` | 确定性阈值采样器与可解释的 `Decision` |
| `logging`  | 关联追踪的结构化日志器、脱敏 `Masker` |
| `export`   | 有界队列、批次 JSON Lines 写出、背压策略与计数 |
| `obs`      | 把上述组件装配为 `Pipeline` |
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
- `sampling`：同键结论稳定、rate=0/1 边界、forced、近似比率分布、盐、并发安全。
- `logging`：关联字段、脱敏后无原始值残留（含嵌套）、原 map 不被修改、
  32 协程并发日志不串号、drop 链路不产生日志。
- `export`：三种背压策略、丢弃计数与守恒、阻塞可被 Flush/cancel 解除、
  Shutdown 后入队报错、500 条收尾零丢失、并发入队竞态。
- 根目录集成测试：端到端并发归属、父取消、采样稳定可解释、队列积压丢弃
  可观测、收尾完整、混合负载脱敏。

所有失败断言都会打印**输入、实际结果、期望值与判定依据**，便于复盘。

## 设计边界（非目标）

- 不实现网络上报 / OTLP / 外部后端；导出目标是任意 `io.Writer`。
- 不做时钟同步、不保证跨进程全局唯一 ID（`CounterIDSource` 仅进程内确定）。
- 脱敏为字段名规则匹配，不做语义识别；具体规则由使用方配置。
