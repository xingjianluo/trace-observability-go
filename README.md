# trace-observability-go

一套**完全本地、零外部依赖**的追踪（tracing）与结构化日志（structured logging）管线。
目标是让并发调用下的**上下文关联、错误归属、数据脱敏**结果可预期、可复现。

- 纯标准库实现，不启动任何网络服务；
- 导出目标是本地 JSONL 文件（测试中是内存 buffer）；
- 全部测试在 `-race` 竞态检测下通过。

---

## 1. 包结构

| 包 | 职责 |
|---|---|
| `tracing` | trace/span 标识、上下文传播、父子层级、取消级联 |
| `logx` | 与 span 关联的结构化日志、脱敏、JSONL 渲染 |
| `sampler` | 确定性、可解释的采样决策（仅作用于根 span） |
| `export` | 有界队列（背压/丢弃策略）、批处理、可插拔导出器、指标 |
| `pipeline` | 组装以上组件，提供确定性的启动/优雅关停生命周期 |
| `cmd/demo` | 本地可运行的完整示例，输出到 `./data/pipeline.jsonl` |

---

## 2. 上下文传播与生命周期语义

### 2.1 标识与层级

- 每个 trace 有 16 字节 `TraceID`，每个 span 有 8 字节 `SpanID`。
- `tracer.Start(ctx, name)`：
  - `ctx` 上没有 span → 创建**根 span**，生成新的 `TraceID`，此时调用采样器；
  - `ctx` 上有 span → 创建**子 span**，复用父级 `TraceID`，记录 `ParentSpanID`，`Depth = 父Depth + 1`。
- span 上下文只通过显式传入的 `context.Context` 传播，不使用任何全局变量，
  因此并发分支不可能串号。

### 2.2 跨协程派生（推荐用 `Tracer.Go`）

```go
join := tracer.Go(ctx, "child", func(ctx context.Context, span tracing.Span) {
    // 这里的 ctx 已挂好子 span；日志用同一个 ctx 自动关联
    logger.Info(ctx, "working")
})
<-join // 确定性汇合点：join 关闭 => 子 span 已结束、原因已记录
```

- `Go` 在新 goroutine 中挂子 span 执行 `fn`，**恰好结束一次**（`fn` 不需要也不应手动 End）。
- 返回的 channel 是确定性的 join 点，父操作据此等待所有并发子操作，杜绝：
  **孤立片段**（父结束后子丢失归属）、**错挂**（挂到别的父）、**重复计入**（同一 span 记两次）。
- `Span.End()` 幂等：并发调用任意次只产生一条记录（`sync.Once`）。

### 2.3 取消传播与终止原因

span 记录一个明确的 `end_reason`：

| reason | 触发条件 |
|---|---|
| `finished` | 正常 `End()`，且调用方 ctx 未取消 |
| `parent_cancelled` | 非根 span 的父链 context 被取消 |
| `context_cancelled` | 根 span 的调用方 context 被取消 |
| `dropped` | 预留给导出丢弃场景 |

关键语义（也是最容易出竞态的地方）：

- 取消通过 `context` 自然级联：取消父 ctx，所有在其 ctx 链上等待的子 span
  会被同步取消。
- **取消优先于显式 End**：当 `End()` 与取消在不同 goroutine 上竞争时，只要调用方
  context 已处于取消状态，`finish` 一律记录取消原因。这样即使 `Go` 的 defer-End
  比内部 watcher 先跑，结果仍是稳定的 `parent_cancelled`，不会时而是 `finished`。
- 已正常结束的子 span 不受随后取消影响（其记录保持 `finished`）。
- 从“已取消的 ctx”创建的 span “出生即取消”，立即记录对应取消原因。

> 等待取消的典型写法是 `<-ctx.Done()` 后再让 span 结束；原因由库统一归并，
> 业务代码无需自己判断是“父取消”还是“自己取消”。

---

## 3. 与追踪关联的结构化日志

`logx.Logger` 从传入的 `context.Context` 提取当前 span：

- 每条日志携带 `trace_id`、`span_id`、`parent_id`、`depth`，可直接与 span 数据 join；
- 关联字段在调用点从 ctx 快照得到，**并发 goroutine 各用各的 ctx，不共享可变状态**，
  同一并发场景下日志不会串号；
- 级别过滤在脱敏前进行；
- **脱敏先于 sink**：任何 sink（文件/内存/自定义）都不可能看到未脱敏的原始值；
- 日志本身不采样（采样只决定 span 是否记录），避免排障信息静默丢失。

时间统一为 UTC、RFC3339Nano；输出为单对象单行 JSONL。

---

## 4. 脱敏配置

通过 `logx.NewRedactor(rules, mask)` 构造；`mask` 传空串时使用默认
`***REDACTED***`。规则可叠加：

```go
r, _ := logx.NewRedactor([]logx.RedactRule{
    {Key: "password"},                       // 精确匹配键名
    {Keys: []string{"api_key", "secret"}},   // 多个精确键名
    {KeySuffix: "token"},                    // 大小写不敏感后缀（sessionToken、auth_token…）
    {KeySuffixes: []string{"password"}},     // 多个后缀
    {ValuePattern: `Bearer [A-Za-z0-9\.\-]+`}, // 对“字符串值”做正则替换
}, logx.DefaultMask)
```

语义保证：

- 命中键规则的字段整体替换为 mask；
- 值正则会递归作用于 `string`、`map[string]any`、`map[string]string`、
  `[]any`、`[]string`，嵌套结构里的敏感 token 也会被替换；
- `Apply` **深拷贝**，绝不修改调用方传入的 entry/字段；
- 测试用例对每一个已知敏感子串做 `strings.Contains == false` 断言，
  确保脱敏后不残留任何原始敏感值。

---

## 5. 采样：稳定且可解释

采样**只在根 span 创建时决策一次**，子 span 原样继承该决策
（trace 级 all-or-nothing），因此：

- 同一 `TraceID` 无论跑多少次、在多少 goroutine 中传播，决策都一致；
- 不会出现一个 trace 里部分子 span 被采、部分没被采的割裂状态。

### 5.1 确定性比率采样

`sampler.NewRatio(p)`：

1. 对 16 字节 `TraceID` 计算 FNV-1a 64；
2. 经过一次 splitmix64 finalizer（FNV 低位雪崩弱，直接取低位会导致相似 ID 严重偏斜）；
3. 取混合结果的高 32 位作为 `[0, 2^32)` 桶，与阈值 `p * 2^32` 比较（严格小于）。

`p=0` 全丢，`p=1` 全留；同一 trace id 永远同一结果。`Decision` 同时返回
`Rule / Reason / Input`，`Reason` 是形如
`fnv1a64=… mix64hi=… threshold=… (ratio=…): keep|drop` 的可审计解释。

### 5.2 名称规则

`sampler.NewNameRules(fallback, allow, deny)`：精确 span 名强制 allow/deny，
**deny 与 allow 冲突时 deny 胜出**（安全优先），其余走 fallback。

测试用 `tracing.NewCounterIDs` 生成确定性 ID，可对采样数量做黄金复现。

---

## 6. 导出：有界队列、背压、丢弃策略

### 6.1 队列策略

`export.NewQueue(capacity, policy)`，两种策略：

- `PolicyBlock`（默认）：队列满时生产者**阻塞**，直到有空位 / ctx 超时 / 队列关闭。
  这是**零丢失**策略，对生产者天然形成背压。
- `PolicyDropNewest`：队列满时**立即拒绝新到项**（保留队列里较旧的数据），
  通过计数器与事件通道上报丢弃，不阻塞调用方。

可观测性：

- `Queue.Stats()` → `Accepted`、`Dropped`；
- `Queue.Drops()` → 每个丢弃产生一条 `DropEvent{Kind, Reason}`；
- 丢弃原因：`queue_full`（满）或 `queue_closed`（关停后到达）；
- 恒等关系：`Accepted + Dropped == 总投递数`（测试断言）。

实现注意：数据 channel **从不 close**（close 会与阻塞的 send 产生数据竞争）；
关停通过独立的 `closeCh` 广播，并用 `WaitGroup` 跟踪在途生产者。

### 6.2 批处理与重试

`export.NewBatcher(queue, exporter, cfg)` 按 `MaxBatchSize` 或 `FlushInterval`
（先到先触发）成批导出；单批失败按线性退避重试 `MaxRetries` 次，
重试次数、成功批数、最终失败条数均有计数。

`FileExporter` 将每批编码为 JSONL、加锁后**一次 Write**，多导出器并发也不会串行交错。

### 6.3 优雅关停（不丢已接受数据）

`pipeline.Shutdown(ctx)`：

1. 关闭队列：此后新数据得到 `ErrQueueClosed`，不会被静默接收；
2. 等待所有在途 `Enqueue` 结束（包括“正阻塞在满队列上”的生产者）；
3. batcher 把缓冲区排空，导出所有尾批；
4. 阻塞到导出 worker 退出，或 `ctx` 超时。

**在 Close 之前已 accepted 的数据保证导出**；只有调用方传入的 `ctx` 提前超时
才可能留下未刷出数据（此时返回 `ctx.Err()`，属于调用方主动选择的放弃）。

---

## 7. 本地验证方法

前置：Go 1.25+（开发环境为 Go 1.26）。

```bash
# 1. 全部测试 + 竞态检测
go test -race ./...

# 提高抖动压力，反复跑并发/取消相关用例
go test -race -count=10 ./tracing/ ./export/ ./pipeline/

# 2. 静态检查
go vet ./...

# 3. 运行本地 demo（无需任何外部服务）
go run ./cmd/demo
# => data/pipeline.jsonl （span 与 log 混合的 JSONL）
# 并打印 queue/batcher 统计
```

demo 覆盖：一个父操作 fan-out 三个并发子（其中一支被父取消）、
孙 span 跨层级归属、密码/API key/bearer token 脱敏。

### 失败用例的信息约定

所有测试断言失败时都打印三段信息：

```
input : <构造的输入与规模>
actual: <实际观测值 / 实际原因 / 实际记录>
rule  : <判定依据与期望值>
```

便于在没有外部 UI 的情况下直接定位是输入构造、并发时序还是判定预期的问题。

---

## 8. 最小使用示例

```go
redactor, _ := logx.NewRedactor([]logx.RedactRule{{KeySuffix: "token"}}, "")
f, _ := os.Create("trace.jsonl")

p := pipeline.New(
    export.NewFileExporter(f),
    pipeline.Config{
        QueueCapacity: 256,
        Policy:        export.PolicyBlock,
        Sampler:       sampler.NewRatio(0.5),
        Redactor:      redactor,
        // 测试中可传 tracing.NewCounterIDs(...) 与固定 Clock 实现完全确定性复现
    },
)

ctx, root := p.Tracer().Start(context.Background(), "request")
join := p.Tracer().Go(ctx, "downstream", func(ctx context.Context, sp tracing.Span) {
    p.Logger().Info(ctx, "call", logx.F("sessionToken", "abc"))
})
<-join
root.End()

_ = p.Shutdown(context.Background()) // 已接受数据全部刷盘
```
