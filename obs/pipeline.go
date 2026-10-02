package obs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/xingjianluo/trace-observability-go/export"
	"github.com/xingjianluo/trace-observability-go/logging"
	"github.com/xingjianluo/trace-observability-go/sampling"
	"github.com/xingjianluo/trace-observability-go/trace"
)

// Config 描述整条观测管线的配置。
type Config struct {
	Output        io.Writer         // 批次输出目标（JSON Lines）
	QueueCapacity int               // 有界队列容量（默认 1024）
	BatchSize     int               // 批次大小（默认 64）
	FlushInterval time.Duration     // 定期刷新间隔（默认 200ms）
	DropPolicy    export.DropPolicy // 队列满时的背压/丢弃策略（默认 DropNewest）
	SampleRate    float64           // 采样比率 [0,1]；0=全不采样，<0/NaN=默认全采样
	SampleSalt    string            // 采样盐（可选，用于改变稳定采样集合）
	MaskRules     []logging.Rule    // 脱敏规则（nil 表示不脱敏；可用 logging.DefaultRules）
	IDSource      trace.IDSource    // ID 源（nil 使用进程默认）
}

// FullSampling 是 SampleRate 的便捷常量：全采样。
const FullSampling = 1.0

// Pipeline 把追踪、日志、采样、导出装配在一起。
type Pipeline struct {
	exporter *export.Exporter
	logger   *logging.Logger
	sampler  *sampling.ThresholdSampler
	masker   *logging.Masker
}

// NewPipeline 根据配置装配管线并启动导出协程。
func NewPipeline(cfg Config) (*Pipeline, error) {
	if cfg.Output == nil {
		return nil, fmt.Errorf("obs: Output is required")
	}
	// SampleRate 语义：显式取值 [0,1]；
	//   0      = 全不采样（合法配置）；
	//   1      = 全采样；
	//   <0/NaN = 未设置，按默认全采样处理。
	// 零值结构体若想采样，请显式传 obs.FullSampling(=1.0)。
	rate := cfg.SampleRate
	if math.IsNaN(rate) || rate < 0 {
		rate = 1.0
	}
	s := sampling.NewThresholdSamplerWithSalt(rate, cfg.SampleSalt)

	var exOpts []export.Option
	if cfg.QueueCapacity > 0 {
		exOpts = append(exOpts, export.WithCapacity(cfg.QueueCapacity))
	}
	if cfg.BatchSize > 0 {
		exOpts = append(exOpts, export.WithBatchSize(cfg.BatchSize))
	}
	if cfg.FlushInterval != 0 {
		exOpts = append(exOpts, export.WithFlushInterval(cfg.FlushInterval))
	}
	exOpts = append(exOpts, export.WithDropPolicy(cfg.DropPolicy))
	ex := export.New(cfg.Output, exOpts...)

	if cfg.IDSource != nil {
		trace.SetIDSource(cfg.IDSource)
	}
	var masker *logging.Masker
	if cfg.MaskRules != nil {
		masker = logging.NewMasker(cfg.MaskRules)
	}
	p := &Pipeline{exporter: ex, sampler: s, masker: masker}
	p.logger = logging.NewLogger(p, masker)
	return p, nil
}

// StartSpan 开启一个经过采样决策并绑定导出回调的 Span。
//   - 根 Span（无父）：以 TraceID 为采样键做确定性决策；
//   - 子 Span：直接继承父级的采样状态，整条追踪链路结论一致；
//   - forced=true 强制采样（如错误路径），决策可在 Span 属性
//     sampling.decision_reason 中解释。
func (p *Pipeline) StartSpan(ctx context.Context, name string, forced bool, opts ...trace.SpanOption) (context.Context, trace.Span) {
	var state trace.SamplingState
	var reason string
	parent := trace.SpanFromContext(ctx)
	if parent != nil {
		state = parent.SamplingState()
		reason = fmt.Sprintf("inherit parent span=%s state=%d", parent.SpanID(), state)
	} else {
		// 先用临时 TraceID 不合适：采样键必须与最终 TraceID 一致。
		// 因此由 IDSource 先生成 TraceID，再决策，再创建 Span。
		ids := trace.DefaultIDSource()
		tid := ids.NewTraceID()
		d := p.sampler.Decide(ctx, samplingTraceKey(tid), forced)
		if d.Accept {
			state = trace.SamplingAccept
		} else {
			state = trace.SamplingDrop
		}
		reason = d.Reason
		opts = append(opts, trace.WithTraceID(tid))
	}
	opts = append(opts, trace.WithFinalSampling(state), trace.WithOnEnd(p.onSpanEnd))
	c, s := trace.StartSpan(ctx, name, opts...)
	s.SetAttribute("sampling.decision_reason", reason)
	return c, s
}

// onSpanEnd 是 Span 结束回调：仅把被采样（Accept）的 Span 投递到有界队列，
// 并把下游投递结论返回给 trace 层（分组观察者据此统计覆盖情况）。
func (p *Pipeline) onSpanEnd(rec trace.SpanRecord) trace.DeliveryResult {
	if rec.SamplingState != trace.SamplingAccept {
		return trace.DeliveryResult{
			Outcome: trace.DeliveryOutcomeNotSampled,
			Reason:  "trace sampled out: member detail not exported",
		}
	}
	// Span 属性在入队前同样要脱敏：SetAttribute 可能写入敏感内容
	// （如组属性透传、业务标注），保证导出中不残留原始敏感值。
	attrs := rec.Attributes
	if p.masker != nil && len(attrs) > 0 {
		attrs = p.masker.MaskStringMap(attrs)
	}
	v := export.SpanView{
		TraceID:       rec.TraceID,
		SpanID:        rec.SpanID,
		ParentSpanID:  rec.ParentSpanID,
		GroupSpanID:   rec.Attributes["group_span_id"],
		Name:          rec.Name,
		StartTime:     rec.StartTime,
		EndTime:       rec.EndTime,
		DurationNanos: rec.EndTime.Sub(rec.StartTime).Nanoseconds(),
		Attributes:    attrs,
		Status:        rec.Status,
		EndReason:     rec.EndReason,
		Depth:         rec.Depth,
		RemoteParent:  rec.RemoteParent,
		SamplingState: samplingStateName(rec.SamplingState),
	}
	data, err := json.Marshal(v)
	if err != nil {
		return trace.DeliveryResult{Outcome: trace.DeliveryOutcomeUnknown, Reason: err.Error()}
	}
	res := p.exporter.EnqueueR(context.Background(), export.Record{
		Kind:     export.KindSpan,
		Data:     data,
		TraceID:  rec.TraceID,
		SpanID:   rec.SpanID,
		Accepted: true,
	})
	switch res.Outcome {
	case export.OutcomeAccepted:
		return trace.DeliveryResult{Outcome: trace.DeliveryOutcomeAccepted, Reason: res.Reason}
	case export.OutcomeDroppedNew:
		return trace.DeliveryResult{Outcome: trace.DeliveryOutcomeDroppedQueue, Reason: res.Reason}
	case export.OutcomeRejected:
		return trace.DeliveryResult{Outcome: trace.DeliveryOutcomeClosed, Reason: res.Reason}
	default:
		return trace.DeliveryResult{Outcome: trace.DeliveryOutcomeUnknown, Reason: res.Reason}
	}
}

// OnLog 实现 logging.Sink：把脱敏后的日志条目投递到有界队列。
func (p *Pipeline) OnLog(e logging.Entry) {
	data, err := json.Marshal(e)
	if err != nil {
		return
	}
	_ = p.exporter.Enqueue(context.Background(), export.Record{
		Kind:     export.KindLog,
		Data:     data,
		TraceID:  e.TraceID,
		SpanID:   e.SpanID,
		Accepted: true,
	})
}

// Logger 返回关联到本管线的结构化日志器。
func (p *Pipeline) Logger() *logging.Logger { return p.logger }

// Sampler 返回底层采样器（便于检查/解释决策）。
func (p *Pipeline) Sampler() *sampling.ThresholdSampler { return p.sampler }

// Exporter 返回底层导出器。
func (p *Pipeline) Exporter() *export.Exporter { return p.exporter }

// Shutdown 收尾：排空队列，保证已接受数据全部导出。
func (p *Pipeline) Shutdown(ctx context.Context) error {
	return p.exporter.Shutdown(ctx)
}

// samplingTraceKey 给采样键加域前缀，避免连续/相似的 TraceID
// 在 FNV-1a 下哈希聚集，保证采样集合近似均匀且仍完全确定。
func samplingTraceKey(traceID string) string { return "trace:" + traceID }

func samplingStateName(s trace.SamplingState) string {
	switch s {
	case trace.SamplingAccept:
		return "accept"
	case trace.SamplingDrop:
		return "drop"
	default:
		return "defer"
	}
}
