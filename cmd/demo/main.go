// Command demo 在不依赖任何外部服务的情况下演示追踪 + 结构化日志管线：
//
//  1. 正常的并发子操作归属到各自父级；
//  2. 父操作取消时子操作被同步取消，记录 parent_canceled；
//  3. 日志携带 trace/span 关联信息，敏感字段脱敏；
//  4. 有界队列背压/丢弃计数可观测；
//  5. Shutdown 后排空队列，已接受数据不丢失。
//
// 输出文件（默认 /tmp/trace-pipeline-demo.jsonl）中每行是一个导出批次（JSON 数组）。
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/xingjianluo/trace-observability-go/export"
	"github.com/xingjianluo/trace-observability-go/logging"
	"github.com/xingjianluo/trace-observability-go/obs"
	"github.com/xingjianluo/trace-observability-go/trace"
)

func main() {
	outPath := flag.String("out", "/tmp/trace-pipeline-demo.jsonl", "导出文件路径（JSON Lines）")
	rate := flag.Float64("rate", 1.0, "采样比率 [0,1]")
	capacity := flag.Int("capacity", 256, "有界队列容量")
	flag.Parse()

	f, err := os.Create(*outPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create output: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	p, err := obs.NewPipeline(obs.Config{
		Output:        f,
		QueueCapacity: *capacity,
		BatchSize:     32,
		FlushInterval: 20 * time.Millisecond,
		DropPolicy:    export.DropNewest,
		SampleRate:    *rate,
		MaskRules:     logging.DefaultRules(),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "new pipeline: %v\n", err)
		os.Exit(1)
	}
	log := p.Logger()

	// 场景一：一个父操作派生两个并发子操作，各自归属正确。
	ctx1, root1 := p.StartSpan(context.Background(), "order.checkout", false)
	grp := trace.NewGroup(ctx1)
	grp.Go("payment.charge", func(ctx context.Context, s trace.Span) {
		log.Info(ctx, "charging card", map[string]any{
			"phone":    "13800138000",
			"password": "supersecret",
			"amount":   99.5,
		})
		s.SetAttribute("payment.provider", "local-mock")
	})
	grp.Go("inventory.lock", func(ctx context.Context, s trace.Span) {
		log.Warn(ctx, "locking items", map[string]any{
			"email":   "alice@example.com",
			"item_id": "sku-1",
		})
	})
	grp.Wait()
	root1.End(trace.ReasonCompleted)

	// 场景二：父操作取消，子操作同步取消。
	ctx2, cancel := context.WithCancel(context.Background())
	ctx2span, root2 := p.StartSpan(ctx2, "long.running.job", false)
	// 必须用 StartSpan 返回的 ctx 派生并发组，子 Span 才能正确挂到 root2 下。
	grp2 := trace.NewGroup(ctx2span)
	grp2.Go("worker.polling", func(ctx context.Context, s trace.Span) {
		log.Info(ctx, "start polling", map[string]any{"token": "tok_live_abcdef"})
		<-ctx.Done() // 等待父取消
		log.Info(ctx, "stopping after cancel", nil)
	})
	time.Sleep(20 * time.Millisecond)
	cancel()
	root2.Cancel()
	grp2.Wait()
	root2.End("")

	// 立即把取消场景刷盘，避免与后续突发流量竞争有界队列。
	if err := p.Exporter().Flush(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "flush: %v\n", err)
		os.Exit(1)
	}

	// 先把前两个场景全部落盘，保证正常/取消场景的数据完整。
	if err := p.Exporter().Flush(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "flush: %v\n", err)
		os.Exit(1)
	}

	// 场景三：业务分组——一批并发操作 + 嵌套子组，收尾产出唯一组级汇总。
	bizGrp := p.StartGroup(context.Background(), "order.fulfill")
	bizGrp.Go("inventory.lock", func(ctx context.Context, s trace.Span) {
		log.Info(ctx, "locking stock", map[string]any{"phone": "13900001111"})
	})
	ship := bizGrp.SubGroup("shipment")
	ship.Go("carrier.book", func(ctx context.Context, s trace.Span) {
		log.Info(ctx, "booking carrier", map[string]any{"token": "tok_ship_secret"})
	})
	ship.Go("label.print", func(ctx context.Context, s trace.Span) {
		s.SetAttribute("api_key", "AKIA-DEMO-RAW")
	})
	shipSum := ship.Wait()
	rootSum := bizGrp.Wait()
	fmt.Printf("group outer: total=%d ok=%d observed=%d sampled_out=%d queue_dropped=%d\n",
		rootSum.Coverage.Total, rootSum.SuccessCount,
		rootSum.Coverage.Observed, rootSum.Coverage.SampledOut, rootSum.Coverage.QueueDropped)
	fmt.Printf("group inner: total=%d parent_group=%s\n",
		shipSum.Coverage.Total, shipSum.ParentGroup)

	// 场景四：制造队列积压（同步快速投递超过容量），展示丢弃计数可观测。
	ctx3, root3 := p.StartSpan(context.Background(), "backpressure.writer", false)
	for i := 0; i < *capacity+50; i++ {
		log.Debug(ctx3, "burst event", map[string]any{"i": i, "api_key": "AKIA1234567890"})
	}
	root3.End("")

	// 收尾：排空队列，已接受数据全部落盘。
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutCancel()
	if err := p.Shutdown(shutCtx); err != nil {
		fmt.Fprintf(os.Stderr, "shutdown: %v\n", err)
		os.Exit(1)
	}

	st := p.Exporter().Snapshot()
	fmt.Printf("pipeline shutdown complete\n")
	fmt.Printf("enqueued=%d exported=%d batches=%d dropped_new=%d dropped_old=%d\n",
		st.Enqueued, st.Exported, st.Batches, st.DroppedNew, st.DroppedOld)
	fmt.Printf("output written to %s\n", *outPath)
	summarize(*outPath)
}

func summarize(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read output: %v\n", err)
		return
	}
	var spans, logs, groups int
	var reasons = map[string]int{}
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var batch []map[string]json.RawMessage
		if err := json.Unmarshal(line, &batch); err != nil {
			continue
		}
		for _, rec := range batch {
			var kind string
			if k, ok := rec["kind"]; ok {
				_ = json.Unmarshal(k, &kind)
			}
			switch kind {
			case "span":
				spans++
				var v export.SpanView
				raw, _ := json.Marshal(rec)
				_ = json.Unmarshal(raw, &v) // 记录在 data 内嵌时这里取外层；仅演示
				_ = v
				if d, ok := rec["data"]; ok {
					_ = json.Unmarshal(d, &v)
					reasons[v.EndReason]++
				}
			case "group":
				groups++
			case "log":
				logs++
			}
		}
	}
	fmt.Printf("summary: spans=%d groups=%d logs=%d end_reasons=%v\n", spans, groups, logs, reasons)
}
