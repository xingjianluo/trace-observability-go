// Command demo runs a fully local scenario: a parent operation fans
// out concurrent children, one branch is cancelled, logs are
// correlated and redacted, and everything is exported as JSONL under
// ./data. It requires no external services.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/xingjianluo/trace-observability-go/export"
	"github.com/xingjianluo/trace-observability-go/logx"
	"github.com/xingjianluo/trace-observability-go/pipeline"
	"github.com/xingjianluo/trace-observability-go/sampler"
	"github.com/xingjianluo/trace-observability-go/tracing"
)

func main() {
	dir := "data"
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fatal(err)
	}
	f, err := os.Create(filepath.Join(dir, "pipeline.jsonl"))
	if err != nil {
		fatal(err)
	}
	defer f.Close()

	redactor, err := logx.NewRedactor([]logx.RedactRule{
		{Keys: []string{"password", "api_key"}},
		{KeySuffix: "token"},
		// Mask bearer tokens that appear inside free-form strings.
		{ValuePattern: `Bearer [A-Za-z0-9\-\._~+\/]+`},
	}, logx.DefaultMask)
	if err != nil {
		fatal(err)
	}

	p := pipeline.New(export.NewFileExporter(f), pipeline.Config{
		QueueCapacity: 8,
		Policy:        export.PolicyBlock,
		Batch: export.BatcherConfig{
			MaxBatchSize:  4,
			FlushInterval: 50 * time.Millisecond,
		},
		Log:      logx.Config{MinLevel: logx.LevelDebug},
		Sampler:  sampler.NewRatio(1.0),
		Redactor: redactor,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runScenario(ctx, p)

	shutdownCtx, sd := context.WithTimeout(context.Background(), 2*time.Second)
	defer sd()
	if err := p.Shutdown(shutdownCtx); err != nil {
		fatal(err)
	}

	fmt.Println("queue stats:   ", p.Queue().Stats())
	fmt.Println("batcher stats: ", p.Batcher().Stats())
	fmt.Println("export written:", filepath.Join(dir, "pipeline.jsonl"))
}

func runScenario(ctx context.Context, p *pipeline.Pipeline) {
	tr, lg := p.Tracer(), p.Logger()

	rootCtx, root := tr.Start(ctx, "parent-op")
	lg.Info(rootCtx, "parent started", logx.F("user", "alice"))

	// Three concurrent children. Child B's branch gets cancelled by
	// the parent to demonstrate propagated termination reasons.
	branchCtx, branchCancel := context.WithCancel(rootCtx)
	doneA := tr.Go(rootCtx, "child-a", func(ctx context.Context, sp tracing.Span) {
		lg.Info(ctx, "child A working",
			logx.F("password", "hunter2"),
			logx.F("auth", "Bearer abc.def-123/ghi"))
		sp.SetAttribute("ok", true)
	})
	doneB := tr.Go(branchCtx, "child-b", func(ctx context.Context, sp tracing.Span) {
		<-ctx.Done()
		lg.Warn(ctx, "child B terminating", logx.F("api_key", "sk-1234"))
		sp.RecordError(errors.New("aborted"))
	})
	doneC := tr.Go(rootCtx, "child-c", func(ctx context.Context, sp tracing.Span) {
		// A grandchild proves depth/parent attribution across layers.
		gDone := tr.Go(ctx, "grandchild-c1", func(ctx context.Context, sp tracing.Span) {
			lg.Debug(ctx, "grandchild done", logx.F("sessionToken", "t-9"))
		})
		<-gDone
	})

	branchCancel() // cancels child B only; A and C finish normally
	<-doneA
	<-doneB
	<-doneC

	root.End()
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "demo error:", err)
	os.Exit(1)
}
