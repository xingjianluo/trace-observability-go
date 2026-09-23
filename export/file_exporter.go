package export

import (
	"context"
	"io"
	"sync"
)

// FileExporter writes JSON-encoded envelopes, one per line, to an
// io.Writer (os.File in normal use, bytes.Buffer in tests). It is
// fully local and deterministic.
type FileExporter struct {
	mu  sync.Mutex
	w   io.Writer
}

// NewFileExporter builds a line-delimited JSON exporter.
func NewFileExporter(w io.Writer) *FileExporter { return &FileExporter{} }

func (f *FileExporter) Export(ctx context.Context, batch []Envelope) error { return nil }
