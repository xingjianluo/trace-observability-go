package export

import (
	"context"
	"encoding/json"
	"io"
	"sync"
)

// FileExporter writes JSON-encoded envelopes, one per line, to an
// io.Writer (os.File in normal use, bytes.Buffer in tests). It is
// fully local and deterministic: a batch becomes a single Write under
// a lock, so concurrent exporters cannot interleave lines.
type FileExporter struct {
	mu sync.Mutex
	w  io.Writer
}

// NewFileExporter builds a line-delimited JSON exporter.
func NewFileExporter(w io.Writer) *FileExporter {
	return &FileExporter{w: w}
}

func (f *FileExporter) Export(ctx context.Context, batch []Envelope) error {
	if len(batch) == 0 {
		return nil
	}
	buf := make([]byte, 0, len(batch)*128)
	for _, e := range batch {
		line, err := json.Marshal(e)
		if err != nil {
			return err
		}
		buf = append(buf, line...)
		buf = append(buf, '\n')
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	_, err := f.w.Write(buf)
	return err
}
