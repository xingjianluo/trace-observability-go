package export

import "sync/atomic"

// Metrics is the observable counters exposed by the export layer.
type Metrics struct {
	accepted atomic.Int64
	dropped  atomic.Int64
	exported atomic.Int64
	failed   atomic.Int64
	batches  atomic.Int64
}

// Snapshot is a point-in-time copy of the counters.
type Snapshot struct {
	Accepted int64
	Dropped  int64
	Exported int64
	Failed   int64
	Batches  int64
}

func (m *Metrics) snap() Snapshot { return Snapshot{} }

// Metrics returns the live metrics (shared with the owner).
func (q *Queue) Metrics() *Metrics { return nil }
