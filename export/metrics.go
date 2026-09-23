package export

// QueueStats is a point-in-time view of queue-side counters.
type QueueStats struct {
	Accepted int64
	Dropped  int64
}

// Stats returns accepted/dropped counters for the queue.
func (q *Queue) Stats() QueueStats {
	return QueueStats{
		Accepted: q.AcceptedTotal(),
		Dropped:  q.DroppedTotal(),
	}
}
