// Package queue is the bounded, in-memory queue where admitted requests wait
// for a slot. It only decides which entries to drop; the caller decides when
// to check, by calling Sweep when a slot opens and on a periodic tick.
package queue

import (
	"sync"
	"time"
)

// Reason is why a request left the queue without being served. The values
// are the ones returned to the client in X-Wpcp-Reason.
type Reason string

const (
	ReasonQueueFull Reason = "queue_full"
	ReasonQueueWait Reason = "queue_wait"
	ReasonDeadline  Reason = "deadline"
	ReasonNoWorker  Reason = "no_worker"
)

// Config sets the bound and the drop rules.
type Config struct {
	// Capacity is the number of entries Push accepts.
	Capacity int
	// MaxWait is how long an entry may wait. Zero disables the rule.
	MaxWait time.Duration
	// SendFloor is the smallest remaining budget worth sending. An entry
	// with less left is dropped as if its deadline had passed.
	SendFloor time.Duration
}

// Drop is an entry removed by a drop rule.
type Drop[T any] struct {
	Value  T
	Reason Reason
}

type entry[T any] struct {
	v        T
	enqueued time.Time
	deadline time.Time
}

// Queue is a FIFO of waiting requests, safe for concurrent use.
type Queue[T any] struct {
	cfg Config

	mu      sync.Mutex
	entries []entry[T]
}

// New returns an empty queue.
func New[T any](cfg Config) *Queue[T] {
	return &Queue[T]{cfg: cfg}
}

// Push adds v at the tail. It returns false, meaning queue_full, when the
// queue already holds Capacity entries.
func (q *Queue[T]) Push(v T, enqueued, deadline time.Time) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.entries) >= q.cfg.Capacity {
		return false
	}
	q.entries = append(q.entries, entry[T]{v, enqueued, deadline})
	return true
}

// PushFront adds v at the head. It is used for requests that were already
// admitted: one re-entering after failover, or one whose reservation failed
// twice. It ignores Capacity, since such requests are bounded by the
// admission limit, and rejecting them would drop work the proxy accepted.
func (q *Queue[T]) PushFront(v T, enqueued, deadline time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.entries = append([]entry[T]{{v, enqueued, deadline}}, q.entries...)
}

// Pop removes and returns the head. Call Sweep first so that the head is not
// an entry a drop rule would remove.
func (q *Queue[T]) Pop() (v T, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.entries) == 0 {
		return v, false
	}
	v = q.entries[0].v
	var zero entry[T]
	q.entries[0] = zero
	q.entries = q.entries[1:]
	return v, true
}

// Sweep removes every entry whose deadline leaves less than SendFloor at now,
// or that has waited longer than MaxWait, and returns them in queue order.
func (q *Queue[T]) Sweep(now time.Time) []Drop[T] {
	q.mu.Lock()
	defer q.mu.Unlock()
	var drops []Drop[T]
	kept := q.entries[:0]
	for _, e := range q.entries {
		switch {
		case e.deadline.Sub(now) < q.cfg.SendFloor:
			drops = append(drops, Drop[T]{e.v, ReasonDeadline})
		case q.cfg.MaxWait > 0 && now.Sub(e.enqueued) > q.cfg.MaxWait:
			drops = append(drops, Drop[T]{e.v, ReasonQueueWait})
		default:
			kept = append(kept, e)
		}
	}
	clear(q.entries[len(kept):])
	q.entries = kept
	return drops
}

// DropAll empties the queue and returns every entry in queue order. It is
// used when no worker is live, so that a dead pool fails fast with no_worker.
func (q *Queue[T]) DropAll() []T {
	q.mu.Lock()
	defer q.mu.Unlock()
	vs := make([]T, len(q.entries))
	for i, e := range q.entries {
		vs[i] = e.v
	}
	q.entries = nil
	return vs
}

// Len returns the number of waiting entries.
func (q *Queue[T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.entries)
}
