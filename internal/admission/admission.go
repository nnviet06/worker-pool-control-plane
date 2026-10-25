// Package admission decides, for each request, whether it runs now, waits in
// the queue or is rejected, and hands it a reserved worker slot when it runs.
//
// One mutex serializes admission, placement and dispatch from the queue. It
// is held only for in-memory work, never across a call to a worker.
package admission

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nnviet06/worker-pool-control-plane/internal/limiter"
	"github.com/nnviet06/worker-pool-control-plane/internal/picker"
	"github.com/nnviet06/worker-pool-control-plane/internal/pool"
	"github.com/nnviet06/worker-pool-control-plane/internal/queue"
)

// Limiter reports the current admission limit.
type Limiter interface {
	Limit() int
}

// RejectError is returned when a request is not served. Reason is the value
// sent to the client in X-Wpcp-Reason.
type RejectError struct {
	Reason queue.Reason
}

func (e *RejectError) Error() string { return "admission: rejected: " + string(e.Reason) }

// Stats is a snapshot of the controller for /debug/state.
type Stats struct {
	Limit     int `json:"limit"`
	Effective int `json:"effective_limit"`
	InFlight  int `json:"in_flight"`
	Queued    int `json:"queued"`
}

// Controller admits requests onto a pool.
type Controller struct {
	pool    *pool.Pool
	picker  *picker.Picker
	limiter Limiter
	now     func() time.Time

	mu       sync.Mutex
	queue    *queue.Queue[*waiter]
	inFlight int
}

// New returns a controller. now is the clock; pass time.Now outside tests.
func New(p *pool.Pool, pk *picker.Picker, l Limiter, qc queue.Config, now func() time.Time) *Controller {
	return &Controller{pool: p, picker: pk, limiter: l, now: now, queue: queue.New[*waiter](qc)}
}

// Acquire returns a worker with a slot reserved for a request of class c,
// waiting in the queue if the pool is at its limit. The caller must call
// Release when the request completes. It returns a *RejectError when the
// request is rejected or dropped, or ctx's error if ctx ends first.
func (c *Controller) Acquire(ctx context.Context, class pool.Class, deadline time.Time) (*pool.Worker, error) {
	return c.acquire(ctx, class, deadline, false, nil)
}

// AcquireFront is Acquire for a request that is being failed over from
// failed. It was admitted once already, so if it has to wait it waits at the
// head of the queue and is never rejected as queue_full. It is never placed
// on failed.
func (c *Controller) AcquireFront(ctx context.Context, class pool.Class, deadline time.Time, failed *pool.Worker) (*pool.Worker, error) {
	return c.acquire(ctx, class, deadline, true, failed)
}

func (c *Controller) acquire(ctx context.Context, class pool.Class, deadline time.Time, front bool, avoid *pool.Worker) (*pool.Worker, error) {
	now := c.now()
	c.mu.Lock()
	// After dispatch the queue is either empty or the pool is at its limit,
	// so an arrival never passes a request that could already run.
	c.dispatchLocked()
	live, slots := c.pool.Live()
	if live == 0 {
		c.mu.Unlock()
		return nil, &RejectError{queue.ReasonNoWorker}
	}
	admitted := c.queue.Len() == 0 && c.inFlight < limiter.Effective(c.limiter.Limit(), live, slots)
	if admitted {
		w, err := c.picker.PickAvoiding(class, avoid)
		if err == nil {
			c.inFlight++
			c.mu.Unlock()
			return w, nil
		}
		if errors.Is(err, picker.ErrNoWorker) {
			c.mu.Unlock()
			return nil, &RejectError{queue.ReasonNoWorker}
		}
	}
	wt := &waiter{class: class, avoid: avoid, enqueued: now, deadline: deadline, ready: make(chan result, 1)}
	if admitted || front {
		// Either the state changed under placement after admission, or the
		// request is a failover; both wait at the head.
		c.queue.PushFront(wt, now, deadline)
	} else if !c.queue.Push(wt, now, deadline) {
		c.mu.Unlock()
		return nil, &RejectError{queue.ReasonQueueFull}
	}
	c.mu.Unlock()

	select {
	case r := <-wt.ready:
		return r.w, r.err
	case <-ctx.Done():
		if !wt.cancel() {
			// Dispatch decided first; give back the slot if it got one.
			if r := <-wt.ready; r.w != nil {
				c.Release(r.w, class)
			}
		}
		return nil, ctx.Err()
	}
}

// Release frees the slot of a completed request and lets the next queued
// request run.
func (c *Controller) Release(w *pool.Worker, class pool.Class) {
	w.Release(class)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inFlight--
	c.dispatchLocked()
}

// Run applies the queue's drop rules and dispatches every interval until ctx
// ends. It also picks up workers that came back to life.
func (c *Controller) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.mu.Lock()
			c.dispatchLocked()
			c.mu.Unlock()
		}
	}
}

// InFlight returns the number of requests holding a worker slot.
func (c *Controller) InFlight() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inFlight
}

// Stats returns a snapshot of the controller.
func (c *Controller) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	live, slots := c.pool.Live()
	limit := c.limiter.Limit()
	return Stats{
		Limit:     limit,
		Effective: limiter.Effective(limit, live, slots),
		InFlight:  c.inFlight,
		Queued:    c.queue.Len(),
	}
}

// dispatchLocked drops what the queue rules say to drop, then hands free
// slots to queued requests in order. c.mu must be held.
func (c *Controller) dispatchLocked() {
	live, slots := c.pool.Live()
	if live == 0 {
		for _, wt := range c.queue.DropAll() {
			wt.finish(result{err: &RejectError{queue.ReasonNoWorker}})
		}
		return
	}
	for _, d := range c.queue.Sweep(c.now()) {
		d.Value.finish(result{err: &RejectError{d.Reason}})
	}
	for c.inFlight < limiter.Effective(c.limiter.Limit(), live, slots) {
		wt, ok := c.queue.Pop()
		if !ok {
			return
		}
		if wt.cancelled() {
			continue
		}
		w, err := c.picker.PickAvoiding(wt.class, wt.avoid)
		if err != nil {
			// A worker changed state since Live, or the only free slot is on
			// the worker this request failed on; try again on the next tick.
			c.queue.PushFront(wt, wt.enqueued, wt.deadline)
			return
		}
		if wt.finish(result{w: w}) {
			c.inFlight++
		} else {
			w.Release(wt.class)
		}
	}
}

type result struct {
	w   *pool.Worker
	err error
}

const (
	waiting int32 = iota
	finished
	cancelled
)

// waiter is a queued request. Exactly one of finish and cancel wins.
type waiter struct {
	class    pool.Class
	avoid    *pool.Worker // set on a failover: the worker it failed on
	enqueued time.Time
	deadline time.Time
	ready    chan result // buffered, so finish never blocks under c.mu
	state    atomic.Int32
}

func (w *waiter) finish(r result) bool {
	if !w.state.CompareAndSwap(waiting, finished) {
		return false
	}
	w.ready <- r
	return true
}

func (w *waiter) cancel() bool { return w.state.CompareAndSwap(waiting, cancelled) }

func (w *waiter) cancelled() bool { return w.state.Load() == cancelled }
