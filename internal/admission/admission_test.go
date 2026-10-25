package admission

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nnviet06/worker-pool-control-plane/internal/limiter"
	"github.com/nnviet06/worker-pool-control-plane/internal/picker"
	"github.com/nnviet06/worker-pool-control-plane/internal/pool"
	"github.com/nnviet06/worker-pool-control-plane/internal/queue"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// clock is a settable time source shared by a test and the controller.
type clock struct{ ns atomic.Int64 }

func newClock() *clock {
	c := &clock{}
	c.ns.Store(t0.UnixNano())
	return c
}

func (c *clock) now() time.Time          { return time.Unix(0, c.ns.Load()) }
func (c *clock) advance(d time.Duration) { c.ns.Add(int64(d)) }

func newController(clk *clock, queueCap int, workers ...*pool.Worker) *Controller {
	p := pool.New(workers...)
	_, slots := p.Live()
	return New(p, picker.New(p, picker.LeastInFlight{}), limiter.NewStatic(slots),
		queue.Config{Capacity: queueCap}, clk.now)
}

type outcome struct {
	w   *pool.Worker
	err error
}

// acquireAsync starts Acquire in a goroutine and waits until it is queued.
func acquireAsync(t *testing.T, ctx context.Context, c *Controller, deadline time.Time) <-chan outcome {
	t.Helper()
	before := c.Stats().Queued
	ch := make(chan outcome, 1)
	go func() {
		w, err := c.Acquire(ctx, "x", deadline)
		ch <- outcome{w, err}
	}()
	waitFor(t, func() bool { return c.Stats().Queued > before })
	return ch
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(2 * time.Second); time.Now().Before(end); time.Sleep(time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatal("condition not met within 2s")
}

func reason(err error) queue.Reason {
	var re *RejectError
	if errors.As(err, &re) {
		return re.Reason
	}
	return ""
}

func TestAcquireBelowLimitRunsAtOnce(t *testing.T) {
	c := newController(newClock(), 1, pool.NewWorker("a", 2))
	for range 2 {
		if _, err := c.Acquire(context.Background(), "x", t0.Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	if s := c.Stats(); s.InFlight != 2 || s.Queued != 0 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestQueuedRequestsRunInOrderOnRelease(t *testing.T) {
	c := newController(newClock(), 2, pool.NewWorker("a", 1))
	first, _ := c.Acquire(context.Background(), "x", t0.Add(time.Minute))
	b := acquireAsync(t, context.Background(), c, t0.Add(time.Minute))
	cc := acquireAsync(t, context.Background(), c, t0.Add(time.Minute))

	c.Release(first, "x")
	got := <-b
	if got.err != nil {
		t.Fatal(got.err)
	}
	select {
	case <-cc:
		t.Fatal("second queued request ran before the first released")
	default:
	}
	c.Release(got.w, "x")
	if got := <-cc; got.err != nil {
		t.Fatal(got.err)
	}
}

func TestAcquireFrontWaitsAheadAndIgnoresCapacity(t *testing.T) {
	c := newController(newClock(), 1, pool.NewWorker("a", 1))
	held, _ := c.Acquire(context.Background(), "x", t0.Add(time.Minute))
	back := acquireAsync(t, context.Background(), c, t0.Add(time.Minute))

	front := make(chan outcome, 1)
	go func() {
		w, err := c.AcquireFront(context.Background(), "x", t0.Add(time.Minute), nil)
		front <- outcome{w, err}
	}()
	waitFor(t, func() bool { return c.Stats().Queued == 2 })

	c.Release(held, "x")
	got := <-front
	if got.err != nil {
		t.Fatal(got.err)
	}
	select {
	case <-back:
		t.Fatal("request queued earlier ran before the failover")
	default:
	}
	c.Release(got.w, "x")
	<-back
}

func TestAcquireFrontWaitsForAnotherWorker(t *testing.T) {
	a, b := pool.NewWorker("a", 1), pool.NewWorker("b", 1)
	c := newController(newClock(), 1, a, b)
	held, _ := c.Acquire(context.Background(), "x", t0.Add(time.Minute))
	failed := a
	if held == a {
		failed = b
	}

	got := make(chan outcome, 1)
	go func() {
		w, err := c.AcquireFront(context.Background(), "x", t0.Add(time.Minute), failed)
		got <- outcome{w, err}
	}()
	waitFor(t, func() bool { return c.Stats().Queued == 1 })

	c.Release(held, "x")
	if r := <-got; r.err != nil || r.w != held {
		t.Fatalf("got worker %v, err %v; want the other worker %s", r.w, r.err, held.ID())
	}
}

func TestRejectWhenQueueFull(t *testing.T) {
	c := newController(newClock(), 1, pool.NewWorker("a", 1))
	c.Acquire(context.Background(), "x", t0.Add(time.Minute))
	acquireAsync(t, context.Background(), c, t0.Add(time.Minute))
	_, err := c.Acquire(context.Background(), "x", t0.Add(time.Minute))
	if reason(err) != queue.ReasonQueueFull {
		t.Fatalf("err = %v, want queue_full", err)
	}
}

func TestRejectWhenNoWorkerLive(t *testing.T) {
	w := pool.NewWorker("a", 1)
	w.SetLive(false)
	_, err := newController(newClock(), 1, w).Acquire(context.Background(), "x", t0.Add(time.Minute))
	if reason(err) != queue.ReasonNoWorker {
		t.Fatalf("err = %v, want no_worker", err)
	}
}

func TestDeadPoolDropsQueue(t *testing.T) {
	a := pool.NewWorker("a", 1)
	c := newController(newClock(), 1, a)
	held, _ := c.Acquire(context.Background(), "x", t0.Add(time.Minute))
	queued := acquireAsync(t, context.Background(), c, t0.Add(time.Minute))

	a.SetLive(false)
	c.Release(held, "x")
	if got := <-queued; reason(got.err) != queue.ReasonNoWorker {
		t.Fatalf("err = %v, want no_worker", got.err)
	}
}

func TestExpiredRequestIsDropped(t *testing.T) {
	clk := newClock()
	c := newController(clk, 1, pool.NewWorker("a", 1))
	held, _ := c.Acquire(context.Background(), "x", t0.Add(time.Minute))
	queued := acquireAsync(t, context.Background(), c, t0.Add(time.Second))

	clk.advance(2 * time.Second)
	c.Release(held, "x")
	if got := <-queued; reason(got.err) != queue.ReasonDeadline {
		t.Fatalf("err = %v, want deadline", got.err)
	}
	if s := c.Stats(); s.InFlight != 0 {
		t.Fatalf("in flight = %d after drop", s.InFlight)
	}
}

func TestCancelledWaiterLeavesNoSlotBehind(t *testing.T) {
	c := newController(newClock(), 1, pool.NewWorker("a", 1))
	held, _ := c.Acquire(context.Background(), "x", t0.Add(time.Minute))
	ctx, cancel := context.WithCancel(context.Background())
	queued := acquireAsync(t, ctx, c, t0.Add(time.Minute))

	cancel()
	if got := <-queued; !errors.Is(got.err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", got.err)
	}
	c.Release(held, "x")
	if s := c.Stats(); s.InFlight != 0 || s.Queued != 0 {
		t.Fatalf("stats = %+v after cancel and release", s)
	}
}

func TestConcurrentAcquireNeverExceedsLimit(t *testing.T) {
	c := newController(newClock(), 1000, pool.NewWorker("a", 2), pool.NewWorker("b", 2))
	var running, peak atomic.Int32
	var wg sync.WaitGroup
	for range 200 {
		wg.Go(func() {
			w, err := c.Acquire(context.Background(), "x", t0.Add(time.Minute))
			if err != nil {
				t.Error(err)
				return
			}
			n := running.Add(1)
			for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
			}
			running.Add(-1)
			c.Release(w, "x")
		})
	}
	wg.Wait()
	if peak.Load() > 4 {
		t.Fatalf("peak concurrency %d above limit 4", peak.Load())
	}
	if s := c.Stats(); s.InFlight != 0 || s.Queued != 0 {
		t.Fatalf("stats = %+v after all releases", s)
	}
}
