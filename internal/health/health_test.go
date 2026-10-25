package health

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nnviet06/worker-pool-control-plane/internal/pool"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

type clock struct{ ns atomic.Int64 }

func newClock() *clock {
	c := &clock{}
	c.ns.Store(t0.UnixNano())
	return c
}

func (c *clock) now() time.Time          { return time.Unix(0, c.ns.Load()) }
func (c *clock) advance(d time.Duration) { c.ns.Add(int64(d)) }

// prober fails while down is set.
type prober struct{ down atomic.Bool }

func (p *prober) Probe(context.Context) error {
	if p.down.Load() {
		return errors.New("down")
	}
	return nil
}

var cfg = Config{ProbeTimeout: time.Second, EjectAfter: 3, EjectFor: 5 * time.Second, EjectMax: 12 * time.Second}

func setup() (*Checker, *pool.Worker, *prober, *clock) {
	clk := newClock()
	c := NewChecker(cfg, clk.now)
	w, p := pool.NewWorker("a", 2), &prober{}
	c.Add(w, p)
	return c, w, p, clk
}

func live(w *pool.Worker) bool { return w.Snapshot().Live }

func TestEjectsAfterConsecutiveFailures(t *testing.T) {
	c, w, _, _ := setup()
	c.ReportFailure("a")
	c.ReportFailure("a")
	c.ReportSuccess("a") // breaks the run
	c.ReportFailure("a")
	c.ReportFailure("a")
	if !live(w) {
		t.Fatal("ejected without three consecutive failures")
	}
	c.ReportFailure("a")
	if live(w) {
		t.Fatal("not ejected after three consecutive failures")
	}
}

func TestReadmitsAfterPeriodOnSuccessfulProbe(t *testing.T) {
	c, w, p, clk := setup()
	p.down.Store(true)
	for range 3 {
		c.probeAll(context.Background())
	}
	if live(w) {
		t.Fatal("not ejected by failing probes")
	}

	p.down.Store(false)
	clk.advance(4 * time.Second)
	c.probeAll(context.Background())
	if live(w) {
		t.Fatal("re-admitted before the ejection period ended")
	}
	clk.advance(time.Second)
	c.probeAll(context.Background())
	if !live(w) {
		t.Fatal("not re-admitted after the period and a successful probe")
	}
}

func TestEjectionPeriodDoublesUpToMax(t *testing.T) {
	c, w, _, clk := setup()
	for _, want := range []time.Duration{5 * time.Second, 10 * time.Second, 12 * time.Second, 12 * time.Second} {
		for range 3 {
			c.ReportFailure("a")
		}
		clk.advance(want - time.Millisecond)
		c.probeAll(context.Background())
		if live(w) {
			t.Fatalf("re-admitted before %v", want)
		}
		clk.advance(time.Millisecond)
		c.probeAll(context.Background())
		if !live(w) {
			t.Fatalf("not re-admitted after %v", want)
		}
	}
}

func TestBudget(t *testing.T) {
	var inFlight atomic.Int64
	b := NewBudget(2, 0.5, func() int { return int(inFlight.Load()) })

	if !b.TryAcquire() || !b.TryAcquire() {
		t.Fatal("floor of 2 not available")
	}
	if b.TryAcquire() {
		t.Fatal("third place granted with 0 in flight")
	}
	inFlight.Store(6) // fraction gives 3
	if !b.TryAcquire() {
		t.Fatal("fraction place not granted")
	}
	if b.TryAcquire() {
		t.Fatal("place above fraction granted")
	}
	b.Release()
	if !b.TryAcquire() {
		t.Fatal("released place not granted again")
	}
}
