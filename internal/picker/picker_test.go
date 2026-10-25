package picker

import (
	"errors"
	"sync"
	"testing"

	"github.com/nnviet06/worker-pool-control-plane/internal/pool"
)

type scoreFunc func(pool.State, pool.Class) float64

func (f scoreFunc) Score(s pool.State, c pool.Class) float64 { return f(s, c) }

func TestPickNoWorker(t *testing.T) {
	dead := pool.NewWorker("a", 2)
	dead.SetLive(false)
	for name, p := range map[string]*pool.Pool{
		"empty pool": pool.New(),
		"all dead":   pool.New(dead),
	} {
		if _, err := New(p, LeastInFlight{}).Pick("x"); !errors.Is(err, ErrNoWorker) {
			t.Errorf("%s: err = %v, want ErrNoWorker", name, err)
		}
	}
}

func TestPickAllFull(t *testing.T) {
	a := pool.NewWorker("a", 1)
	a.Reserve("x")
	if _, err := New(pool.New(a), LeastInFlight{}).Pick("x"); !errors.Is(err, ErrNoSlot) {
		t.Fatalf("err = %v, want ErrNoSlot", err)
	}
}

func TestPickPrefersLowerScore(t *testing.T) {
	a, b := pool.NewWorker("a", 2), pool.NewWorker("b", 2)
	a.Reserve("x")
	p := New(pool.New(a, b), LeastInFlight{})
	for range 50 {
		w, err := p.Pick("x")
		if err != nil {
			t.Fatal(err)
		}
		if w.ID() != "b" {
			t.Fatalf("picked %s, want the emptier worker b", w.ID())
		}
		w.Release("x")
	}
}

func TestPickSkipsDeadAndFull(t *testing.T) {
	dead, full, free := pool.NewWorker("dead", 2), pool.NewWorker("full", 1), pool.NewWorker("free", 2)
	dead.SetLive(false)
	full.Reserve("x")
	p := New(pool.New(dead, full, free), LeastInFlight{})
	for range 50 {
		w, err := p.Pick("x")
		if err != nil {
			t.Fatal(err)
		}
		if w.ID() != "free" {
			t.Fatalf("picked %s, want free", w.ID())
		}
		w.Release("x")
	}
}

func TestPickAvoiding(t *testing.T) {
	a, b := pool.NewWorker("a", 2), pool.NewWorker("b", 2)
	b.Reserve("x") // a scores better, but is avoided
	p := New(pool.New(a, b), LeastInFlight{})
	for range 50 {
		w, err := p.PickAvoiding("x", a)
		if err != nil {
			t.Fatal(err)
		}
		if w.ID() != "b" {
			t.Fatalf("picked %s, want b", w.ID())
		}
		w.Release("x")
	}
	if _, err := New(pool.New(a), LeastInFlight{}).PickAvoiding("x", a); !errors.Is(err, ErrNoSlot) {
		t.Fatalf("err = %v, want ErrNoSlot when only the avoided worker is free", err)
	}
}

func TestPickUsesScorer(t *testing.T) {
	a, b := pool.NewWorker("a", 2), pool.NewWorker("b", 2)
	a.Reserve("x")
	busiest := scoreFunc(func(s pool.State, _ pool.Class) float64 { return -float64(s.InFlight) })
	w, err := New(pool.New(a, b), busiest).Pick("x")
	if err != nil {
		t.Fatal(err)
	}
	if w.ID() != "a" {
		t.Fatalf("picked %s, want a under a scorer that prefers busy workers", w.ID())
	}
}

func TestConcurrentPickNeverExceedsCap(t *testing.T) {
	var workers []*pool.Worker
	for _, id := range []string{"a", "b", "c", "d"} {
		workers = append(workers, pool.NewWorker(id, 2))
	}
	p := New(pool.New(workers...), LeastInFlight{})

	var mu sync.Mutex
	var held []*pool.Worker
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			if w, err := p.Pick("x"); err == nil {
				mu.Lock()
				held = append(held, w)
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	if len(held) > 8 {
		t.Fatalf("reserved %d slots, pool capacity is 8", len(held))
	}
	total := 0
	for _, w := range workers {
		total += w.Snapshot().InFlight
	}
	if total != len(held) {
		t.Fatalf("workers report %d in flight, picker returned %d", total, len(held))
	}
	for _, w := range held {
		w.Release("x")
	}
	for _, w := range workers {
		if s := w.Snapshot(); s.InFlight != 0 {
			t.Fatalf("%s in flight after release = %d", s.ID, s.InFlight)
		}
	}
}
