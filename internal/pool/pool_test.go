package pool

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestReserveUpToCap(t *testing.T) {
	w := NewWorker("a", 2)
	if !w.Reserve("x") || !w.Reserve("y") {
		t.Fatal("reserve below cap failed")
	}
	if w.Reserve("x") {
		t.Fatal("reserve above cap succeeded")
	}
	s := w.Snapshot()
	if s.InFlight != 2 || s.ByClass["x"] != 1 || s.ByClass["y"] != 1 {
		t.Fatalf("snapshot = %+v", s)
	}
}

func TestReserveOnDeadWorker(t *testing.T) {
	w := NewWorker("a", 2)
	w.SetLive(false)
	if w.Reserve("x") {
		t.Fatal("reserve on dead worker succeeded")
	}
}

func TestReleaseFreesSlot(t *testing.T) {
	w := NewWorker("a", 1)
	w.Reserve("x")
	w.Release("x")
	if s := w.Snapshot(); s.InFlight != 0 || len(s.ByClass) != 0 {
		t.Fatalf("snapshot after release = %+v", s)
	}
	if !w.Reserve("x") {
		t.Fatal("reserve after release failed")
	}
}

func TestReleaseWithoutReservePanics(t *testing.T) {
	w := NewWorker("a", 1)
	defer func() {
		if recover() == nil {
			t.Fatal("release without reserve did not panic")
		}
	}()
	w.Release("x")
}

func TestSnapshotIsACopy(t *testing.T) {
	w := NewWorker("a", 2)
	w.Reserve("x")
	s := w.Snapshot()
	s.ByClass["x"] = 5
	if got := w.Snapshot().ByClass["x"]; got != 1 {
		t.Fatalf("worker state changed through snapshot: %d", got)
	}
}

func TestLiveCountsOnlyLiveWorkers(t *testing.T) {
	a, b, c := NewWorker("a", 2), NewWorker("b", 3), NewWorker("c", 4)
	b.SetLive(false)
	workers, slots := New(a, b, c).Live()
	if workers != 2 || slots != 6 {
		t.Fatalf("Live() = %d workers, %d slots; want 2, 6", workers, slots)
	}
}

func TestConcurrentReserveNeverExceedsCap(t *testing.T) {
	w := NewWorker("a", 2)
	var ok atomic.Int32
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			if w.Reserve("x") {
				ok.Add(1)
			}
		})
	}
	wg.Wait()
	if ok.Load() != 2 {
		t.Fatalf("reserved %d slots, cap is 2", ok.Load())
	}
}

func TestConcurrentReserveReleaseWithLivenessChanges(t *testing.T) {
	w := NewWorker("a", 2)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 1000 {
				if w.Reserve("x") {
					if s := w.Snapshot(); s.InFlight > s.Cap {
						t.Errorf("in flight %d above cap %d", s.InFlight, s.Cap)
					}
					w.Release("x")
				}
			}
		})
	}
	wg.Go(func() {
		for i := range 1000 {
			w.SetLive(i%2 == 0)
		}
	})
	wg.Wait()
	if s := w.Snapshot(); s.InFlight != 0 {
		t.Fatalf("in flight after all releases = %d", s.InFlight)
	}
}
