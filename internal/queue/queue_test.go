package queue

import (
	"slices"
	"sync"
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func popAll(q *Queue[string]) []string {
	var vs []string
	for {
		v, ok := q.Pop()
		if !ok {
			return vs
		}
		vs = append(vs, v)
	}
}

func TestFIFO(t *testing.T) {
	q := New[string](Config{Capacity: 3})
	for _, v := range []string{"a", "b", "c"} {
		q.Push(v, t0, t0.Add(time.Minute))
	}
	if got := popAll(q); !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Fatalf("pop order = %v", got)
	}
}

func TestPushRejectsWhenFull(t *testing.T) {
	q := New[string](Config{Capacity: 1})
	if !q.Push("a", t0, t0.Add(time.Minute)) {
		t.Fatal("push into empty queue failed")
	}
	if q.Push("b", t0, t0.Add(time.Minute)) {
		t.Fatal("push into full queue succeeded")
	}
}

func TestPushFrontGoesFirstAndIgnoresCapacity(t *testing.T) {
	q := New[string](Config{Capacity: 1})
	q.Push("a", t0, t0.Add(time.Minute))
	q.PushFront("b", t0, t0.Add(time.Minute))
	if got := popAll(q); !slices.Equal(got, []string{"b", "a"}) {
		t.Fatalf("pop order = %v", got)
	}
}

func TestSweepDropRules(t *testing.T) {
	q := New[string](Config{Capacity: 10, MaxWait: 2 * time.Second, SendFloor: 100 * time.Millisecond})
	now := t0.Add(5 * time.Second)
	q.Push("expired", t0.Add(4*time.Second), now.Add(-time.Millisecond))
	q.Push("below floor", t0.Add(4*time.Second), now.Add(50*time.Millisecond))
	q.Push("waited too long", t0, now.Add(time.Minute))
	q.Push("ok", t0.Add(4*time.Second), now.Add(time.Minute))

	want := []Drop[string]{
		{"expired", ReasonDeadline},
		{"below floor", ReasonDeadline},
		{"waited too long", ReasonQueueWait},
	}
	if got := q.Sweep(now); !slices.Equal(got, want) {
		t.Fatalf("drops = %v, want %v", got, want)
	}
	if got := popAll(q); !slices.Equal(got, []string{"ok"}) {
		t.Fatalf("kept = %v", got)
	}
}

func TestSweepWithoutMaxWait(t *testing.T) {
	q := New[string](Config{Capacity: 1})
	q.Push("a", t0, t0.Add(time.Hour))
	if drops := q.Sweep(t0.Add(time.Minute)); len(drops) != 0 {
		t.Fatalf("drops = %v with MaxWait disabled", drops)
	}
}

func TestDropAll(t *testing.T) {
	q := New[string](Config{Capacity: 3})
	q.Push("a", t0, t0.Add(time.Minute))
	q.Push("b", t0, t0.Add(time.Minute))
	if got := q.DropAll(); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("DropAll() = %v", got)
	}
	if q.Len() != 0 {
		t.Fatalf("Len() after DropAll = %d", q.Len())
	}
}

func TestConcurrentUse(t *testing.T) {
	q := New[int](Config{Capacity: 50, MaxWait: time.Second})
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Go(func() { q.Push(i, t0, t0.Add(time.Minute)) })
		wg.Go(func() { q.Pop() })
		wg.Go(func() { q.Sweep(t0) })
	}
	wg.Wait()
	if n := q.Len(); n < 0 || n > 50 {
		t.Fatalf("Len() = %d outside [0, 50]", n)
	}
}
