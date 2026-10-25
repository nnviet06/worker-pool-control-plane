// Package pool holds the control-plane state of each worker: its in-flight
// requests per class and its liveness. One mutex per worker guards both.
package pool

import "sync"

// Class is the request class used for scoring and per-class accounting.
type Class string

// State is a point-in-time copy of one worker's state. It is safe to read
// without holding the worker's lock.
type State struct {
	ID       string
	Live     bool
	Cap      int
	InFlight int
	ByClass  map[Class]int
}

// Free reports whether the worker is live and below its cap.
func (s State) Free() bool { return s.Live && s.InFlight < s.Cap }

// Worker is the state of one worker. Use NewWorker to create one.
type Worker struct {
	id  string
	cap int

	mu       sync.Mutex
	live     bool
	down     chan struct{} // closed when the worker stops being live
	inFlight int
	byClass  map[Class]int
}

// NewWorker returns a live worker with no requests in flight.
func NewWorker(id string, cap int) *Worker {
	return &Worker{id: id, cap: cap, live: true, down: make(chan struct{}), byClass: make(map[Class]int)}
}

// ID returns the worker's identifier.
func (w *Worker) ID() string { return w.id }

// Reserve takes one slot for a request of class c. It succeeds only if the
// worker is live and below its cap; the check and the increment happen under
// the same lock.
func (w *Worker) Reserve(c Class) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.live || w.inFlight >= w.cap {
		return false
	}
	w.inFlight++
	w.byClass[c]++
	return true
}

// Release frees a slot taken by Reserve. Releasing a slot that was never
// reserved is a bug in the caller and panics.
func (w *Worker) Release(c Class) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.byClass[c] == 0 {
		panic("pool: release of class " + string(c) + " with nothing in flight on " + w.id)
	}
	w.inFlight--
	w.byClass[c]--
	if w.byClass[c] == 0 {
		delete(w.byClass, c)
	}
}

// SetLive marks the worker live or not. Requests already in flight keep
// their slots until they are released; marking the worker not live closes
// the channel returned by Down so they can stop.
func (w *Worker) SetLive(live bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if live == w.live {
		return
	}
	w.live = live
	if live {
		w.down = make(chan struct{})
	} else {
		close(w.down)
	}
}

// Down returns a channel that is closed when the worker stops being live.
// A request takes it once after reserving a slot and watches it while it
// runs.
func (w *Worker) Down() <-chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.down
}

// Snapshot returns a copy of the worker's state.
func (w *Worker) Snapshot() State {
	w.mu.Lock()
	defer w.mu.Unlock()
	byClass := make(map[Class]int, len(w.byClass))
	for c, n := range w.byClass {
		byClass[c] = n
	}
	return State{ID: w.id, Live: w.live, Cap: w.cap, InFlight: w.inFlight, ByClass: byClass}
}

// Pool is a fixed set of workers.
type Pool struct {
	workers []*Worker
}

// New returns a pool of the given workers.
func New(workers ...*Worker) *Pool {
	return &Pool{workers: workers}
}

// Workers returns the workers in the pool. The slice must not be modified.
func (p *Pool) Workers() []*Worker { return p.workers }

// Live returns the number of live workers and the sum of their caps.
func (p *Pool) Live() (workers, slots int) {
	for _, w := range p.workers {
		w.mu.Lock()
		if w.live {
			workers++
			slots += w.cap
		}
		w.mu.Unlock()
	}
	return workers, slots
}
