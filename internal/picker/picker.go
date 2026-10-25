// Package picker places a request on a worker: power of two choices over a
// pluggable Scorer, followed by an atomic reservation on the chosen worker.
package picker

import (
	"errors"
	"math/rand/v2"

	"github.com/nnviet06/worker-pool-control-plane/internal/pool"
)

// Scorer returns the expected cost of placing a request of class c on a
// worker in state w. Lower is better.
type Scorer interface {
	Score(w pool.State, c pool.Class) float64
}

// LeastInFlight scores a worker by its in-flight count plus the new request.
type LeastInFlight struct{}

// Score implements Scorer.
func (LeastInFlight) Score(w pool.State, _ pool.Class) float64 {
	return float64(w.InFlight + 1)
}

var (
	// ErrNoWorker means no worker in the pool is live.
	ErrNoWorker = errors.New("picker: no live worker")
	// ErrNoSlot means workers are live but no slot could be reserved.
	ErrNoSlot = errors.New("picker: no free slot")
)

// Picker chooses a worker for each request.
type Picker struct {
	pool   *pool.Pool
	scorer Scorer
}

// New returns a picker over p using scorer s.
func New(p *pool.Pool, s Scorer) *Picker {
	return &Picker{pool: p, scorer: s}
}

// Pick chooses a worker for a request of class c and reserves a slot on it.
// The caller frees the slot with Release on the returned worker. If the
// reservation fails because the state changed after scoring, Pick scores one
// new pair; if that fails too it returns ErrNoSlot, and the caller puts the
// request at the head of the queue.
func (p *Picker) Pick(c pool.Class) (*pool.Worker, error) {
	for range 2 {
		w, err := p.choose(c)
		if err != nil {
			return nil, err
		}
		if w.Reserve(c) {
			return w, nil
		}
	}
	return nil, ErrNoSlot
}

type candidate struct {
	w *pool.Worker
	s pool.State
}

// choose draws two random candidates among live workers with a free slot and
// returns the one with the lower score. Full workers are left out because a
// reservation on them cannot succeed.
func (p *Picker) choose(c pool.Class) (*pool.Worker, error) {
	var cands []candidate
	live := false
	for _, w := range p.pool.Workers() {
		s := w.Snapshot()
		live = live || s.Live
		if s.Free() {
			cands = append(cands, candidate{w, s})
		}
	}
	switch {
	case !live:
		return nil, ErrNoWorker
	case len(cands) == 0:
		return nil, ErrNoSlot
	case len(cands) == 1:
		return cands[0].w, nil
	}
	i := rand.IntN(len(cands))
	j := rand.IntN(len(cands) - 1)
	if j >= i {
		j++
	}
	a, b := cands[i], cands[j]
	if p.scorer.Score(b.s, c) < p.scorer.Score(a.s, c) {
		return b.w, nil
	}
	return a.w, nil
}
