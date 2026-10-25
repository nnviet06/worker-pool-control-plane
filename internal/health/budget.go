package health

import "sync"

// Budget is the retry budget: at most max(floor, fraction × in-flight)
// requests may be in their second attempt at once. The floor keeps failover
// possible on a small pool, where the fraction alone rounds to zero.
type Budget struct {
	floor    int
	fraction float64
	inFlight func() int

	mu     sync.Mutex
	active int
}

// NewBudget returns a budget. inFlight reports the requests in flight now.
func NewBudget(floor int, fraction float64, inFlight func() int) *Budget {
	return &Budget{floor: floor, fraction: fraction, inFlight: inFlight}
}

// TryAcquire takes one place for a second attempt, or reports that the
// budget is spent.
func (b *Budget) TryAcquire() bool {
	limit := max(b.floor, int(b.fraction*float64(b.inFlight())))
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active >= limit {
		return false
	}
	b.active++
	return true
}

// Release gives back a place taken by TryAcquire.
func (b *Budget) Release() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.active--
}
