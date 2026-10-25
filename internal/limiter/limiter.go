// Package limiter holds the admission limit: how many requests may be in
// flight across the pool at once.
package limiter

// Static is a fixed limit, the sum of the configured worker caps. It is the
// baseline the gradient limiter is compared against.
type Static struct {
	limit int
}

// NewStatic returns a limiter fixed at limit, raised to 1 if lower.
func NewStatic(limit int) *Static {
	return &Static{limit: max(limit, 1)}
}

// Limit returns the current limit.
func (s *Static) Limit() int { return s.limit }

// Effective clamps a limit to what the live part of the pool can run: at
// least one request per live worker, at most the sum of their caps. With no
// live worker it returns 0.
func Effective(limit, liveWorkers, liveSlots int) int {
	if liveWorkers == 0 {
		return 0
	}
	return max(liveWorkers, min(limit, liveSlots))
}
