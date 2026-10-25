// Package health decides which workers are live. It probes every worker on
// a fixed interval, counts consecutive failures from probes and from real
// requests, and ejects a worker after too many of them.
package health

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/nnviet06/worker-pool-control-plane/internal/pool"
)

// Prober checks one worker. It returns nil when the worker can serve.
type Prober interface {
	Probe(ctx context.Context) error
}

// Config sets probing and ejection.
type Config struct {
	Interval     time.Duration
	ProbeTimeout time.Duration
	// EjectAfter is the number of consecutive failures that ejects a worker.
	EjectAfter int
	// EjectFor is the first ejection period. It doubles on each further
	// ejection of the same worker, up to EjectMax.
	EjectFor time.Duration
	EjectMax time.Duration
}

// Checker tracks the health of every worker in the pool.
type Checker struct {
	cfg Config
	now func() time.Time

	mu      sync.Mutex
	targets map[string]*target
}

type target struct {
	worker       *pool.Worker
	probe        Prober
	failures     int
	ejections    int
	ejectedUntil time.Time // zero while the worker is not ejected
}

// NewChecker returns a checker with no workers. now is the clock; pass
// time.Now outside tests.
func NewChecker(cfg Config, now func() time.Time) *Checker {
	return &Checker{cfg: cfg, now: now, targets: make(map[string]*target)}
}

// Add starts tracking w, probed through p.
func (c *Checker) Add(w *pool.Worker, p Prober) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.targets[w.ID()] = &target{worker: w, probe: p}
}

// Run probes every worker each interval until ctx ends.
func (c *Checker) Run(ctx context.Context) {
	t := time.NewTicker(c.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.probeAll(ctx)
		}
	}
}

// ReportFailure records a transport error on a real request to worker id.
func (c *Checker) ReportFailure(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t, ok := c.targets[id]; ok {
		c.failLocked(t)
	}
}

// ReportSuccess records a completed request to worker id.
func (c *Checker) ReportSuccess(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t, ok := c.targets[id]; ok {
		c.succeedLocked(t)
	}
}

// probeAll probes every worker at once, so one hung worker does not delay
// the others beyond ProbeTimeout.
func (c *Checker) probeAll(ctx context.Context) {
	c.mu.Lock()
	targets := make([]*target, 0, len(c.targets))
	for _, t := range c.targets {
		targets = append(targets, t)
	}
	c.mu.Unlock()

	var wg sync.WaitGroup
	for _, t := range targets {
		wg.Go(func() {
			pctx, cancel := context.WithTimeout(ctx, c.cfg.ProbeTimeout)
			err := t.probe.Probe(pctx)
			cancel()
			c.mu.Lock()
			defer c.mu.Unlock()
			if err != nil {
				c.failLocked(t)
			} else {
				c.succeedLocked(t)
			}
		})
	}
	wg.Wait()
}

func (c *Checker) failLocked(t *target) {
	if !t.ejectedUntil.IsZero() {
		return
	}
	t.failures++
	if t.failures < c.cfg.EjectAfter {
		return
	}
	d := c.cfg.EjectFor
	for range t.ejections {
		if d >= c.cfg.EjectMax {
			break
		}
		d *= 2
	}
	d = min(d, c.cfg.EjectMax)
	t.ejections++
	t.failures = 0
	t.ejectedUntil = c.now().Add(d)
	t.worker.SetLive(false)
	log.Printf("health: ejected %s for %v", t.worker.ID(), d)
}

// succeedLocked resets the failure count, and re-admits an ejected worker
// once its ejection period is over.
func (c *Checker) succeedLocked(t *target) {
	t.failures = 0
	if t.ejectedUntil.IsZero() || c.now().Before(t.ejectedUntil) {
		return
	}
	t.ejectedUntil = time.Time{}
	t.worker.SetLive(true)
	log.Printf("health: re-admitted %s", t.worker.ID())
}
