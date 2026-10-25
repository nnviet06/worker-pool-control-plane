// Package ingress is the HTTP side of the proxy. It reads the X-Wpcp-*
// headers, admits the request, sends it to the chosen worker and maps the
// outcome to an HTTP status.
package ingress

import (
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	workerpb "github.com/nnviet06/worker-pool-control-plane/api/proto"
	"github.com/nnviet06/worker-pool-control-plane/internal/admission"
	"github.com/nnviet06/worker-pool-control-plane/internal/pool"
	"github.com/nnviet06/worker-pool-control-plane/internal/queue"
)

const (
	headerClass          = "X-Wpcp-Class"
	headerDeadline       = "X-Wpcp-Deadline-Ms"
	headerAttemptTimeout = "X-Wpcp-Attempt-Timeout-Ms"
	headerReason         = "X-Wpcp-Reason"

	defaultClass = "default"
)

// Admitter hands out reserved worker slots.
type Admitter interface {
	Acquire(ctx context.Context, class pool.Class, deadline time.Time) (*pool.Worker, error)
	Release(w *pool.Worker, class pool.Class)
}

// Executor sends one attempt to one worker.
type Executor interface {
	Execute(ctx context.Context, class string, deadline time.Time, attemptTimeout time.Duration, payload []byte) (*workerpb.ExecuteResponse, error)
}

// Config holds the defaults for requests that do not set the headers.
type Config struct {
	Deadline       time.Duration
	AttemptTimeout time.Duration
	MaxBodyBytes   int64
}

// Handler serves proxied requests.
type Handler struct {
	cfg     Config
	admit   Admitter
	workers map[string]Executor
}

// New returns a handler. workers maps a pool worker's ID to its client.
func New(cfg Config, a Admitter, workers map[string]Executor) *Handler {
	return &Handler{cfg: cfg, admit: a, workers: workers}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	arrival := time.Now()
	class := pool.Class(r.Header.Get(headerClass))
	if class == "" {
		class = defaultClass
	}
	budget, err := millisHeader(r, headerDeadline, h.cfg.Deadline)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	attempt, err := millisHeader(r, headerAttemptTimeout, h.cfg.AttemptTimeout)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if attempt >= budget {
		http.Error(w, headerAttemptTimeout+" must be below the deadline", http.StatusBadRequest)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.cfg.MaxBodyBytes))
	if err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "cannot read request body", http.StatusBadRequest)
		return
	}

	deadline := arrival.Add(budget)
	ctx, cancel := context.WithDeadline(r.Context(), deadline)
	defer cancel()

	worker, err := h.admit.Acquire(ctx, class, deadline)
	if err != nil {
		writeRejection(w, err)
		return
	}
	exec, ok := h.workers[worker.ID()]
	if !ok {
		h.admit.Release(worker, class)
		http.Error(w, "no client for worker", http.StatusInternalServerError)
		return
	}
	resp, err := exec.Execute(ctx, string(class), deadline, attempt, body)
	h.admit.Release(worker, class)
	if err != nil {
		writeWorkerError(w, err)
		return
	}
	code := int(resp.GetStatus())
	if code < 100 || code > 599 {
		http.Error(w, "worker returned an invalid status", http.StatusBadGateway)
		return
	}
	w.WriteHeader(code)
	w.Write(resp.GetPayload())
}

// millisHeader reads a positive number of milliseconds, or returns def when
// the header is absent.
func millisHeader(r *http.Request, name string, def time.Duration) (time.Duration, error) {
	v := r.Header.Get(name)
	if v == "" {
		return def, nil
	}
	ms, err := strconv.ParseInt(v, 10, 64)
	if err != nil || ms <= 0 || ms > math.MaxInt64/int64(time.Millisecond) {
		return 0, errors.New(name + " must be a positive integer")
	}
	return time.Duration(ms) * time.Millisecond, nil
}

// writeRejection answers a request that never reached a worker.
func writeRejection(w http.ResponseWriter, err error) {
	var reason queue.Reason
	re, rejected := errors.AsType[*admission.RejectError](err)
	switch {
	case rejected:
		reason = re.Reason
	case errors.Is(err, context.DeadlineExceeded):
		reason = queue.ReasonDeadline
	case errors.Is(err, context.Canceled):
		return // the client is gone
	default:
		http.Error(w, "admission failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set(headerReason, string(reason))
	w.WriteHeader(http.StatusServiceUnavailable)
}

// writeWorkerError answers a request whose attempt failed: 504 when it ran
// out of time, 502 for anything else the worker or the network returned.
func writeWorkerError(w http.ResponseWriter, err error) {
	switch status.Code(err) {
	case codes.Canceled:
		return // the client is gone
	case codes.DeadlineExceeded:
		w.WriteHeader(http.StatusGatewayTimeout)
	default:
		w.WriteHeader(http.StatusBadGateway)
	}
}
