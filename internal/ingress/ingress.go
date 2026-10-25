// Package ingress is the HTTP side of the proxy. It reads the X-Wpcp-*
// headers, admits the request, sends it to the chosen worker, fails it over
// once if allowed, and maps the outcome to an HTTP status.
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
	headerSafeToFailover = "X-Wpcp-Safe-To-Failover"
	headerReason         = "X-Wpcp-Reason"

	defaultClass = "default"
)

var (
	errEjected  = errors.New("ingress: worker ejected during the attempt")
	errNoClient = errors.New("ingress: no client for worker")
)

// Admitter hands out reserved worker slots.
type Admitter interface {
	Acquire(ctx context.Context, class pool.Class, deadline time.Time) (*pool.Worker, error)
	AcquireFront(ctx context.Context, class pool.Class, deadline time.Time, failed *pool.Worker) (*pool.Worker, error)
	Release(w *pool.Worker, class pool.Class)
}

// Executor sends one attempt to one worker.
type Executor interface {
	Execute(ctx context.Context, class string, deadline time.Time, attemptTimeout time.Duration, payload []byte) (*workerpb.ExecuteResponse, error)
}

// Health receives the outcome of real requests, so transport errors count
// toward ejection.
type Health interface {
	ReportFailure(id string)
	ReportSuccess(id string)
}

// Budget is the retry budget for failover.
type Budget interface {
	TryAcquire() bool
	Release()
}

// Config holds the defaults for requests that do not set the headers, and
// the smallest remaining budget worth a failover.
type Config struct {
	Deadline       time.Duration
	AttemptTimeout time.Duration
	SendFloor      time.Duration
	MaxBodyBytes   int64
}

// Handler serves proxied requests.
type Handler struct {
	cfg     Config
	admit   Admitter
	workers map[string]Executor
	health  Health
	budget  Budget
}

// New returns a handler. workers maps a pool worker's ID to its client.
func New(cfg Config, a Admitter, workers map[string]Executor, h Health, b Budget) *Handler {
	return &Handler{cfg: cfg, admit: a, workers: workers, health: h, budget: b}
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
	safe := r.Header.Get(headerSafeToFailover) == "true"
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
	resp, err := h.attempt(ctx, worker, class, deadline, attempt, body)
	if err != nil && safe && h.canFailover(err, deadline) {
		if !h.budget.TryAcquire() {
			writeRejection(w, &admission.RejectError{Reason: queue.ReasonFailoverBudget})
			return
		}
		defer h.budget.Release()
		failed := worker
		if worker, err = h.admit.AcquireFront(ctx, class, deadline, failed); err != nil {
			writeRejection(w, err)
			return
		}
		resp, err = h.attempt(ctx, worker, class, deadline, attempt, body)
	}
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

// attempt sends one try to worker and frees the slot afterwards. The call is
// cancelled if the worker is ejected while it runs.
func (h *Handler) attempt(ctx context.Context, worker *pool.Worker, class pool.Class, deadline time.Time, timeout time.Duration, body []byte) (*workerpb.ExecuteResponse, error) {
	defer h.admit.Release(worker, class)
	exec, ok := h.workers[worker.ID()]
	if !ok {
		return nil, errNoClient
	}

	actx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	down := worker.Down()
	go func() {
		select {
		case <-down:
			cancel(errEjected)
		case <-actx.Done():
		}
	}()

	resp, err := exec.Execute(actx, string(class), deadline, timeout, body)
	switch {
	case err == nil:
		h.health.ReportSuccess(worker.ID())
	case errors.Is(context.Cause(actx), errEjected):
		return nil, errEjected
	case status.Code(err) == codes.Unavailable:
		h.health.ReportFailure(worker.ID())
	}
	return resp, err
}

// canFailover reports whether a failed attempt may move to another worker:
// its worker was ejected, the connection failed, or the attempt timed out,
// and enough of the deadline is left to send again.
func (h *Handler) canFailover(err error, deadline time.Time) bool {
	if time.Until(deadline) <= h.cfg.SendFloor {
		return false
	}
	if errors.Is(err, errEjected) {
		return true
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	}
	return false
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

// writeRejection answers a request that was not sent to a worker.
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

// writeWorkerError answers a request whose last attempt failed: 504 when it
// ran out of time, 502 for anything else the worker or the network returned,
// including an ejection that was not failed over.
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
