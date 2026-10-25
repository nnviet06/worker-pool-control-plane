package ingress

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	workerpb "github.com/nnviet06/worker-pool-control-plane/api/proto"
	"github.com/nnviet06/worker-pool-control-plane/internal/admission"
	"github.com/nnviet06/worker-pool-control-plane/internal/limiter"
	"github.com/nnviet06/worker-pool-control-plane/internal/picker"
	"github.com/nnviet06/worker-pool-control-plane/internal/pool"
	"github.com/nnviet06/worker-pool-control-plane/internal/queue"
)

// fakeExec answers each attempt with fn and records the last call.
type fakeExec struct {
	fn func(ctx context.Context, call int) (*workerpb.ExecuteResponse, error)

	mu      sync.Mutex
	calls   int
	class   string
	attempt time.Duration
	payload []byte
}

func (f *fakeExec) Execute(ctx context.Context, class string, _ time.Time, attempt time.Duration, payload []byte) (*workerpb.ExecuteResponse, error) {
	f.mu.Lock()
	f.calls++
	call := f.calls
	f.class, f.attempt, f.payload = class, attempt, payload
	f.mu.Unlock()
	return f.fn(ctx, call)
}

func answer(resp *workerpb.ExecuteResponse) *fakeExec {
	return &fakeExec{fn: func(context.Context, int) (*workerpb.ExecuteResponse, error) { return resp, nil }}
}

func fail(code codes.Code) *fakeExec {
	return &fakeExec{fn: func(context.Context, int) (*workerpb.ExecuteResponse, error) {
		return nil, status.Error(code, "")
	}}
}

var ok = &workerpb.ExecuteResponse{Status: 200}

type fakeHealth struct {
	mu        sync.Mutex
	failures  int
	successes int
}

func (f *fakeHealth) ReportFailure(string) { f.mu.Lock(); f.failures++; f.mu.Unlock() }
func (f *fakeHealth) ReportSuccess(string) { f.mu.Lock(); f.successes++; f.mu.Unlock() }

type fakeBudget struct {
	mu   sync.Mutex
	room int
}

func (f *fakeBudget) TryAcquire() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.room == 0 {
		return false
	}
	f.room--
	return true
}

func (f *fakeBudget) Release() { f.mu.Lock(); f.room++; f.mu.Unlock() }

var cfg = Config{Deadline: 5 * time.Second, AttemptTimeout: 2 * time.Second, SendFloor: 10 * time.Millisecond, MaxBodyBytes: 16}

type env struct {
	h       *Handler
	ctrl    *admission.Controller
	health  *fakeHealth
	budget  *fakeBudget
	workers []*pool.Worker
}

// setup builds one worker with cap 1 per exec, named a, b, ...
func setup(execs ...*fakeExec) *env {
	e := &env{health: &fakeHealth{}, budget: &fakeBudget{room: 1}}
	byID := make(map[string]Executor, len(execs))
	for i, exec := range execs {
		w := pool.NewWorker(string(rune('a'+i)), 1)
		e.workers = append(e.workers, w)
		byID[w.ID()] = exec
	}
	p := pool.New(e.workers...)
	e.ctrl = admission.New(p, picker.New(p, picker.LeastInFlight{}), limiter.NewStatic(len(execs)),
		queue.Config{Capacity: 1}, time.Now)
	e.h = New(cfg, e.ctrl, byID, e.health, e.budget)
	return e
}

func serve(h http.Handler, body string, headers map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	return rec
}

var safe = map[string]string{headerSafeToFailover: "true"}

func TestPassesWorkerResponseThrough(t *testing.T) {
	exec := answer(&workerpb.ExecuteResponse{Status: 201, Payload: []byte("done")})
	e := setup(exec)
	rec := serve(e.h, "job", map[string]string{headerClass: "prefill", headerAttemptTimeout: "300"})

	if rec.Code != 201 || rec.Body.String() != "done" {
		t.Fatalf("got %d %q", rec.Code, rec.Body.String())
	}
	if exec.class != "prefill" || exec.attempt != 300*time.Millisecond || string(exec.payload) != "job" {
		t.Fatalf("worker got class %q, attempt %v, payload %q", exec.class, exec.attempt, exec.payload)
	}
	if e.ctrl.InFlight() != 0 || e.health.successes != 1 {
		t.Fatalf("in flight %d, successes reported %d", e.ctrl.InFlight(), e.health.successes)
	}
}

func TestDefaultsWithoutHeaders(t *testing.T) {
	exec := answer(ok)
	serve(setup(exec).h, "", nil)
	if exec.class != defaultClass || exec.attempt != cfg.AttemptTimeout {
		t.Fatalf("worker got class %q, attempt %v", exec.class, exec.attempt)
	}
}

func TestBadRequests(t *testing.T) {
	h := setup(answer(ok)).h
	for name, tc := range map[string]struct {
		body    string
		headers map[string]string
		want    int
	}{
		"deadline not a number":    {"", map[string]string{headerDeadline: "soon"}, http.StatusBadRequest},
		"deadline not positive":    {"", map[string]string{headerDeadline: "0"}, http.StatusBadRequest},
		"deadline overflows":       {"", map[string]string{headerDeadline: "99999999999999"}, http.StatusBadRequest},
		"attempt not below budget": {"", map[string]string{headerDeadline: "100", headerAttemptTimeout: "100"}, http.StatusBadRequest},
		"body too large":           {strings.Repeat("x", 17), nil, http.StatusRequestEntityTooLarge},
	} {
		if rec := serve(h, tc.body, tc.headers); rec.Code != tc.want {
			t.Errorf("%s: status %d, want %d", name, rec.Code, tc.want)
		}
	}
}

func TestRejectionCarriesReason(t *testing.T) {
	e := setup(answer(ok))
	e.workers[0].SetLive(false)
	rec := serve(e.h, "", nil)
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get(headerReason) != string(queue.ReasonNoWorker) {
		t.Fatalf("got %d reason %q", rec.Code, rec.Header().Get(headerReason))
	}
}

func TestWorkerErrorsWithoutFailover(t *testing.T) {
	for _, tc := range []struct {
		code     codes.Code
		want     int
		failures int
	}{
		{codes.DeadlineExceeded, http.StatusGatewayTimeout, 0},
		{codes.Unavailable, http.StatusBadGateway, 1},
		{codes.Internal, http.StatusBadGateway, 0},
	} {
		e := setup(fail(tc.code))
		rec := serve(e.h, "", nil)
		if rec.Code != tc.want || rec.Header().Get(headerReason) != "" {
			t.Errorf("%v: got %d reason %q, want %d and no reason", tc.code, rec.Code, rec.Header().Get(headerReason), tc.want)
		}
		if e.ctrl.InFlight() != 0 || e.health.failures != tc.failures {
			t.Errorf("%v: in flight %d, failures reported %d, want %d", tc.code, e.ctrl.InFlight(), e.health.failures, tc.failures)
		}
	}
}

func TestInvalidWorkerStatus(t *testing.T) {
	if rec := serve(setup(answer(&workerpb.ExecuteResponse{Status: 0})).h, "", nil); rec.Code != http.StatusBadGateway {
		t.Fatalf("status %d, want 502", rec.Code)
	}
}

func TestFailoverAfterTransportError(t *testing.T) {
	exec := &fakeExec{fn: func(_ context.Context, call int) (*workerpb.ExecuteResponse, error) {
		if call == 1 {
			return nil, status.Error(codes.Unavailable, "")
		}
		return ok, nil
	}}
	e := setup(exec, exec)
	rec := serve(e.h, "", safe)
	if rec.Code != http.StatusOK || exec.calls != 2 {
		t.Fatalf("got %d after %d calls, want 200 after 2", rec.Code, exec.calls)
	}
	if e.budget.room != 1 || e.ctrl.InFlight() != 0 {
		t.Fatalf("budget room %d, in flight %d after failover", e.budget.room, e.ctrl.InFlight())
	}
}

func TestFailoverAvoidsFailedWorker(t *testing.T) {
	broken := fail(codes.Unavailable)
	e := setup(broken, answer(ok))
	for range 20 {
		if rec := serve(e.h, "", safe); rec.Code != http.StatusOK {
			t.Fatalf("got %d, want 200: the second attempt went back to the failed worker", rec.Code)
		}
	}
	if broken.calls == 0 {
		t.Fatal("the broken worker was never tried, the test proved nothing")
	}
}

func TestWorkerErrorIsNotFailedOver(t *testing.T) {
	exec := fail(codes.Internal)
	rec := serve(setup(exec, exec).h, "", safe)
	if rec.Code != http.StatusBadGateway || exec.calls != 1 {
		t.Fatalf("got %d after %d calls, want 502 after 1", rec.Code, exec.calls)
	}
}

func TestFailoverBudgetSpent(t *testing.T) {
	e := setup(fail(codes.Unavailable), fail(codes.Unavailable))
	e.budget.room = 0
	rec := serve(e.h, "", safe)
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get(headerReason) != string(queue.ReasonFailoverBudget) {
		t.Fatalf("got %d reason %q", rec.Code, rec.Header().Get(headerReason))
	}
}

func TestEjectionCancelsAttemptAndFailsOver(t *testing.T) {
	started := make(chan struct{})
	exec := &fakeExec{fn: func(ctx context.Context, call int) (*workerpb.ExecuteResponse, error) {
		if call == 1 {
			close(started)
			<-ctx.Done()
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return ok, nil
	}}
	e := setup(exec, exec)
	done := make(chan *httptest.ResponseRecorder)
	go func() { done <- serve(e.h, "", safe) }()

	<-started
	for _, w := range e.workers {
		if w.Snapshot().InFlight == 1 {
			w.SetLive(false)
		}
	}
	if rec := <-done; rec.Code != http.StatusOK || exec.calls != 2 {
		t.Fatalf("got %d after %d calls, want 200 after 2", rec.Code, exec.calls)
	}
}
