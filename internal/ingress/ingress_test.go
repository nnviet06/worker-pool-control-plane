package ingress

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
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

// fakeExec answers every attempt with resp or err and records the last call.
type fakeExec struct {
	resp    *workerpb.ExecuteResponse
	err     error
	class   string
	attempt time.Duration
	payload []byte
}

func (f *fakeExec) Execute(_ context.Context, class string, _ time.Time, attempt time.Duration, payload []byte) (*workerpb.ExecuteResponse, error) {
	f.class, f.attempt, f.payload = class, attempt, payload
	return f.resp, f.err
}

var cfg = Config{Deadline: 5 * time.Second, AttemptTimeout: 2 * time.Second, MaxBodyBytes: 16}

func setup(exec *fakeExec, live bool) (*Handler, *admission.Controller) {
	w := pool.NewWorker("a", 1)
	w.SetLive(live)
	p := pool.New(w)
	ctrl := admission.New(p, picker.New(p, picker.LeastInFlight{}), limiter.NewStatic(1),
		queue.Config{Capacity: 1}, time.Now)
	return New(cfg, ctrl, map[string]Executor{"a": exec}), ctrl
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

func TestPassesWorkerResponseThrough(t *testing.T) {
	exec := &fakeExec{resp: &workerpb.ExecuteResponse{Status: 201, Payload: []byte("done")}}
	h, ctrl := setup(exec, true)
	rec := serve(h, "job", map[string]string{headerClass: "prefill", headerAttemptTimeout: "300"})

	if rec.Code != 201 || rec.Body.String() != "done" {
		t.Fatalf("got %d %q", rec.Code, rec.Body.String())
	}
	if exec.class != "prefill" || exec.attempt != 300*time.Millisecond || string(exec.payload) != "job" {
		t.Fatalf("worker got class %q, attempt %v, payload %q", exec.class, exec.attempt, exec.payload)
	}
	if s := ctrl.Stats(); s.InFlight != 0 {
		t.Fatalf("slot not released: %+v", s)
	}
}

func TestDefaultsWithoutHeaders(t *testing.T) {
	exec := &fakeExec{resp: &workerpb.ExecuteResponse{Status: 200}}
	h, _ := setup(exec, true)
	serve(h, "", nil)
	if exec.class != defaultClass || exec.attempt != cfg.AttemptTimeout {
		t.Fatalf("worker got class %q, attempt %v", exec.class, exec.attempt)
	}
}

func TestBadRequests(t *testing.T) {
	h, _ := setup(&fakeExec{}, true)
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
	h, _ := setup(&fakeExec{}, false)
	rec := serve(h, "", nil)
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get(headerReason) != string(queue.ReasonNoWorker) {
		t.Fatalf("got %d reason %q", rec.Code, rec.Header().Get(headerReason))
	}
}

func TestWorkerErrors(t *testing.T) {
	for _, tc := range []struct {
		code codes.Code
		want int
	}{
		{codes.DeadlineExceeded, http.StatusGatewayTimeout},
		{codes.Unavailable, http.StatusBadGateway},
		{codes.Internal, http.StatusBadGateway},
	} {
		h, ctrl := setup(&fakeExec{err: status.Error(tc.code, "")}, true)
		rec := serve(h, "", nil)
		if rec.Code != tc.want || rec.Header().Get(headerReason) != "" {
			t.Errorf("%v: got %d reason %q, want %d and no reason", tc.code, rec.Code, rec.Header().Get(headerReason), tc.want)
		}
		if s := ctrl.Stats(); s.InFlight != 0 {
			t.Errorf("%v: slot not released", tc.code)
		}
	}
}

func TestInvalidWorkerStatus(t *testing.T) {
	h, _ := setup(&fakeExec{resp: &workerpb.ExecuteResponse{Status: 0}}, true)
	if rec := serve(h, "", nil); rec.Code != http.StatusBadGateway {
		t.Fatalf("status %d, want 502", rec.Code)
	}
}
