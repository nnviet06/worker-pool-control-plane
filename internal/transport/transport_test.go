package transport

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	workerpb "github.com/nnviet06/worker-pool-control-plane/api/proto"
)

// recorder answers at once and keeps what the last request carried.
type recorder struct {
	workerpb.UnimplementedWorkerServer
	req      *workerpb.ExecuteRequest
	deadline time.Time
}

func (r *recorder) Execute(ctx context.Context, req *workerpb.ExecuteRequest) (*workerpb.ExecuteResponse, error) {
	r.req = req
	r.deadline, _ = ctx.Deadline()
	return &workerpb.ExecuteResponse{Status: 200, Payload: req.GetPayload()}, nil
}

func startWorker(t *testing.T) (*recorder, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rec := &recorder{}
	s := grpc.NewServer()
	workerpb.RegisterWorkerServer(s, rec)
	go s.Serve(lis)
	t.Cleanup(s.Stop)
	return rec, lis.Addr().String()
}

func TestExecuteCarriesDeadlineAndCutsAttempt(t *testing.T) {
	rec, addr := startWorker(t)
	c, err := Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	deadline := time.Now().Add(time.Minute)
	resp, err := c.Execute(context.Background(), "prefill", deadline, time.Second, []byte("hi"))
	if err != nil {
		t.Fatal(err)
	}
	if string(resp.GetPayload()) != "hi" {
		t.Fatalf("payload = %q", resp.GetPayload())
	}
	if rec.req.GetClass() != "prefill" || rec.req.GetDeadlineMs() != deadline.UnixMilli() {
		t.Fatalf("worker got %v", rec.req)
	}
	if left := time.Until(rec.deadline); left > time.Second {
		t.Fatalf("attempt deadline %v away, want at most the 1s attempt timeout", left)
	}
}
