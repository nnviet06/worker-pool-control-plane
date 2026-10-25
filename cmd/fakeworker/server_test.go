package main

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	workerpb "github.com/nnviet06/worker-pool-control-plane/api/proto"
)

func TestExecuteWaitsClassLatency(t *testing.T) {
	s := newServer(latencyFlag{"default": time.Millisecond, "slow": 30 * time.Millisecond}, 1)
	start := time.Now()
	resp, err := s.Execute(context.Background(), &workerpb.ExecuteRequest{Class: "slow", Payload: []byte("hi")})
	if err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took < 30*time.Millisecond {
		t.Fatalf("returned after %v, want at least 30ms", took)
	}
	if string(resp.GetPayload()) != "hi" || resp.GetStatus() != 200 {
		t.Fatalf("resp = %v", resp)
	}
	if _, ok := resp.GetTimingsMs()["service"]; !ok {
		t.Fatal("timings_ms has no service entry")
	}
}

func TestExecuteRejectsOverCap(t *testing.T) {
	s := newServer(latencyFlag{"default": time.Millisecond}, 1)
	s.slots <- struct{}{}
	_, err := s.Execute(context.Background(), &workerpb.ExecuteRequest{})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("err = %v, want ResourceExhausted", err)
	}
}

func TestExecuteStopsAtDeadline(t *testing.T) {
	s := newServer(latencyFlag{"default": time.Minute}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := s.Execute(ctx, &workerpb.ExecuteRequest{})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	if len(s.slots) != 0 {
		t.Fatal("slot not released after deadline")
	}
}

func TestLatencyFlag(t *testing.T) {
	l := latencyFlag{"default": time.Second}
	if err := l.Set("prefill=400ms,decode=2s"); err != nil {
		t.Fatal(err)
	}
	if l.of("prefill") != 400*time.Millisecond || l.of("decode") != 2*time.Second || l.of("other") != time.Second {
		t.Fatalf("parsed %v", l)
	}
	for _, bad := range []string{"prefill", "=1s", "prefill=fast"} {
		if err := (latencyFlag{}).Set(bad); err == nil {
			t.Errorf("Set(%q) accepted", bad)
		}
	}
}
