package main

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	workerpb "github.com/nnviet06/worker-pool-control-plane/api/proto"
)

// server answers Execute after a fixed latency per class. It runs at most
// cap requests at once and rejects the rest, since the proxy is expected to
// hold the cap and an overflow means a proxy bug.
type server struct {
	workerpb.UnimplementedWorkerServer
	latency latencyFlag
	slots   chan struct{}
}

func newServer(latency latencyFlag, capacity int) *server {
	return &server{latency: latency, slots: make(chan struct{}, capacity)}
}

func (s *server) Execute(ctx context.Context, req *workerpb.ExecuteRequest) (*workerpb.ExecuteResponse, error) {
	select {
	case s.slots <- struct{}{}:
	default:
		return nil, status.Error(codes.ResourceExhausted, "fakeworker: over cap")
	}
	defer func() { <-s.slots }()

	start := time.Now()
	t := time.NewTimer(s.latency.of(req.GetClass()))
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &workerpb.ExecuteResponse{
		Status:    200,
		Payload:   req.GetPayload(),
		TimingsMs: map[string]int64{"service": time.Since(start).Milliseconds()},
	}, nil
}
