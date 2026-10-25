// Package transport is the gRPC client from the proxy to one worker.
package transport

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	workerpb "github.com/nnviet06/worker-pool-control-plane/api/proto"
)

// Client sends requests and health probes to one worker.
type Client struct {
	conn   *grpc.ClientConn
	worker workerpb.WorkerClient
	health healthpb.HealthClient
}

// Dial returns a client for the worker at addr. The connection is opened
// lazily on the first call. Traffic is plaintext: workers sit on a private
// network next to the proxy.
func Dial(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, worker: workerpb.NewWorkerClient(conn), health: healthpb.NewHealthClient(conn)}, nil
}

// Probe asks the worker's grpc.health.v1 service for its overall status and
// returns an error unless it is SERVING.
func (c *Client) Probe(ctx context.Context) error {
	resp, err := c.health.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		return err
	}
	if s := resp.GetStatus(); s != healthpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("transport: worker reports %s", s)
	}
	return nil
}

// Execute sends one attempt. The worker receives the absolute deadline of the
// whole request; the attempt itself is cut off at the earlier of
// attemptTimeout and that deadline.
func (c *Client) Execute(ctx context.Context, class string, deadline time.Time, attemptTimeout time.Duration, payload []byte) (*workerpb.ExecuteResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, min(attemptTimeout, time.Until(deadline)))
	defer cancel()
	return c.worker.Execute(ctx, &workerpb.ExecuteRequest{
		Class:      class,
		DeadlineMs: deadline.UnixMilli(),
		Payload:    payload,
	})
}

// Close closes the connection.
func (c *Client) Close() error { return c.conn.Close() }
