// Package transport is the gRPC client from the proxy to one worker.
package transport

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	workerpb "github.com/nnviet06/worker-pool-control-plane/api/proto"
)

// Client sends requests to one worker.
type Client struct {
	conn   *grpc.ClientConn
	worker workerpb.WorkerClient
}

// Dial returns a client for the worker at addr. The connection is opened
// lazily on the first call. Traffic is plaintext: workers sit on a private
// network next to the proxy.
func Dial(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, worker: workerpb.NewWorkerClient(conn)}, nil
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
