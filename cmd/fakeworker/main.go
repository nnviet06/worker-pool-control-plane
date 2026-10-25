// Command fakeworker is a gRPC worker for development and fault injection.
// It answers after a configured latency per class, enforces a concurrency
// cap, serves grpc.health.v1, and exits at once on POST /die.
package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	workerpb "github.com/nnviet06/worker-pool-control-plane/api/proto"
)

func main() {
	grpcAddr := flag.String("grpc", ":50051", "gRPC listen address")
	adminAddr := flag.String("admin", ":8081", "admin HTTP listen address")
	capacity := flag.Int("cap", 2, "requests run at once")
	latency := latencyFlag{"default": 200 * time.Millisecond}
	flag.Var(latency, "latency", "service time per class, e.g. default=200ms,prefill=400ms")
	flag.Parse()

	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatal(err)
	}
	gs := grpc.NewServer()
	workerpb.RegisterWorkerServer(gs, newServer(latency, *capacity))
	healthpb.RegisterHealthServer(gs, health.NewServer())

	mux := http.NewServeMux()
	mux.HandleFunc("POST /die", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		log.Print("fakeworker: exiting on /die")
		os.Exit(1)
	})
	go func() { log.Fatal(http.ListenAndServe(*adminAddr, mux)) }()

	log.Printf("fakeworker: grpc %s, admin %s, cap %d, latency %s", *grpcAddr, *adminAddr, *capacity, latency)
	log.Fatal(gs.Serve(lis))
}
