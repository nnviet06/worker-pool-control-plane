// Command wpcp is the worker pool control plane proxy.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nnviet06/worker-pool-control-plane/internal/admission"
	"github.com/nnviet06/worker-pool-control-plane/internal/ingress"
	"github.com/nnviet06/worker-pool-control-plane/internal/limiter"
	"github.com/nnviet06/worker-pool-control-plane/internal/picker"
	"github.com/nnviet06/worker-pool-control-plane/internal/pool"
	"github.com/nnviet06/worker-pool-control-plane/internal/queue"
	"github.com/nnviet06/worker-pool-control-plane/internal/transport"
)

type config struct {
	listen, admin string
	workers       workersFlag
	cap           int
	queue         queue.Config
	ingress       ingress.Config
	sweep         time.Duration
}

func main() {
	var cfg config
	flag.StringVar(&cfg.listen, "listen", "127.0.0.1:8080", "proxy listen address")
	flag.StringVar(&cfg.admin, "admin", "127.0.0.1:9090", "admin listen address for /debug/state")
	flag.Var(&cfg.workers, "workers", "worker list, e.g. a=127.0.0.1:50051,b=127.0.0.1:50052")
	flag.IntVar(&cfg.cap, "cap", 2, "requests each worker runs at once")
	flag.IntVar(&cfg.queue.Capacity, "queue", 16, "queue capacity")
	flag.DurationVar(&cfg.queue.MaxWait, "max-queue-wait", 2*time.Second, "longest a request may wait in the queue")
	flag.DurationVar(&cfg.queue.SendFloor, "send-floor", 50*time.Millisecond, "smallest remaining budget worth sending")
	flag.DurationVar(&cfg.ingress.Deadline, "deadline", 5*time.Second, "default request deadline")
	flag.DurationVar(&cfg.ingress.AttemptTimeout, "attempt-timeout", 3*time.Second, "default per-attempt timeout")
	flag.Int64Var(&cfg.ingress.MaxBodyBytes, "max-body", 1<<20, "largest request body in bytes")
	flag.DurationVar(&cfg.sweep, "sweep", 100*time.Millisecond, "queue sweep interval")
	flag.Parse()
	if len(cfg.workers) == 0 {
		log.Fatal("wpcp: -workers is required")
	}
	if err := run(cfg); err != nil {
		log.Fatal("wpcp: ", err)
	}
}

func run(cfg config) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	workers := make([]*pool.Worker, 0, len(cfg.workers))
	clients := make(map[string]ingress.Executor, len(cfg.workers))
	for _, w := range cfg.workers {
		c, err := transport.Dial(w.addr)
		if err != nil {
			return err
		}
		defer c.Close()
		clients[w.id] = c
		workers = append(workers, pool.NewWorker(w.id, cfg.cap))
	}
	p := pool.New(workers...)
	lim := limiter.NewStatic(len(workers) * cfg.cap)
	ctrl := admission.New(p, picker.New(p, picker.LeastInFlight{}), lim, cfg.queue, time.Now)
	go ctrl.Run(ctx, cfg.sweep)
	log.Printf("wpcp: %d workers, limit %d", len(workers), lim.Limit())

	proxyMux := http.NewServeMux()
	proxyMux.Handle("POST /", ingress.New(cfg.ingress, ctrl, clients))
	adminMux := http.NewServeMux()
	adminMux.HandleFunc("GET /debug/state", debugState(p, ctrl))

	servers := []*http.Server{
		{Addr: cfg.listen, Handler: proxyMux, ReadHeaderTimeout: 5 * time.Second},
		{Addr: cfg.admin, Handler: adminMux, ReadHeaderTimeout: 5 * time.Second},
	}
	errc := make(chan error, len(servers))
	for _, s := range servers {
		go func() { errc <- s.ListenAndServe() }()
	}
	log.Printf("wpcp: proxy %s, admin %s", cfg.listen, cfg.admin)

	var err error
	select {
	case <-ctx.Done():
	case err = <-errc:
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, s := range servers {
		s.Shutdown(shutdown)
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

type workerState struct {
	ID       string         `json:"id"`
	Live     bool           `json:"live"`
	Cap      int            `json:"cap"`
	InFlight int            `json:"in_flight"`
	ByClass  map[string]int `json:"by_class"`
}

// debugState serves the control-plane state as JSON.
func debugState(p *pool.Pool, ctrl *admission.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		var ws []workerState
		for _, worker := range p.Workers() {
			s := worker.Snapshot()
			byClass := make(map[string]int, len(s.ByClass))
			for c, n := range s.ByClass {
				byClass[string(c)] = n
			}
			ws = append(ws, workerState{s.ID, s.Live, s.Cap, s.InFlight, byClass})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(struct {
			Admission admission.Stats `json:"admission"`
			Workers   []workerState   `json:"workers"`
		}{ctrl.Stats(), ws})
	}
}
