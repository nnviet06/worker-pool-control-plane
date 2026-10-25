<div align="center">

# worker-pool-control-plane

### Keeps a small pool of expensive workers serving under overload and worker failure.

[![CI](https://github.com/nnviet06/worker-pool-control-plane/actions/workflows/ci.yml/badge.svg)](https://github.com/nnviet06/worker-pool-control-plane/actions/workflows/ci.yml)
![Go](https://img.shields.io/badge/Go-1.27-00ADD8?logo=go&logoColor=white)
![gRPC](https://img.shields.io/badge/gRPC-244c5a?logo=grpc&logoColor=white)
![k6](https://img.shields.io/badge/k6-7D64FF?logo=k6&logoColor=white)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

[Overview](#overview) · [Features](#features) · [Quick start](#quick-start) · [How it works](#how-it-works) · [Status](#status) · [Design](docs/design.md)

</div>

## Overview

Some workers are slow, expensive and can only run a few requests at once: a
self-hosted model server, a headless browser pool, an ffmpeg pool. Send them
more than they can take and every request waits, then every request times
out.

**`wpcp` sits in front of the pool and keeps it serving.** It admits what the
pool can run, rejects the rest fast with a reason, and moves requests off a
worker that dies. Clients keep the worker's API: the request goes in, the
worker's response or a fast 503 comes out.

| Without `wpcp` | With `wpcp` |
|---|---|
| Overload: everything queues, most requests time out | Admitted requests run, the rest get a fast 503 |
| A worker dies: its requests fail | Requests marked safe move to another worker |
| A dead worker fails fast, looks idle, and attracts traffic | It is ejected, and a failover never goes back to it |
| The client cannot tell why a request failed | `X-Wpcp-Reason` says `queue_full`, `deadline`, `no_worker`, ... |

## Features

- **Admission control.** Requests run up to the pool's capacity, wait in a bounded in-memory queue, or are rejected at once when it is full.
- **Deadlines.** Each request carries an absolute deadline to the worker, and each attempt has its own timeout. Requests that can no longer finish in time are dropped from the queue.
- **Placement.** Power of two choices over a pluggable scorer, with an atomic slot reservation on the chosen worker.
- **Health and ejection.** gRPC health probes plus transport errors on real requests. A failing worker is ejected for a period that doubles each time.
- **Failover with a retry budget.** A request the client marks safe moves at most once, to a different worker. A budget caps second attempts so a dying pool does not double its own load.
- **Small worker contract.** One gRPC method. A fake worker is included for local runs and fault injection.

## Quick start

Requires Go 1.27 and, for load, [k6](https://k6.io). Run each line in its own terminal:

```bash
go run ./cmd/fakeworker -grpc 127.0.0.1:50051 -admin 127.0.0.1:8081
go run ./cmd/fakeworker -grpc 127.0.0.1:50052 -admin 127.0.0.1:8082
go run ./cmd/wpcp -workers a=127.0.0.1:50051,b=127.0.0.1:50052
```

Then send traffic:

```bash
curl -X POST -d hello http://127.0.0.1:8080/        # one request
curl http://127.0.0.1:9090/debug/state              # control-plane state
k6 run -e RATE=60 experiments/k6/baseline.js        # about 3x capacity
k6 run experiments/k6/worker-death.js               # stops worker a mid-load
make test                                           # tests with the race detector
```

Request headers:

| Header | Meaning | Default |
|---|---|---|
| `X-Wpcp-Class` | request class | `default` |
| `X-Wpcp-Deadline-Ms` | budget for the whole request | `-deadline` (5s) |
| `X-Wpcp-Attempt-Timeout-Ms` | budget for one attempt, below the deadline | `-attempt-timeout` (3s) |
| `X-Wpcp-Safe-To-Failover` | `true` if the request may start twice | `false` |

## How it works

```
client ── HTTP ──> ingress ──> admission ──> placement ──> gRPC ──> worker
                      │        limit, queue   P2C, reserve    │
                      │                                       │
                      └──── failover once, to another worker ─┘
                                        ▲
                        health: probes + transport errors → ejection
```

Every request passes four decisions: admit, wait, pick a worker, send. All
four read in-memory control-plane state: the admission limit, the requests in
flight per worker, and which workers are live.

```
cmd/wpcp            proxy: flags, wiring, /debug/state
cmd/fakeworker      gRPC worker with fixed latency per class and POST /die
internal/admission  admit, queue or reject; hand out worker slots
internal/pool       per-worker slots and liveness
internal/picker     power of two choices, scorers
internal/queue      bounded queue and drop rules
internal/limiter    admission limit
internal/health     probes, ejection, retry budget
internal/ingress    HTTP side, headers, failover
internal/transport  gRPC client to workers
api/proto           worker contract
experiments/k6      load scripts
```

## Status

The prototype is done: the proxy runs end to end on a laptop against fake
workers. **No experiment has been run yet**, so this README has no result
numbers. They will come from the experiments in
[design §8](docs/design.md#8-experiments), on the setup the design describes.

Next, in order:

1. Measure whether request classes slow each other down on a real worker (experiment 0).
2. Class-aware placement: score a worker by which classes are already running on it. This is the part Envoy's load balancer has no slot for, and whether it helps is measured, not assumed.
3. Ollama adapter and `wpcp calibrate`, then the main comparison against least-in-flight.
4. Gradient limiter, network fault injection, Prometheus and OpenTelemetry, Kubernetes discovery.

Progress is tracked in [docs/roadmap.md](docs/roadmap.md).

## License

[MIT](LICENSE)
