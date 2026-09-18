# worker-pool-control-plane — roadmap

This file answers *when* and *how far*. It points to sections of
[design.md](design.md) and never repeats them. Update the checkboxes at the
end of every branch.

## Layers

The proxy is built from the inside out. The order of layers is the order of
work:

```
Control logic  ->  Data path  ->  Worker adapter  ->  Resilience  ->  Telemetry  ->  Discovery
```

| Layer | Packages | Web analogy |
|---|---|---|
| Control logic | `limiter`, `queue`, `picker`, `pool` | business logic |
| Data path | `ingress`, `transport`, `cmd/wpcp` | API layer |
| Worker adapter | `api/proto`, `cmd/fakeworker`, `adapter/ollama`, `wpcp calibrate` | database driver |
| Resilience | `health`, `experiments/faults` | circuit breaker, failover |
| Telemetry | `telemetry`, Prometheus, OTel, Grafana | logging and dashboards |
| Discovery | `discovery` (static, Kubernetes) | service registry |

## Phases

Each phase answers one question and has one exit criterion. Phases are
sequential; branches inside a phase can be parallel.

| # | Phase | Question | Exit criterion | Design |
|---|---|---|---|---|
| 0 | Setup and probe | Does the machine run the toolchain, and is there a signal to chase? | Go, Docker via WSL2 with `.wslconfig` memory raised, k6, proto tooling, repo skeleton, Makefile, CI with race detector. Laptop core-set sibling map recorded. `tc netem` checked inside Docker (fallback: toxiproxy). **Experiment 0a and 0b run and recorded** before any proxy code. `make test` green on the empty skeleton, `docker compose up` runs two Ollama instances on their core sets. | §7, §8, §9 |
| 1 | Core | Is the state correct under concurrency? | `limiter`, `queue`, `picker`, `pool` with unit tests, race detector clean, limiter simulation shows no oscillation. | §3, §4, §5 |
| 2 | Proxy on Compose | Does a request go end to end? | HTTP in, gRPC out to fakeworker, `/debug/state` on the admin port and a log line on every limit change, first k6 script with a baseline. | §0, §2, §7 |
| 3 | Placement | Does the scorer work mechanically? | contention scorer, fakeworker slowdown model, experiment 3 secondary run with a pass criterion. | §5, §8 |
| 4 | Real worker | Is the signal real end to end? **Go/no-go gate.** | `calibrate`, Ollama adapter, experiment 3 main run. | §5, §7, §8 |
| 5 | Resilience | What happens when a worker dies? | health polling, ejection, failover, retry budget, fault scripts, experiments 1, 2a and 2b. | §4, §6, §8 |
| 6 | Observability | Can we see inside? | Prometheus metrics, OTel traces with `placement.pick`, Grafana dashboard, separate Compose profile. | §0 |
| 7 | Kubernetes | Does discovery work on Kubernetes? | EndpointSlice watch, kind manifests. | §0 |
| 8 | Polish | Is it ready to show? | README results table, vLLM adapter if time allows. | §7 |

Experiment 0 sits in phase 0 because it needs no proxy code and answers the
two questions the project depends on. Phase 4 sits before 5 on purpose. If
experiment 3 shows no gain, phases 5 to 7 are still built, but the README
pitch changes. Knowing early is cheaper.

## Checklist

### Phase 0 — Setup and probe
- [ ] `chore/setup`: `go.mod`, Makefile, `.github/workflows`
- [ ] `.wslconfig` memory raised, Docker Desktop verified
- [ ] Go, `make`, k6, protoc/buf on PATH in Git Bash; gcc present for `-race`
- [ ] k6 as a Compose service on the third core set
- [ ] `chore/probe`: core-set sibling map, `tc netem` check, Ollama on two core sets
- [ ] `exp/0-leak-check` (0a)
- [ ] `exp/0-pair-cost` (0b), verdict recorded in `experiments/probe/README.md`

### Phase 1 — Core
- [ ] `feat/limiter`
- [ ] `feat/queue`
- [ ] `feat/picker`
- [ ] `feat/pool`

### Phase 2 — Proxy on Compose
- [ ] `feat/proto` (`timings_ms` field, read only by `calibrate`)
- [ ] `feat/fakeworker`
- [ ] `feat/ingress`
- [ ] `feat/transport`
- [ ] `feat/wpcp-cmd` (wiring, config, Compose file, `/debug/state`, limit-change log)
- [ ] first k6 baseline script

### Phase 3 — Placement
- [ ] `feat/contention-scorer` (includes the slowdown table file package shared with fakeworker and `calibrate`)
- [ ] `feat/fakeworker-slowdown`
- [ ] `exp/3-secondary`

### Phase 4 — Real worker
- [ ] `feat/calibrate`
- [ ] `feat/ollama-adapter`
- [ ] `exp/3-main`
- [ ] go/no-go recorded in design.md §8

### Phase 5 — Resilience
- [ ] `feat/health`
- [ ] `feat/failover`
- [ ] `feat/fault-scripts`
- [ ] `exp/1-worker-death` (with k6 time-series post-processing)
- [ ] `exp/2-overload` (2a queue vs pass-through, 2b gradient vs static limit)

### Phase 6 — Observability
- [ ] `feat/metrics`
- [ ] `feat/tracing`
- [ ] Grafana dashboard and Compose profile

### Phase 7 — Kubernetes
- [ ] `feat/discovery-k8s`
- [ ] kind manifests

### Phase 8 — Polish
- [ ] README results table
- [ ] `feat/vllm-adapter` (optional)

## Branch conventions

- `chore/<name>`: tooling, CI, probes, docs without behaviour change.
- `feat/<package>`: one package or one adapter per branch, with tests.
- `exp/<n>-<name>`: one experiment per branch. Adds the script, the
  `make expN` target, the numbers into `experiments/<name>/README.md`, and
  the one-line verdict into design.md §8. Experiment 0 is the exception: no
  `make expN` target, scripts and numbers under `experiments/probe/`.
- Every PR passes `make test`, which runs with the race detector.
- Every PR names the roadmap phase it belongs to and adds a line under
  `Unreleased` in `CHANGELOG.md`.
