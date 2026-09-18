# worker-pool-control-plane — design

`worker-pool-control-plane` (binary: `wpcp`) is a Go proxy that sits in front
of a small pool of expensive workers. Under overload it rejects fast instead of
letting everything time out. When a worker dies, in-flight requests that are
safe to move are moved. It keeps the worker's API unchanged: input is the
worker's request, output is the worker's response or a fast 503.

This document is the reference for *what* the proxy is and *why*. Progress and
build order live in [roadmap.md](roadmap.md).

---

## 0. System overview

```
                     k6 (load generator; source of every client-side number)
                              |
                              | HTTP
                              v
 +------------------------------ wpcp -------------------------------+
 |  ingress :8080                                                    |
 |    admission (limiter + bounded queue)                            |
 |      -> placement (P2C over a pluggable Scorer)                   |
 |        -> health / failover                                       |
 |          -> gRPC client                                           |
 |                                                                   |
 |  discovery: static list | Kubernetes EndpointSlice watch          |
 |  admin :9090   /metrics   /healthz   /debug/state                 |
 |  OTel exporter ---------------------------------> Tempo / Jaeger  |
 +----------------+------------------------------+-------------------+
                  | gRPC                         | gRPC
             worker A                        worker B
        (fakeworker | Ollama adapter)   (fakeworker | Ollama adapter)
                  ^                              ^
        fault injection: process stop, network latency / loss / network partition

 Prometheus --scrape--> wpcp:9090/metrics --> Grafana (dashboards only)
```

### Runtime environments

| Environment | Purpose | Runs experiments? |
|---|---|---|
| Docker Compose | development, all experiments, fault injection | yes, this is where every number comes from |
| kind (local Kubernetes) | proves the EndpointSlice discovery path | no, the laptop cannot host both Kubernetes and a real worker |

### Role of each external system

**Prometheus.** Looks inside the proxy while it runs: current limit, queue
depth, in-flight per worker per class, drop reasons. Used to draw dashboards
and to watch the limiter over time. It is never the source of published
numbers, and it is turned off during measured runs so it does not compete for
CPU with the workers.

Prometheus arrives late in the build (roadmap phase 6), but the limiter is
built in phase 1 and exercised from phase 2. So the admin port serves
`/debug/state` from phase 2: a JSON dump of the four pieces of control-plane
state (§3), plus a structured log line whenever the limit changes. That is
enough to debug the limiter without any telemetry stack.

**OpenTelemetry.** Traces one request from ingress to worker. The
`placement.pick` span records the two candidates and their scores, which is
the evidence that the scorer chose what the design says it should choose. Off
during measured runs.

**Kubernetes.** One implementation of the `discovery` interface. The proxy
does not own a CRD or an operator; it only watches EndpointSlices to learn
which worker endpoints exist. It is not a deployment target for published
numbers; the manifests exist to prove the discovery path.

**k6.** Generates load and records client-observed latency, success rate and
goodput. Every client-side number comes from k6 output: the end-of-test
summary for experiments 2 and 3, the time series for experiment 1.
Experiment 0 and `calibrate` measure the worker itself and take their
numbers from the timings the worker reports (§5, §8). No published number
comes from the proxy's own metrics.

---

## 1. Problem and non-goals

### Target worker

The proxy is built for workers with all four of these properties:

1. **Slow and expensive per request.** Hundreds of milliseconds to tens of
   seconds. Rejecting one request early is cheaper than letting ten time out.
2. **Hard concurrency cap.** The worker can run only N requests at once, and
   past N it either queues internally or falls over.
3. **Shared physical resource.** Requests running on the same worker slow each
   other down, and the slowdown depends on *which* requests share the worker.
4. **Can die.** Process crash, host loss, network partition.

The flagship target is self-hosted inference serving (Ollama, vLLM, TGI).
Other pools with the same four properties, such as a headless browser pool, an
ffmpeg pool or an OCR pool, fit too. The list stops there on purpose.

### What the proxy is not

- **Not a message queue.** The queue is bounded and in memory. A request that
  cannot be served soon is rejected, not stored.
- **Not exactly-once.** A request failed over from a dead worker may have
  started on that worker. The client, which knows the request's semantics,
  declares whether a second start is acceptable via
  `X-Wpcp-Safe-To-Failover`; the proxy moves only flagged requests and
  adapters do not override the flag in v1.
- **Not a fairness layer between callers.** Admission is a single decision
  derived from internal state, applied equally to every caller.
- **Not a Kubernetes controller.** No CRD, no operator, no reconciliation.
- **Not highly available itself in v1.** One proxy process, no shared state
  between replicas.
- **Not a general service mesh.** Envoy already has adaptive concurrency,
  pending-request limits, least-request P2C and outlier detection. Envoy's
  load balancer scores each host by a scalar: its in-flight count, or a load
  metric the backend reports through ORCA. Nothing in that path carries the
  class of the incoming request against the classes already running on the
  host, so pairwise interference between classes cannot be expressed. This
  proxy rebuilds the subset a small expensive pool needs and adds that one
  signal. Whether the signal helps is measured in experiment 3, not claimed.

---

## 2. Request lifecycle

Every request passes the same four decisions in order.

```
client --> [1 admit?] --> [2 wait in queue] --> [3 pick worker] --> [4 send, failover on death] --> client
              |               |                     |                        |
             503             503                   503                      503
          queue full     wait too long        no live worker         failover budget spent
       no live worker    deadline expired
                         no live worker
```

1. **Admit, queue or reject.** If no worker is live, the request is rejected
   at once with `no_worker`. Otherwise, if in-flight requests are below the
   current limit, the request goes straight to placement. If not, and the
   queue has room, it waits. If the queue is full, it is rejected
   immediately. There is no separate "over limit" rejection: being over the
   limit always means waiting, and the queue bound is what turns waiting into
   rejection.
2. **Wait in queue.** A queued request is dropped when it has waited longer
   than the configured maximum, when its deadline has already expired, or
   when no worker is live. All return 503. Queue order is FIFO within the
   bounded capacity, with two exceptions, both placed at the head: a request
   re-entering after failover, and an admitted request whose reservation
   failed twice (§3).
3. **Pick a worker.** Placement chooses between two random live workers using
   the configured scorer, and reserves a slot on the chosen one atomically
   (see §3). If no worker is live, 503.
4. **Send and watch.** The request is sent over gRPC with its absolute
   deadline. Each attempt also carries a per-class `attempt_timeout`, shorter
   than the deadline, so that a stalled attempt is noticed while there is
   still budget left to move the request. Failover is triggered by either of:
   - a transport error, or `attempt_timeout` elapsing, on this request itself;
   - ejection of the worker while this request is in flight (a hung worker).
     The old call is cancelled and its slot released.

   If the request is flagged `safe_to_failover` and the retry budget allows,
   it re-enters the queue at the head with its remaining deadline and is
   placed again, respecting the per-worker cap. A request is failed over at
   most once. Otherwise the client receives an error (statuses below).

   A request whose absolute deadline passes is not failed over: its budget is
   gone, and re-entering the queue would only drop it with reason `deadline`.
   The client receives the error directly.

### Headers on the ingress side

| Header | Meaning | Default |
|---|---|---|
| `X-Wpcp-Class` | request class used for scoring and per-class accounting | `default` |
| `X-Wpcp-Deadline-Ms` | budget for the whole request, measured from arrival | configured per class |
| `X-Wpcp-Safe-To-Failover` | `true` if the request may be started twice | `false` |
| `X-Wpcp-Attempt-Timeout-Ms` | budget for one attempt on one worker; must be below the deadline | configured per class |

`safe_to_failover` is declared by the client, which knows the request's
semantics. Adapters do not override it in v1.

Every 503 carries `X-Wpcp-Reason` with one of `queue_full`, `queue_wait`,
`deadline`, `no_worker`, `failover_budget`.

Errors that are not admission decisions keep their own status: a worker
error, a transport error, or a cancellation by ejection (§6) that is not
failed over returns 502; an `attempt_timeout` that is not failed over
returns 504. Neither carries `X-Wpcp-Reason`.

### Deadline propagation

The deadline is fixed at arrival as an absolute instant and is carried in the
request body (§7) so the worker knows the final cutoff. The gRPC deadline of
each attempt is `min(attempt_timeout, remaining budget)`. A request whose
remaining budget is below a configured floor is
dropped in the queue rather than sent, because the worker would not finish in
time. Deadline *feasibility* (predicting whether a specific worker can finish
in time) is deferred to v2.

---

## 3. Control-plane state

The proxy keeps four pieces of state in memory. All four decisions above are
plain conditionals over this state.

| State | Written by | Read by | Guard |
|---|---|---|---|
| current admission limit | limiter, on each completed request | admission, clamped against liveness at read time | atomic |
| in-flight set per worker (count per class) | placement on reserve, transport on completion | placement, telemetry | one mutex per worker |
| liveness per worker (live, ejected until T) | health checker | placement, failover | one mutex per worker |
| slowdown table (class × class → ratio) | loaded at start from `calibrate` output | contention scorer | read-only after load |

Rules:

- This is control-plane state, not a copy of something stored elsewhere. It
  is the only copy.
- **One mutex per worker guards both its in-flight set and its liveness.**
  They are never read or written separately.
- **Reservation is atomic.** Placement scores two candidates, then on the
  chosen worker checks that it is still live and below its cap, and
  increments the in-flight count, all under that worker's mutex. If the check
  fails because another request took the last slot, or the worker was
  ejected, between scoring and reserving, placement picks a new pair once; if
  that also fails, the request goes to the head of the queue, since it has
  already been admitted. The queue therefore supports head insertion from the
  start, not only for failover.
- Every package that touches this state is tested with `go test -race`. A
  data race is a failed build.
- The core packages (`limiter`, `queue`, `picker`, `pool`) import only the
  standard library. Telemetry observes them from outside.

---

## 4. Admission

### Limiter

A gradient limiter in the style of Netflix's concurrency-limits. It keeps a
running estimate of the no-load latency and compares it to recent latency:

```
ratio     = rtt_noload[class] / rtt_recent[class]     (per completed request)
gradient  = 1                        if rtt_recent < tolerance × rtt_noload
          = clamp(ratio, 0.5, 1.0)   otherwise
new_limit = limit × gradient + queue_headroom
limit     = clamp(smooth(limit, new_limit), 1, sum_of_configured_caps)
effective = clamp(limit, live_worker_count, sum_of_live_worker_caps)   (at read time)
```

When latency grows past the tolerance band, the gradient falls below one and
the limit shrinks. When latency is back within the band, the limit grows by
the headroom term until it finds the next slowdown. `queue_headroom` is
`min(queue_length, headroom_max)`, so the limit only grows while there is
demand waiting.

The limiter clamps its own value to `[1, sum_of_configured_caps]`. Without
that ceiling, a full queue and a gradient of 1 would add headroom on every
completion and the stored limit would grow without bound, and a later
gradient below 1 would need many steps to bring it back under the pool's
capacity. The configured ceiling needs no liveness, so the limiter package
never sees it. Admission clamps again when it reads, to
`[live_worker_count, sum_of_live_worker_caps]`: ejecting a worker shrinks
both, so the effective limit can never sit below what the remaining pool can
run, nor above it.

**What RTT is.** RTT is measured from the moment the request leaves the proxy
to the moment the response arrives: worker service time plus network. Time
spent in the proxy's own queue is excluded. Including it would make the
limiter react to a backlog the proxy itself created, and would put the
limiter and the queue drop rules in a feedback loop with each other.

**Baseline estimator.** `rtt_noload[class]` is the 10th percentile of RTT
over a long sliding window (on the order of minutes), re-seeded on a fixed
period so that a laptop that throttles down and back up does not keep a stale
baseline forever. The minimum is not used: one unusually fast sample would
pin the baseline low for the whole window, and every later sample would read
as overload.

One limit of this estimator: under sustained full load every sample is an
RTT measured while sharing a worker, so the 10th percentile drifts up to the
shared RTT, the gradient reads 1, and the limit rests at the ceiling. The
re-seed does not fix that, because the new window sees the same samples.
Netflix's limiter handles it by periodically lowering the limit to probe the
baseline; v1 does not, and experiment 2b is read with this in mind.

Mixed classes: the baseline is kept per class, each sample's ratio is
computed against its own class baseline, and the ratios feed a single
gradient. Each completed request updates one exponentially weighted gradient
with the ratio of its own class. This keeps a slow class from being read as
overload.

**What the limiter can and cannot see on this setup.** The proxy holds a hard
per-worker cap equal to the worker's own parallelism (§5, §7), so a worker
never queues internally and RTT does not grow with offered load beyond the
cap. The only latency signal left is interference: a request sharing a
worker with another one is slower than a request running alone. So on a pool
with a known hard cap, the limiter's range is `[live_worker_count,
sum_of_live_worker_caps]`, and its job is narrow: back off from full concurrency
when interference pushes RTT past the tolerance band, and back off further
when a worker degrades (thermal throttling, a noisy neighbour). With two
workers at cap 2 that range is three values.

The limiter earns its place with workers that *do* queue internally, where
the proxy cannot know the true capacity: vLLM with `max_num_seqs`, a headless
browser pool, an ffmpeg pool without a fixed slot count. For those, RTT grows
with load and the gradient has a real signal. This is stated here so that
experiment 2 is read correctly: 2a measures the bounded queue and deadline
drop, 2b measures the limiter on its own, and a flat result in 2b on the
Ollama setup is the expected outcome, not a failure.

The hard part is not the formula but keeping the loop from oscillating; the
limiter package carries a simulation test that drives it with a synthetic
worker, once with internal queueing and once with a hard cap, and asserts the
limit settles in both.

### Queue

Bounded, in memory, FIFO. Three drop rules, checked when a slot opens and by
a periodic sweep:

- waited longer than `max_queue_wait`
- deadline already expired, or remaining budget below the send floor
- no worker is live (the sweep drops the whole queue with `no_worker`, so a
  dead pool fails fast instead of after `max_queue_wait`)

Bounded is a design constraint, not a tuning choice. An unbounded queue turns
an overload into a latency collapse, and a persisted queue turns the proxy
into a message broker, which it is not. A pass-through configuration (very
large capacity, `max_queue_wait` and deadline drop disabled) exists only so
experiment 2a has a baseline; it is not a supported mode.

### Why goodput

The primary metric is goodput: requests completed *within their deadline* per
second, as seen by the client. A response that arrives after the client gave
up is wasted worker time and counts against goodput, not for it. Raw request
rate is not reported.

---

## 5. Placement

### Power of two choices

Placement picks two live workers uniformly at random, scores both, and sends
to the lower score. P2C is used instead of scoring the whole pool because it
avoids herd behaviour when many requests arrive at once, and because the
score is cheap enough that a third candidate would not change the outcome.

### Scorer interface

```go
type Scorer interface {
    // Score returns the expected cost of placing a request of class c on
    // worker w given w's current in-flight set. Lower is better.
    Score(w WorkerState, c Class) float64
}
```

Two implementations ship in v1:

| Scorer | Score | Uses the slowdown table? |
|---|---|---|
| `leastinflight` | `in_flight(w) + 1` | no |
| `contention` | `(in_flight(w) + 1) × (slowdown[r][c] + slowdown[c][r])` | yes |

where `r` is the class of the request already running on `w`. When `w` is
empty there is no `r`, and the sum is defined as 2 (both ratios taken as 1),
so an empty worker scores `1 × 2 = 2` and a worker with one running request
scores at least `2 × 2 = 4`; the contention scorer then agrees with
least-in-flight whenever the table has nothing to add.

Both terms matter: `slowdown[r][c]` is how much the running request slows
the new one, `slowdown[c][r]` is how much the new request slows the running
one. A scorer
that only looks at the first term would happily place a heavy request next to
a light one that then misses its deadline.

Because the score sums both directions, only the pair total matters. With
cap 2, placing a `prefill` request compares `2 × slowdown[p][p]` (a worker
already running `prefill`) against `slowdown[d][p] + slowdown[p][d]` (a
worker running `decode`). Whether the two cross terms differ from each other
changes nothing; whether the cross total differs from the same-class totals
is the whole signal.

Per-class base latency does not appear in the score. Both candidates are
scored for the same request, so a factor that depends only on the request's
class is identical on both sides and cancels.

`leastinflight` is the special case of `contention` where every slowdown
ratio is 1. Keeping both behind one interface is what makes experiment 3 a
fair comparison: the limiter, queue and health logic are identical, only the
scorer changes.

The v1 per-worker cap is exactly 2, so at most one other request is running
when a placement is scored and the table is consulted directly.
Configuration rejects any other value when the contention scorer is
selected. A cap above 2 is a v2 change and would require a rule for
combining several running classes.

### Slowdown table

`slowdown[a][b]` is the factor by which a request of class `b` slows down when
a request of class `a` is already running on the same worker.

The table is produced only by `wpcp calibrate` and never written by hand:

1. Run class A alone on one worker, repeatedly, record its latency.
2. Run class A while a loop of class B requests keeps the worker's second slot
   busy for the whole duration of A's run, record A's latency.
3. Emit `slowdown[b][a] = latency(A with B) / latency(A alone)` for every
   ordered pair, including each class with itself.

B must be a continuous loop, not a single request: a single B finishes
partway through A and the measurement blends interference with no
interference.

Every request carries a random nonce at the start of its prompt: `calibrate`
takes one payload template per class and substitutes the nonce into it.
Ollama reuses the context of an earlier prompt with the same prefix, and a
repeated prompt would report a `prompt_eval_duration` near zero. Experiment
0 follows the same rule.

Where the worker reports server-side timings, `calibrate` uses them instead
of end-to-end latency, so HTTP and adapter overhead do not blur the ratio.
Ollama returns `prompt_eval_duration` (prefill phase) and `eval_duration`
(decode phase) in every response. The table entry for a class is computed on
the **sum** of the two phases of that class's request; the per-phase ratios
are reported alongside for diagnosis only. A class is not mapped to a single
phase, because every request has both phases and the table is indexed by
class, not by phase. End-to-end latency is the fallback for adapters that
report nothing finer.

Two preconditions, otherwise calibrate measures queueing rather than
interference:

- The proxy's per-worker cap must equal the worker's own parallelism (for
  Ollama, `OLLAMA_NUM_PARALLEL`), so the worker never queues internally.
- That parallelism must be 2, the v1 cap, so two classes actually share the
  worker.

### Table file

The table is a JSON file with `classes` (ordered list of class names),
`slowdown` (square matrix, row `a`, column `b`), optional `phases` (the same
matrix per reported phase, diagnosis only), and `source` (`calibrate` or
`fakeworker`). `calibrate` writes it; the contention scorer and fakeworker
read it through one small package under `internal/picker`. The format is
fixed here so that phase 3 (fakeworker slowdown model) and phase 4
(`calibrate`) agree on it.

### Request classes for the flagship target

Two classes chosen because they stress different resources, so that the
cost of sharing a worker is likely to differ from pair to pair:

| Class | Shape | Bound by |
|---|---|---|
| `prefill` | long prompt, short output | compute |
| `decode` | short prompt, long output | memory bandwidth |

If every pair turns out to slow down by roughly the same factor, the table
degenerates into an in-flight count and the contention scorer cannot beat
least-in-flight. That outcome is a valid result of experiment 3 and is
reported as such.

---

## 6. Health and failover

### Health check

Each worker is polled with the standard gRPC health protocol on a fixed
interval. A worker is also observed passively: consecutive transport errors
on real requests (connection refused, connection reset, `UNAVAILABLE`) count
as failures. A request that exceeds its own deadline is **not** a passive
failure: under overload, exceeded deadlines are normal, and counting them
would eject every worker in the pool and remove all capacity exactly when it
is most needed. A hung worker is still caught, by the active health check
and by each attempt's `attempt_timeout`, which triggers failover for that
request while it still has budget (§2).

### Outlier ejection

After `eject_after` consecutive failures a worker is marked ejected for
`eject_for`, doubling on each further ejection up to a ceiling. An ejected
worker receives no new requests, and every request still in flight on it is
cancelled and handed to failover (§2). It is re-admitted after one successful
health check once the ejection period ends. Ejection is a per-worker
decision; there is no global circuit that stops all traffic.

### Failover

A request reaches failover from either trigger in §2. It is moved only if:

- it is flagged `safe_to_failover`;
- it has not been failed over before;
- its remaining budget is above the send floor (§4);
- the retry budget has room.

The **retry budget** is `max(floor, fraction × in_flight)`: at most that many
requests may be in their second attempt at any time. The absolute floor
exists because with a pool of two workers and a handful of in-flight
requests, a percentage alone rounds to zero and no failover would ever
happen. The fraction exists so that a dying pool does not double its own
load. When the budget is spent, further failovers are rejected with
`failover_budget`.

A moved request re-enters the queue at the head with its remaining deadline
and is placed like any other request, including the per-worker cap. It is
not sent to a worker that has no free slot. It counts against the
retry budget from the moment failover is granted until its second attempt
completes or is dropped, including the time it waits at the head of the
queue.

Vocabulary used throughout: *failover*, *retry budget*, *fault injection*,
*ejection*. These are the only terms for these mechanisms.

---

## 7. Worker contract

### Protocol

Workers speak gRPC. The proto is small on purpose:

```proto
service Worker {
  rpc Execute(ExecuteRequest) returns (ExecuteResponse);
}

message ExecuteRequest {
  string class       = 1;   // copied from X-Wpcp-Class
  int64  deadline_ms = 2;   // absolute, unix millis; final cutoff for the whole request
  bytes  payload     = 3;   // opaque to the proxy
}

message ExecuteResponse {
  int32  status     = 1;
  bytes  payload    = 2;
  map<string, int64> timings_ms = 3;   // server-side timings, read only by calibrate
}
```

Health uses `grpc.health.v1.Health`. The proxy never inspects `payload` or
`timings_ms`. `timings_ms` is filled by the adapter from whatever the worker
reports (Ollama: `prompt_eval`, `eval`; fakeworker: `service`) and is read
only by `calibrate`.

### fakeworker

A gRPC worker used for development, unit-level experiments and fault
injection. Configuration:

- base latency per class
- concurrency cap
- a slowdown model: either a table in the same format `calibrate` emits, or a
  table with random per-pair noise (see experiment 3, secondary run)
- a `/die` admin endpoint so fault scripts can stop it cleanly, in addition to
  external process stop and network faults

### Ollama adapter

A thin process that exposes the `Worker` gRPC service and forwards to one
Ollama HTTP endpoint. Deployment rules that make the numbers meaningful:

- Each Ollama instance is pinned to its own **core set** with
  `--cpuset-cpus`, and its thread count equals the number of vCPUs in the
  core set; experiment 0a shows whether the physical core count is the
  better setting. Interference is then confined *inside* a worker; workers
  do not slow each other through the CPU scheduler.
- `OLLAMA_NUM_PARALLEL` equals the proxy's per-worker cap, which is 2 in v1.
- The adapter maps a failed connection to Ollama (refused, reset, probe
  timeout) to gRPC `UNAVAILABLE`, so the proxy sees a transport error (§6)
  even though the adapter process is alive. An HTTP 5xx from Ollama is
  returned as `INTERNAL` and reaches the client as a worker error (502).
- Model is small (about 1B parameters, 4-bit) so two core sets fit in the
  laptop's memory alongside Docker.
- k6 and the proxy run on a third core set, outside both workers'. k6 runs
  as a Compose service so that it can be pinned; a host install is only for
  authoring scripts.
- The adapter's gRPC health service reports `SERVING` only when a light probe
  to its Ollama endpoint returns within a timeout. Without that, a hung Ollama
  behind a live adapter would keep reporting `SERVING` and never be ejected.
- Prometheus and Tempo are down during measured runs.

**WSL2 caveat.** Docker on Windows runs inside a WSL2 virtual machine.
`--cpuset-cpus` pins a container to *vCPUs* of that VM; Hyper-V may schedule a
vCPU on any physical core, and two vCPUs may be hyperthread siblings of one
core. So: core sets are chosen as whole sibling pairs from the laptop's
topology, every measurement is repeated several times and reported with its
spread, and results are treated as indicative rather than exact. Experiment 0
exists to measure how much isolation the core sets actually give on this
setup; it is not assumed.

Shared memory bandwidth between core sets is not controllable either and is
covered by the same experiment.

**Process stop.** A worker is stopped with `docker stop -t 0` (immediate
termination) or, for fakeworker, `POST /die`. Fault scripts use no other
mechanism, so the vocabulary check stays clean on `experiments/`.

**Network faults.** Latency, loss and network partition are injected with `tc netem`
inside the worker containers, which needs `NET_ADMIN` and the `sch_netem`
kernel module in the WSL2 kernel. If the module is missing, toxiproxy is used
instead as a user-space proxy between `wpcp` and each worker; it supports the
same three faults without kernel help. The two paths differ in wiring: with
`tc netem` the proxy talks to the workers directly, with toxiproxy the
discovery list points at the toxiproxy listeners instead of the workers. Which
path is in use, and the matching discovery configuration, are recorded in
`experiments/faults/README.md`.

### vLLM adapter

Same shape as the Ollama adapter, targeting vLLM's OpenAI-compatible HTTP API.
Built last and may slip to v1.1; it does not gate any experiment.

---

## 8. Experiments

Every experiment is a k6 script under `experiments/k6/` and a Makefile target
`make expN`, except experiment 0, which runs before any proxy code exists and
lives under `experiments/probe/`. A run that does not meet the pass criterion
is reported as a failed run, not tuned until it passes.

Each experiment's numbers live in `experiments/<name>/README.md`
(`experiments/probe/README.md` for experiment 0); this section records only
the one-line verdict and a link. The README results table in phase 8 copies
from there.

| # | Question | Setup | Metric | Pass |
|---|---|---|---|---|
| 0a | Does a core set actually isolate workers on this hardware? | A alone on core set 1; A on set 1 with B looping on set 2; A and B on set 1 | latency of A in each case, repeated runs | same-set slowdown clearly larger than cross-set slowdown; if they are close, the worker cannot demonstrate interference on this machine and the candidate is switched |
| 0b | Does the cost of sharing a worker differ between class pairs? | call Ollama directly, random prompt prefix (§5): `prefill` alone and `decode` alone, then each with a loop of `prefill` in the second slot, then each with a loop of `decode` | the four table entries on the sum of both phases (§5), the three pair totals `p+p`, `p+d`, `d+d`, and their run-to-run spread; per-phase ratios for diagnosis | the three pair totals differ by more than run-to-run spread; if they are alike, experiment 3 cannot win and the pitch changes now, before any proxy code |
| 1 | What happens when a worker dies mid-load? | steady load; at T stop one worker (run once with process stop of the Ollama container, not the adapter; once with network partition) | success rate over the run, time until goodput recovers, from k6 time series | success rate of `safe_to_failover` requests stays near 100%; recovery bounded by health interval plus ejection threshold |
| 2a | Does admission control protect goodput under overload? | single class, offered load at 3× capacity; bounded queue with deadline drop vs pass-through (experiment-only configuration: very large queue capacity, `max_queue_wait` and deadline drop disabled, cap still enforced) | goodput, client timeouts, p99 of admitted requests | with admission, goodput does not collapse and admitted p99 stays under the class deadline; without it, timeouts dominate |
| 2b | Does the gradient limiter add anything over a fixed limit on this pool? | mixed classes with the strongest interference pair from calibrate, queue fixed; gradient limiter vs static limit = sum of caps | goodput, p99 per class | gradient beats static by a margin outside noise, or the result is "no gain on a pool with a known hard cap", which §4 predicts and which is reported as such |
| 3 | Does the contention scorer beat least-in-flight? | mixed `prefill` and `decode` load, limiter fixed, scorer is the only variable | p99 per class, goodput | contention scorer improves p99 or goodput by a margin outside run-to-run noise; otherwise the result is "no measurable gain", and the pitch changes accordingly |

### Experiment 0 runs first

Both parts of experiment 0 need only Ollama, Docker and a small script. They
answer the two questions the whole project depends on, so they run in phase 0,
before the proxy exists. The core-set sibling map of the laptop and the raw
results are recorded in `experiments/probe/README.md`.

### Experiment 1 needs time series

Recovery time is the gap between the moment a worker stops and the moment
goodput is back to its pre-fault level. That cannot be read from an
end-of-test summary. k6 writes its time series with `--out csv`, and a small
script under `experiments/k6/` computes success rate over time and the
recovery gap.

### Experiment 3 has two runs

**Main run: real worker plus calibrated table.** Ollama workers on core sets,
table from `wpcp calibrate`. These are the only numbers that go into the
README. This run proves the signal exists in a real worker.

**Secondary run: fakeworker plus perturbed table.** fakeworker slows requests
by a table T. The scorer is given T with random per-pair noise strong enough
to change the ranking of some pairs. Uniform scaling (every entry times the
same constant) does not change any ranking and proves nothing. This run
proves the scorer is not fragile when the table is imprecise. Pass: with the
perturbed table the contention scorer still beats least-in-flight outside
run-to-run noise, and its gap to the run with the exact table is within that
noise. Losing to least-in-flight is a failed run.

A fakeworker-only experiment 3, where the scorer reads the same table the
worker uses to slow itself, is circular. It is never presented as the main
result.

### The real question behind experiment 3

Does the cost of sharing a worker differ between class pairs? Because the
score sums both directions (§5), the contention scorer only has information
that least-in-flight lacks when the cross-pair total
`slowdown[prefill][decode] + slowdown[decode][prefill]` differs from the
same-class totals `2 × slowdown[prefill][prefill]` and
`2 × slowdown[decode][decode]`. Whether the two cross terms differ from each
other does not matter. Experiment 0b gives the first look at that spread;
`calibrate` reports it again on the real deployment before the load test
runs.

---

## 9. Package layout

```
worker-pool-control-plane/
├── cmd/
│   ├── wpcp/            # main: config, wiring, `calibrate` subcommand
│   └── fakeworker/      # development worker with slowdown model
├── internal/
│   ├── limiter/         # gradient limiter                   core
│   ├── queue/           # bounded queue, drop rules           core
│   ├── picker/          # P2C, Scorer, contention scorer      core
│   ├── pool/            # per-worker state, liveness          core
│   ├── ingress/         # HTTP server, headers, deadlines
│   ├── transport/       # gRPC client to workers
│   ├── adapter/
│   │   └── ollama/      # gRPC Worker -> Ollama HTTP
│   ├── health/          # health polling, ejection, failover
│   ├── discovery/       # static list, Kubernetes EndpointSlice
│   └── telemetry/       # Prometheus and OTel, wraps core from outside
├── api/proto/           # worker.proto and generated code
├── deploy/
│   ├── compose/         # docker-compose and profiles
│   └── k8s/             # kind manifests
├── experiments/
│   ├── probe/           # experiment 0: core-set map, scripts, raw results
│   ├── k6/              # exp1 .. exp3 scripts, time-series post-processing
│   └── faults/          # process stop and network fault scripts
├── docs/
│   ├── design.md
│   └── roadmap.md
├── .github/workflows/   # lint, test with race detector
├── CHANGELOG.md
├── Makefile
├── go.mod
└── README.md
```

Rules:

- Packages marked *core* import only the standard library. They have no
  knowledge of HTTP, gRPC, Prometheus or OpenTelemetry.
- `telemetry` takes references to core objects and exposes their state; it
  never changes their behaviour.
- `internal/` is used so nothing outside this repository can import the
  packages. This is a service, not a library.
