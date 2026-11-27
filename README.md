# GPU Job Broker

A small, persistent simulator for submitting GPU jobs, scheduling them onto
fake rented nodes, reporting per-team usage and cost, and replaying the supplied
synthetic workload. The backend uses Python 3.10+ and its standard library;
the dashboard is built with Angular 21 and TypeScript.

## Run locally

The committed Angular production build is served by the Python API, so running
the application does not require Node.js:

```bash
python3 server.py
```

Open [http://127.0.0.1:8000](http://127.0.0.1:8000) for the dashboard. The
server binds to loopback by default and stores state in `broker.sqlite3` beside
the source. Stop it with Ctrl+C; restart it with the same database to restore
the simulated clock, jobs, nodes, and cost history.

Optional environment settings:

```bash
PORT=8000
BROKER_DB=/path/to/broker.sqlite3
BROKER_IDLE_TIMEOUT_SECONDS=300
```

The Angular dashboard shows queued and running jobs, active nodes, per-team
cost, and controls for submitting jobs, cancelling work, and advancing
simulated time.

### Build or develop the Angular frontend

Node.js and npm are needed only to rebuild or develop the frontend. Angular 21
requires Node.js 20.19+, 22.12+, or 24+. For example, with `nvm`:

```bash
nvm install 22
nvm use 22
cd frontend
npm ci
npm run build
```

The production build is emitted to `../static`, which the Python server serves.
For live frontend development, start the backend in one terminal and the
Angular dev server in another:

```bash
# Terminal 1, from the repository root
python3 server.py

# Terminal 2
cd frontend
npm start
```

Open [http://localhost:4200](http://localhost:4200). The Angular dev server
proxies `/api` requests to the Python service on port 8000.

The checked-in production dashboard is served by Python and does not require
Node.js at runtime. The Angular runtime dependency audit reports no
vulnerabilities; the CLI/build-only dependency tree currently reports moderate
transitive advisories that require a future toolchain update to resolve.

## HTTP API

All request and response bodies are JSON.

| Method and path | Behavior |
|---|---|
| `POST /api/jobs` | Submit a job. Required: `team`, `gpus` (1, 2, 4, or 8), `estimated_minutes`, and `client_key` (or `Idempotency-Key` header). Optional `priority`: `low`, `normal`, or `high`. |
| `GET /api/jobs` | List jobs; optional `?status=queued` filter. |
| `GET /api/jobs/{id}` | Get one job. |
| `POST /api/jobs/{id}/cancel` | Cancel a queued or running job. |
| `GET /api/queue` | List queued jobs. |
| `GET /api/nodes` | List active nodes. Use `?active=false` to include terminated and failed nodes. |
| `GET /api/state` | Get simulated time, queue, running jobs, and active nodes. |
| `GET /api/cost?start=...&end=...` | Get GPU-hours and cost by team for an optional ISO-8601 UTC window. |
| `POST /api/advance` | Advance the clock by `{"seconds": 600}` or to an ISO timestamp with `{"to": "..."}`. |

Example:

```bash
curl -X POST http://127.0.0.1:8000/api/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-001' \
  -d '{"team":"vision","gpus":2,"estimated_minutes":20,"priority":"normal"}'
```

Jobs with the same client key return the original job, even if a later request
uses different values. The simulation does not execute user-supplied code.

## Scheduling and accounting decisions

- **Time is simulated.** There are no sleeps or real cloud calls. Advancing the
  clock processes node readiness/failure, job completion, scheduling, retries,
  and idle cleanup at their event times.
- **Fairness:** high, then normal, then low priority. Within the same priority,
  the scheduler round-robins alphabetically among teams and takes each team's
  oldest queued job. It never preempts a running job. The replay's comparison
  policy is submission-order FIFO, with no priority ordering.
- **Placement and isolation:** jobs may share a node only with jobs from the
  same team. A node is permanently assigned to the first team it serves for
  that node's lifetime, including after it becomes idle. The scheduler uses a
  smallest-fitting node (1 GPU: `gpu-s`; 2 or 4: `gpu-m`; 8: `gpu-l`) and packs
  onto a same-team node when enough GPUs are free.
- **Fake provider:** node startup takes a deterministic 4–8 simulated minutes.
  Capacity failures occur in deterministic two-attempt bursts; the broker
  retries after one minute and continues scheduling other work. A deterministic
  3% of launch attempts fail readiness after 15 minutes. A maximum of 10 nodes
  can be starting or ready at once. These deterministic rules make tests and
  replay comparisons repeatable.
- **Lifecycle:** jobs normally complete after their estimated duration. A
  running cancellation releases its GPUs immediately; queued cancellation
  ends without starting. Trace replay uses `actual_minutes` and preserves the
  row's succeeded, failed, or cancelled outcome. Running jobs are not
  interrupted by higher-priority work.
- **Idle cleanup:** an empty ready node is terminated once it has been idle for
  the configured timeout (five minutes by default). Starting nodes are not
  reaped early.
- **Cost:** provider rates are $1.00/hour for `gpu-s`, $4.50/hour for `gpu-m`,
  and $8.50/hour for `gpu-l`. Every node is billed from launch, including
  startup and idle time, with a 10-minute minimum. Node cost (including that
  minimum) is attributed to the node's pinned team. For a cost window that
  covers only part of a node's lifetime, the node's total minimum-adjusted
  charge is prorated by the share of its lifetime inside the window.
  GPU-hours are requested GPUs multiplied by job runtime; they do not include
  idle capacity. A running job is accounted through the current simulated time.
- **Persistence:** SQLite is the source of truth for jobs, node lifecycle,
  idempotency keys, clock, and accounting inputs. SQLite commits state changes
  before returning API results. The fake provider is also stateless across
  restarts, so its deterministic outcomes require no external state.

## Replay and policy comparison

Replay the included synthetic trace and print wait-time percentiles by team,
GPU-hours, cost, and terminal status counts for both policies:

```bash
python3 replay.py
```

Run one policy or set a different idle timeout:

```bash
python3 replay.py --policy fair
python3 replay.py --policy fifo --idle-timeout-minutes 10
```

The replay uses each row's `actual_minutes` as execution duration and its
`outcome` as the terminal result. Wait is submission-to-start; a job that never
starts is measured through its terminal time. Percentiles use nearest-rank
selection. Trace values are checked and reported by line: unsupported requests
from 1 through 8 GPUs are rounded up to the next supported size (for example,
3 to 4), while requests above 8 and other invalid rows are rejected and listed
in the output rather than silently discarded.

### Included trace results

The committed trace has 494 input rows. One request for 3 GPUs (line 88) is
rounded to 4; one request for 16 GPUs (line 393) is rejected because the
provider has no node larger than 8 GPUs. The remaining 493 jobs all reach their
recorded terminal outcome.

| Policy | Team | Wait p50 (min) | p95 (min) | p99 (min) |
|---|---|---:|---:|---:|
| Priority + team round-robin | forecast | 4.62 | 36.30 | 55.67 |
| Priority + team round-robin | nlp | 6.32 | 17.48 | 40.18 |
| Priority + team round-robin | platform | 4.02 | 41.58 | 197.23 |
| Priority + team round-robin | speech | 6.50 | 22.97 | 78.40 |
| Priority + team round-robin | vision | 89.30 | 230.85 | 248.05 |
| Submission-order FIFO | forecast | 5.35 | 43.00 | 44.32 |
| Submission-order FIFO | nlp | 6.75 | 34.72 | 51.00 |
| Submission-order FIFO | platform | 0.00 | 284.68 | 285.68 |
| Submission-order FIFO | speech | 6.35 | 14.92 | 30.00 |
| Submission-order FIFO | vision | 115.18 | 292.68 | 304.20 |

Both policies consume 2,107.37 GPU-hours. Total simulated node cost is
$2,932.51 for priority/team round-robin and $2,915.16 for FIFO (about $17.35
less with FIFO for this trace). Fairness is not a cost optimization: these
results illustrate the tradeoff, especially the improvement in vision's tail
wait under the selected policy and FIFO's very high platform tail wait. These
results use the deterministic provider, default five-minute idle timeout, and
the accounting assumptions above; rerun `python3 replay.py` to reproduce them.

## Tests

```bash
python3 -m unittest discover -s tests -v
```

Backend tests cover idempotent submission, lifecycle and idle reaping,
accounting, team isolation, transient provider capacity failure (including
progress for work that fits existing capacity during retry backoff), restart
persistence, HTTP routes, trace validation, and replay outcomes. Frontend tests
cover dashboard rendering, key user actions, error handling, metrics, and API
request/response mapping.

```bash
python3 -m unittest discover -s tests -v
cd frontend
npm test -- --watch=false --code-coverage
```

Python tests require no package installation. Angular build and tests require
the npm dependencies from `frontend/package-lock.json`.

## Scope and next steps

This is a local assessment simulator, not a production cloud service. It omits
real provider integrations, authentication/authorization, interruptible
instances, multiple API workers sharing a database, and a production-grade
frontend. The HTTP server is intended for local use only. Next steps would
include API authentication, operational metrics, explicit provider adapters,
and richer failure/recovery policy tests.

## Assessment notes

- **AI assistance:** GitHub Copilot in VS Code was used to help implement and
  test the simulator and prepare the documentation. Review the code and be
  ready to explain its assumptions and tradeoffs.
- **Work log desc:** record the actual start/end times and total hands-on time,
  including time spent with AI tools, before submitting. Do not estimate this
  from the repository's commit timestamps alone.
- **Work log:** Around 2 hours overall including copilot time and coding time. Created repo on 23rd November. Worked from 12 to 2pm on 27th November.