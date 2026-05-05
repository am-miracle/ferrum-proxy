# Ferrum Proxy Architecture

## Purpose

`ferrum-proxy` is a high-performance HTTP proxy written in Rust. It sits between clients and a pool of backend services and is responsible for:

- accepting client HTTP requests
- choosing an upstream backend
- forwarding traffic efficiently
- detecting backend failures
- avoiding unhealthy instances until they recover

The system is designed so the fast path for healthy traffic stays simple, while failure handling and health state are managed in the background.

## High-Level View

At runtime, the proxy is made of a small set of cooperating components:

1. `Listener`
   Accepts inbound TCP/HTTP connections from clients.

2. `Request Handler`
   Parses the incoming request, applies proxy rules, and coordinates the rest of the pipeline.

3. `Router`
   Decides which backend pool should receive the request. In a simple deployment this may map all traffic to one pool, but the boundary should exist so path, host, or header-based routing can be added later.

4. `Load Balancer`
   Picks one backend from the selected pool using a policy such as round-robin, least-connections, or weighted selection.

5. `Health Manager`
   Tracks whether each backend is healthy, suspect, or unavailable based on active probes and passive failures observed during live traffic.

6. `Upstream Client`
   Maintains outbound connections to backends and forwards requests/responses with as little overhead as possible.

7. `Observability Layer`
   Emits logs, metrics, and health state changes so operators can understand traffic patterns and failures.

## How the Pieces Fit Together

The request path should look like this:

```text
Client
  -> Listener
  -> Request Handler
  -> Router
  -> Load Balancer
  -> Upstream Client
  -> Backend
  -> Upstream Client
  -> Response back to Client
```

That is the normal success path. The important detail is that `Router`, `Load Balancer`, and `Health Manager` are separate responsibilities even though they participate in one decision.

- The `Router` answers: "Which service or backend pool should handle this request?"
- The `Load Balancer` answers: "Which specific backend in that pool should receive it?"
- The `Health Manager` answers: "Which backends are safe to consider right now?"

The load balancer should never choose from the full backend list directly. It should choose from the subset that the health manager currently marks as available. That keeps health logic out of the hot path decision code and makes behavior easier to reason about.

## Core Data Flow

### 1. Startup

On startup, the proxy should:

- load configuration
- build backend pools
- initialize shared health state
- start the HTTP listener
- start background health-check tasks

Configuration is the source of truth for listeners, routes, backend pools, balancing policy, and health-check intervals.

### 2. Request Processing

For each incoming request:

1. Accept the connection and parse the HTTP request.
2. Match the request to a route or backend pool.
3. Ask the health manager for currently eligible backends.
4. Use the load balancer to pick one backend.
5. Forward the request through the upstream client.
6. Stream the upstream response back to the caller.
7. Record latency, status, and failure/success signals.

This design keeps the proxy efficient because request handling mainly coordinates shared components instead of performing expensive work inline.

### 3. Failure Feedback Loop

Failure detection should happen in two ways:

- `Passive health checks`
  The proxy watches real traffic for timeouts, connection errors, resets, or repeated `5xx` responses.

- `Active health checks`
  A background task periodically probes each backend on a health endpoint or with a lightweight request.

Both signals feed the health manager. If a backend crosses a failure threshold, it is marked unhealthy and removed from load-balancing decisions. If it later passes enough health checks, it can be reinstated.

## Main Runtime Components

## Listener

The listener is responsible for the network edge:

- bind on configured addresses/ports
- accept client connections
- hand off work to async request tasks

It should stay thin. Business logic does not belong here.

## Request Handler

The request handler owns the per-request lifecycle:

- normalize request metadata
- invoke routing
- invoke backend selection
- forward the request
- map upstream errors to client-facing responses

This is the orchestration layer of the proxy.

## Router

The router translates request attributes into a backend pool. Likely matching inputs include:

- host
- path prefix
- HTTP method
- headers

Even if the first version uses one static pool, keeping routing as a dedicated component prevents the project from baking policy directly into the request handler.

## Load Balancer

The load balancer chooses a backend from the eligible set. That component should be policy-driven so different strategies can be swapped without rewriting request forwarding.

Examples:

- round-robin for fairness
- weighted round-robin for uneven capacity
- least-connections for variable request durations

The balancing layer should not own backend health state. It consumes filtered candidates and returns one choice.

## Health Manager

The health manager is the control plane for backend availability.

It should maintain:

- current state for each backend
- recent failure counters
- recovery thresholds
- timestamps for probes and transitions

It serves two audiences:

- the request path, which needs a fast read of healthy candidates
- background check tasks, which need to update backend state safely

Because this state is shared and hot, its concurrency model matters. The implementation should prefer simple, low-contention structures and avoid forcing expensive locks into every request.

## Upstream Client

This is the data plane component that talks to backends. It should handle:

- connection establishment
- connection reuse / pooling
- request forwarding
- response streaming
- timeout enforcement

For performance, this layer must avoid unnecessary buffering when bodies can be streamed. It also needs clear timeout boundaries so slow or dead backends do not tie up resources indefinitely.

## Observability

A proxy without visibility is hard to operate. From early versions, the system should expose:

- structured request logs
- upstream latency metrics
- backend success/failure counters
- health transition events
- active backend counts per pool

This is what makes balancing and failure behavior explainable in production.

## Concurrency Model

Rust is a strong fit here because the proxy has a high-concurrency, IO-heavy workload.

The expected concurrency shape is:

- many concurrent inbound requests
- many outbound upstream connections
- a small set of background health-check tasks
- shared backend state read frequently and written occasionally

The design goal is to keep the request path mostly non-blocking and make shared state reads cheap. Background tasks should do the slower control-plane work so individual requests stay lightweight.

## Separation of Concerns

The architecture is healthiest if the project is split into two broad planes:

- `Data plane`
  The fast path that receives, routes, balances, proxies, and responds to traffic.

- `Control plane`
  The slower path that manages config, health state, backend availability, and telemetry aggregation.

This separation matters because proxy throughput depends on keeping control-plane complexity away from the per-request hot path.

## Suggested Module Direction

As the codebase grows, a clean internal structure would likely look like:

```text
src/
  main.rs
  config/
  server/
  http/
  routing/
  balancing/
  health/
  upstream/
  telemetry/
```

Possible responsibilities:

- `config/`: configuration loading and validation
- `server/`: listener bootstrapping and runtime wiring
- `http/`: request/response proxy mechanics
- `routing/`: route matching and pool selection
- `balancing/`: backend selection policies
- `health/`: backend state tracking and active checks
- `upstream/`: outbound backend client and connection management
- `telemetry/`: logs, metrics, and tracing

## End-to-End Example

Here is how one request should move through the system:

1. A client sends `GET /api/users`.
2. The listener accepts the connection and creates a request task.
3. The router matches `/api` to the `user-service` pool.
4. The health manager returns the currently healthy backends in that pool.
5. The load balancer picks one backend, for example `10.0.0.12:8080`.
6. The upstream client forwards the request to that backend.
7. The backend returns a response.
8. The proxy streams that response back to the client.
9. Metrics and success state are recorded.

If the backend times out instead:

1. The upstream client reports the timeout.
2. The request handler returns an appropriate error to the client or retries if policy allows.
3. The passive failure signal is sent to the health manager.
4. If the backend exceeds the configured threshold, it is removed from future balancing decisions.

## Design Intent

The key architectural idea behind `ferrum-proxy` is simple:

- keep request forwarding fast
- isolate routing, balancing, and health into clear components
- let background health logic continuously shape which backends are eligible

That gives the proxy a clean mental model: route first, balance second, forward efficiently, and continuously learn from backend behavior.
