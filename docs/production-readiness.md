# checklist

this checklist is specific to the current `ferrum-proxy` codebase and is ordered by priority.

## tier 1

these are the blockers before trusting the proxy with real production traffic.

- add graceful shutdown and connection draining.
  the server should stop accepting new connections, allow in-flight requests to finish, and then exit cleanly.

- harden header handling.
  review `Host`, `X-Forwarded-For`, `X-Forwarded-Proto`, `Forwarded`, hop-by-hop headers, and connection-specific headers. A proxy should not forward these naively.

- add request and response size limits.
  streaming is in place, but the proxy still needs limits so clients or backends cannot abuse resources indefinitely.

- define timeout policy more completely.
  upstream connect/read timeouts exist already, but production needs a clearer end-to-end policy including idle and slow-client behavior.

- add structured logging.
  replace plain prints with structured request, error, and health-transition logs so operators can correlate failures, backends, status codes, and latency.

- add proper metrics export.
  the built-in `/metrics` text output is useful for debugging, but production should expose a scrape-friendly format such as Prometheus.

## tier 2

these are the main reliability and operational gaps after the first set of blockers.

- add retries carefully.
  only retry safe or idempotent requests, with strict limits and clear timeout boundaries.

- add circuit-breaking or temporary backend ejection.
  health checks help, but the proxy also needs faster protection against flapping or overloaded backends.

- make health policy configurable.
  failure thresholds, recovery thresholds, passive `5xx` handling, and probe rules should not stay hardcoded.

- add config reload or a controlled restart workflow.
  updating routes and backends should have a safe operational story.

- add access control around debug endpoints.
  endpoints such as `/health/backends` and `/metrics` should not be exposed freely in every environment.

- add startup warnings for dead backend pools.
  the proxy does not need to block boot, but it should clearly report when all configured backends are already unavailable.

## tier 3

these items improve correctness and behavior under real-world traffic and failure conditions.

- audit HTTP semantics.
  large uploads, chunked request limits, upstream header stalls, upstream body stalls, partial client disconnects during request buffering, and client disconnects during streamed responses are covered now. upstream disconnects mid-stream still need deeper end-to-end verification. if HTTP/2 downstream support is added later, re-audit h1-upstream to h2-downstream response header forwarding for retained shared header buffers instead of only logical leaks.

- support TLS properly.
  `https://` upstreams are now rejected explicitly. production still needs either real TLS support in this service or a documented deployment requirement that a trusted front proxy terminates TLS and forwards plain HTTP.

- add per-route policy.
  route-level retry and passive failure status overrides exist now, along with route-specific connect/read/body timeouts, health endpoints, and balancing strategies. what remains is deciding how far route policy should go beyond those basics, for example per-route health thresholds or richer balancing behavior.

- improve response classification.
  passive failure classification is no longer limited to one global `5xx` range. routes can now override which upstream statuses count as passive failures, but production rules may still need more protocol-specific nuance.

- add stronger error mapping.
  connect or response-header timeout, refused connection, invalid upstream response, oversized upstream response, no healthy backends, client request body timeout, client request body read failure, and upstream response body timeout now surface as separate proxy error kinds. upstream disconnects mid-stream could still use more explicit reporting.

## tier 4

these items improve scale, maintainability, and operational maturity.

- load test the proxy.
  use realistic concurrency, payload sizes, slow backends, and failure scenarios.

- benchmark and profile it.
  measure latency, throughput, connection counts, and shared-state contention in the health and balancing layers.

- add tracing.
  request IDs and distributed tracing support make production debugging much easier.

- add deployment packaging.
  provide a Dockerfile, runtime config examples, and deployment guidance for systemd or Kubernetes.

- add runbooks.
  Document what `503`, unhealthy backends, timeout spikes, and transition logs mean operationally.
