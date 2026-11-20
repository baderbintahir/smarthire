# SmartHire — Part A checkpoint (`v0.1-core`)

**Date:** 2026-11-04 · **Scope:** Phases 0–14 of `docs/implementation-plan.md` (the core hiring platform;
Part B adds the semantic index and the AI assistant).

## 1. What works

| Area | What it does | Where |
| --- | --- | --- |
| Gateway (Apache APISIX) | Single entry point: routing, JWT verification against Identity's JWKS, token revocation (Redis denylist), rate limits, request IDs, health checks, metrics and tracing | `infra/apisix/` |
| Identity | Registration, login (RS256 JWT + rotating refresh cookie with reuse detection), logout-all, admin suspension | `services/identity` |
| Job | Job CRUD with a state machine; publishing runs a **saga** (Temporal) with compensation | `services/job` |
| Job Enrichment | Skill extraction and job breakdown (saga activities), structured jobs in MongoDB, text search | `services/job_enrichment` |
| Candidate | Profiles, resume upload to MinIO (private bucket, presigned download links), background parsing (Celery) | `services/candidate` |
| Application | Apply with idempotency keys and a per-candidate quota (row lock), recruiter board, stages, notes, decisions; an **ApplicationWorkflow** (Temporal) scores and screens each application | `services/application` |
| Scoring | Scores an application against the job (skills + experience); results via a MongoDB outbox | `services/scoring` |
| Notification | Emails for application and job events; retries, circuit breaker, sweeper | `services/notification` |
| Analytics | Event-fed read model (CQRS); metrics API; full rebuild by replaying Kafka | `services/analytics` |
| BFF (web) | Composed screens (application view, recruiter dashboard) with deadlines, circuit breakers and degraded responses | `services/bff_web` |

**Cross-cutting**
- Database per service.
- Every state change other services care about leaves through a **transactional outbox** to Kafka (Schema Registry, JSON Schema, BACKWARD compatibility).
- Every consumer is **idempotent** (`processed_events`) and has a dead-letter topic.
- Observability:
  - one OpenTelemetry trace follows a request through HTTP, Kafka, Temporal and Celery (Jaeger);
  - RED and messaging metrics in Prometheus;
  - three Grafana dashboards;
  - seven alert rules.
- Retention jobs keep the outbox, `processed_events`, refresh tokens and the email log bounded.

## 2. How it was tested

| Level | What | Result |
| --- | --- | --- |
| Unit and integration (per service, real Postgres/Redis/Mongo/MinIO in Testcontainers; Temporal's time-skipping test server) | `make test s=<service>` | shared library 108, Identity 39, Job 89, Enrichment 12, Candidate 87, Application 82, Scoring 13, Notification 18, Analytics 8, BFF 11, template 3: **all pass** |
| Gateway, against the running stack | `make test-gateway` | 13 of 14 pass; the 14th fails only because this network's DNS stalls on the name `ai_assistant` (see §5) |
| End to end, against the running stack | `make test-events` | **15 pass**. Covers registration → profile, wire format, replay skipped, poison → DLQ, the publishing saga, scoring → screening, emails sent exactly once, analytics rebuild giving identical numbers, BFF degradation, and **one trace linking an application's workflow and its Kafka events** |
| Load (Locust, through the gateway) | 500 concurrent candidates, 5 jobs, ~2,500 applications | below |
| Chaos (during load) | Kafka, Scoring, Notification, its worker and Candidate each stopped for 30 s | below |

### Load and chaos results

The setup was 500 candidates with parsed resumes and 5 published jobs. Each simulated user applies to every job once, with an `Idempotency-Key`, resends 10% of its applies (double clicks), retries failures with the **same** key, and polls its own applications.

| Run | Requests | Throughput | Apply p50 / p95 | Errors (excluding 429) | Accepted = in DB | Duplicates | Converged |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 Application process | 34,302 | 190 req/s | 1.9 s / 7.2 s | 96 (0.28%: gateway timeouts while Application was saturated) | 2,500 = 2,500 | 0 | yes |
| 2 Application replicas **+ chaos** | 66,122 | 220 req/s | 0.67 s / 3.1 s | **0** | 2,500 = 2,500 | 0 | yes: every workflow finished, every application SCORED, no email left queued, outbox and consumer lag back to 0 |

What the runs showed, and what changed:
- **A single Application process saturated one CPU core.** Python runs one thread of Python code at a time, and this process serves the API, its Kafka consumer, its outbox relay and its workflow worker. Application now runs **2 replicas**, which:
  - share the consumer group's partitions;
  - share the outbox (`FOR UPDATE SKIP LOCKED`);
  - poll the same Temporal task queue;
  - are both scraped by Prometheus and load-balanced by APISIX.
- **A real bug in the shared JWKS client.** Right after a service restart, a burst of requests got 503 while the first key fetch was still in flight. Concurrent requests now wait for that fetch.
- **A stale metric.** After a Kafka rebalance, a replica kept reporting lag for partitions it no longer owned. It now forgets them.
- **Rate limits held.** The public job board (120/min per IP) returned 429 to most browse requests, because all simulated users share one IP. That's the limit doing its job.

Reproduce:
1. `make load-setup n=500`
2. `make load-test u=500 t=5m` (and `make chaos` in a second terminal)
3. `make load-verify`

## 3. Reliability review (Step 14.1)

Every network call has a timeout, a retry policy and a defined final failure:
- **HTTP between services:** 2–5 s timeouts. Application's call to Candidate and the BFF's calls have circuit breakers.
- **Gateway → services:** connect 2 s; read 10 s (60 s for uploads, 300 s for the assistant's streams); active and passive health checks.
- **Postgres:** connect 5 s, statement 30 s, pool wait 10 s.
- **Redis:** 2 s.
- **Mongo:** 3 s to connect, 10 s per operation.
- **MinIO:** 3 s to connect, 30 s to read, 3 retries.
- **Schema Registry:** 5 s.
- **Kafka produce:** 10 s.
- **SMTP:** 10 s plus a circuit breaker.
- **Temporal activities:** per-activity timeouts and retry policies, compensation on failure.

A dependency outage reaches clients as **503 with Retry-After**, never as a 500. Postgres, Redis and MinIO now listen on 127.0.0.1 only.

## 4. Screenshots

> Add these before tagging. Save the files in `docs/images/`.

1. **Jaeger:** the trace of one application (search service `gateway`, operation `POST /api/applications`). `docs/images/jaeger-application-trace.png`
2. **Grafana, Service overview** during a load test. `docs/images/grafana-service-overview.png`
3. **Grafana, Async processing** during the chaos run: lag and outbox spikes, then recovery. `docs/images/grafana-async-processing.png`
4. **Temporal UI:** an `app-<id>` workflow history. `docs/images/temporal-application-workflow.png`

## 5. Known limits and next steps

- **One node per service except Application.** With a single node, a health check can't route around a failure; the 10 s read timeout bounds the cost. Scaling another service means adding a container-name node in `infra/apisix/apisix.yaml`.
- **Consumers handle one message at a time.** That gave about 30–60 events/s per group, so a 30 s outage under load takes a few minutes to drain. More partitions or replicas would raise it.
- **Celery workers expose no metrics of their own.** Queue depth comes from RabbitMQ.
- **Local-only security shortcuts:** Redis, Kafka and the Temporal dev server have no authentication, and RabbitMQ uses guest/guest. All are reachable from this machine only.
- **This network's DNS stalls on the name `ai_assistant`.** It takes about 8 s, so the gateway test for an undeployed service times out here. The AI assistant itself arrives in Phase 16.
- **Part B (Phases 15–17):** semantic index (Qdrant), AI assistant (LangGraph, online, offline and degraded paths), final testing and docs.
