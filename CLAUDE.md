# SmartHire — project context for Claude

## What this is
Microservice backend for a recruitment platform. The owner is a beginner in
backend microservices and is learning while building. Explain before coding,
keep changes small, and add comments on non-obvious lines.

## How to start any session (read this first)
Each phase/step may run in a fresh conversation. Do not expect prior chat context.
1. Read this file, then the **Progress tracker** below to see where we are.
2. Read ONLY the relevant step in `docs/implementation-plan.md`
   (grep for `Step X.Y` or `# Phase N`). Do not re-read the whole plan.
3. For any schema work, read the matching Appendix E section (E.1–E.3 always apply).
   Topics: Appendix A. Task queues: Appendix B. Ports: "Port map" section.
4. Inspect the existing code the step touches before proposing changes.
5. Explain the concept and plan in simple terms, list files to create/change,
   and WAIT for the user's OK before writing code.

## Working agreement
- The user drives; Claude assists. One step at a time, never a whole service at once.
- Keep it minimal: only what the current step needs.
- After generating code: offer a file-by-file walkthrough, curl commands for
  manual testing (including failure cases), and pytest tests.
- Verify against the step's "Done when" checklist and suggest a "Break it" experiment.
- Do not commit unless asked. Use the commit message given in the plan.
- On errors: explain the root cause before fixing, then the smallest fix.
- End each step by updating the Progress tracker below (tick the step, note
  anything the next session must know under "Decisions & notes").
- Quiz with "Check your understanding" questions when the user wants.

## Stack
Python 3.12, FastAPI, uv, PostgreSQL (SQLAlchemy 2.0 async + Alembic),
MongoDB (Motor), Redis, Celery + RabbitMQ, Kafka + Schema Registry
(confluent-kafka), Temporal (temporalio), MinIO, Qdrant, LangGraph,
OpenTelemetry, Jaeger, Prometheus, Grafana, Docker Compose, pytest.

## Services
identity, gateway, job, job_enrichment, candidate, application, scoring,
notification, analytics, semantic_index, ai_assistant.

## Architecture rules (never break these)
1. Database per service: a service never reads another service's tables.
2. Cross-service data comes only from that service's API or its events.
3. Every DB state change that others care about writes an outbox row in the
   same transaction. Never publish to Kafka directly from a request handler.
4. Every Kafka consumer and Celery task is idempotent (processed_events table).
5. Temporal for multi-step workflows that must complete or compensate;
   Kafka for side effects; Celery for fire-and-forget background tasks.
6. Routers handle HTTP only; business rules live in app/domain; DB access in
   app/repositories.
7. JWT (RS256) is verified at the gateway AND in each service. Services check
   role and ownership.
8. Shared code lives only in libs/smarthire_common.
9. Database schemas must match Appendix E of docs/implementation-plan.md:
   UUIDv7 public IDs, timestamptz, text + CHECK for statuses, explicit
   constraint names, rules enforced by constraints, indexes from queries.

## Conventions
- Config via pydantic-settings and environment variables. No secrets in code
  (`.env` and `*.pem` are gitignored; keep `.env.example` updated).
- JSON structured logging with trace_id.
- Every service exposes /health/live, /health/ready, /metrics.
- Tests in services/<name>/tests, run with pytest.
- Service internal layout: app/{main,config}.py, api/, domain/, models/,
  schemas/, repositories/, services/, events/, workflows/, tests/
  (see "Final repository structure" in the plan).
- Inside Docker Compose, services reach each other by name
  (`http://identity:8001`, `postgres:5432`), never `localhost`.
- Each service: own Postgres schema + own DB user + own Alembic version table.
- Commit style: conventional commits (`feat(identity): ...`, `infra: ...`, `chore: ...`).

## Environment (verified 2026-10-08)
macOS (darwin, arm64), Docker Compose v5.5.1, Python 3.12.15, uv 0.12.23, git 2.50.1.

## Progress tracker
Update this as steps finish. Mark `[x]` only when "Done when" is verified.

- [x] 0.1 Install tools
- [x] 0.2 Learn minimum concepts (async, FastAPI, SQLAlchemy/Alembic, Docker, HTTP codes)
- [ ] 0.3 Repo + CLAUDE.md  (files written; first commit pending user OK)
- [ ] Phase 1 Foundation: 1.1 compose pg+redis · 1.2 chassis skeleton · 1.3 db+health · 1.4 service template · 1.5 Makefile
- [ ] Phase 2 Identity (2.1–2.11)
- [ ] Phase 3 Shared auth in chassis
- [ ] Phase 4 Gateway
- [ ] Phase 5 Job service
- [ ] Phase 6 Candidate service
- [ ] Phase 7 Kafka + outbox
- [ ] Phase 8 Application service
- [ ] Phase 9 Temporal + job publishing saga
- [ ] Phase 10 Resume parsing, scoring, application workflow
- [ ] Phase 11 Notification
- [ ] Phase 12 Analytics
- [ ] Phase 13 Observability
- [ ] Phase 14 Reliability + load tests (tag v0.1-core)
- [ ] Phase 15 Semantic index
- [ ] Phase 16 AI assistant
- [ ] Phase 17 Testing, docs, polish (tag v1.0)

**Next step:** 0.3 commit, then Phase 1, Step 1.1.

## Decisions & notes
(Append facts future sessions need that aren't obvious from the code, e.g.
chosen library variants, deviations from the plan, ports changed, known TODOs.)

- `docs/SmartHire_Implementation_Plan.md` is the original; `docs/implementation-plan.md`
  is the copy CLAUDE.md refers to. If the plan is edited, keep them in sync
  (or delete the original).
