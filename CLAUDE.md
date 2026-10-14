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
- [x] 0.3 Repo + CLAUDE.md
- [x] Phase 1 Foundation: [x] 1.1 compose pg+redis · [x] 1.2 chassis skeleton · [x] 1.3 db+health · [x] 1.4 service template · [x] 1.5 Makefile
- [x] Phase 2 Identity (2.1–2.11): 30 tests pass; live checks in Compose verified (register/409, login cookie flags, JWKS verify, rotation + reuse detection, 401/403, 429)
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

**Next step:** commit Phase 2 (if not yet committed), then Phase 3 (shared auth in chassis).

## Decisions & notes
(Append facts future sessions need that aren't obvious from the code, e.g.
chosen library variants, deviations from the plan, ports changed, known TODOs.)

- `docs/SmartHire_Implementation_Plan.md` is the original; `docs/implementation-plan.md`
  is the copy CLAUDE.md refers to. If the plan is edited, keep them in sync
  (or delete the original).
- Service name stays `identity` (not `auth`): schema/user `identity`/`identity_user`,
  topic `identity.users`, route `/api/identity/*`.
- Step 1.1: Postgres init is `infra/postgres/init.sh` (not `init.sql` as in the plan),
  because passwords come from env vars (`<SERVICE>_DB_PASSWORD` in `.env`). It runs only
  on an empty volume; changing it requires `docker compose down -v`. Compose project
  is `smarthire`; volumes `pgdata`, `redisdata`.
- `.env` is gitignored and created by hand from `.env.example` (placeholders only, committed).
  Keep `.env.example` in sync with the vars used in docker-compose.yml. Claude's
  permissions deny reading/writing `.env*`, so ask the user to edit those files.
- DB URL pattern per service: `postgresql+asyncpg://<svc>_user:<pw>@postgres:5432/smarthire`
  (host `postgres` inside Compose, `localhost` from the host machine).
- Step 1.2: chassis lives in `libs/smarthire_common` (hatchling build, deps: fastapi,
  pydantic, pydantic-settings). Dev venv: `cd libs/smarthire_common && uv venv &&
  uv pip install -e . pytest httpx && .venv/bin/python -m pytest`. Error body shape:
  `{"error": {"code", "message", "request_id", "details?"}}`; `AppError` subclasses set
  `code`/`http_status`. `request_id_var` (ContextVar in logging.py) must be set by
  middleware (gateway/service) per request.
- Step 1.3: chassis now has `db.py` (module-level `init_db/close_db/session_scope/get_session`),
  `health.py` (`health_router`, `register_readiness_check`, `db_ping_check`), `models.py`
  (`Base` with E.2 naming convention, `TimestampMixin`, `new_id()` UUIDv7). `updated_at` is set
  in repositories, not via ORM `onupdate`. CHECK constraints must be given a `name=` (naming
  convention uses it). IMPORTANT: `get_session` commits AFTER the response is sent (FastAPI
  0.143), so use cases that must surface DB errors (409 on unique violation, quota checks)
  must `await session.commit()` themselves before returning.
- Chassis tests: `cd libs/smarthire_common && .venv/bin/python -m pytest` (needs pytest-asyncio,
  aiosqlite, httpx in the venv; uses in-memory SQLite, no Postgres required).
- Step 1.4: `services/_template/` is the copy-me service. To make a service: copy the folder,
  rename in pyproject.toml/Dockerfile (`_template` -> name), set `service_name`, `port`,
  `db_schema` defaults in app/config.py, run `uv lock`, add a compose block (build context is
  the REPO ROOT; copy the block from `services/_template/compose.snippet.yml`). Apps use `create_app()` with uvicorn
  `--factory`. Local dev/test per service: `cd services/<name> && uv sync && uv run pytest`.
  Alembic: `alembic.ini` has `prepend_sys_path = .`; env.py puts `alembic_version` in
  `settings.db_schema` (verified it lands in the service schema, not public).
- Chassis additions in 1.4: `RequestIdMiddleware` (middleware.py), handler for framework
  HTTPExceptions (errors shape is uniform), and `setup_logging` re-routes uvicorn's loggers
  through the JSON handler so access logs carry request_id.
- Dockerfile installs uv via `pip install uv==0.12.23` because pulls from ghcr.io (the official
  uv image) stalled on this network; Docker Hub and PyPI work. Switch back if ghcr works for you.
- Docker builds can hang on network stalls: use `perl -e 'alarm N; exec @ARGV' <cmd>` (macOS has
  no `timeout`).
- `.dockerignore` at repo root keeps `.env`, `.git`, venvs out of the build context.
- Step 1.5: Makefile at repo root: `make help|up|down|ps|logs s=<svc>|migrate s=<svc>|test s=<svc>`.
  `s=common` tests the shared lib (`libs/smarthire_common`). `migrate` needs the service running
  (uses `docker compose exec`); `test` runs on the host with `uv run pytest`. Services need
  `pythonpath = ["."]` in pytest config (already in the template) because `app` isn't installed.
  Both `libs/smarthire_common/uv.lock` and each service's `uv.lock` are committed. Deliberately no
  `down -v` shortcut (destroys data; type it by hand).
- Phase 2 was built in one pass at the user's request (one-off; the step-by-step working agreement
  still applies to later phases).
- Identity layout: api/{auth,users,jwks,deps}.py, domain/{passwords,tokens,errors}.py,
  repositories/{users,refresh_tokens}.py, services/{registration,sessions,login_rate_limiter,admin}.py,
  cli.py (`python -m app.cli create-admin <email>`; ADMIN can't self-register), scripts/generate_dev_keys.py.
- Chassis additions in Phase 2: `redis.py` (init_redis/get_redis/close_redis/redis_ping_check),
  errors `UnauthorizedError` (adds WWW-Authenticate), `ForbiddenError`, `TooManyRequestsError`
  (adds Retry-After); `AppError(headers=...)`.
- JWT: RS256, claims sub, role, tv (token_version), iss=smarthire-identity, aud=smarthire, iat, exp
  (15 min), jti; header kid = RFC 7638 thumbprint of the public key. JWKS at
  `/.well-known/jwks.json`. Key: `make keys` -> `secrets/identity/jwt_private.pem` (gitignored,
  mounted read-only at /run/secrets/identity). Claude's permissions deny access to `secrets/`:
  ask the user to run `make keys`.
- Identity's `get_current_user` (app/api/deps.py) verifies locally AND checks DB status +
  token_version; Phase 3's chassis version can't see the DB, so other services rely on denylist +
  15-min expiry for revocation. Keep the 401 messages/behaviour identical when moving it.
- Refresh cookie: httpOnly, Secure, SameSite=strict, Path=`/auth` (setting `refresh_cookie_path`).
  Phase 4 MUST change it to the gateway's public path (`/api/identity/auth`). Login rate limiting
  uses `request.client.host`; Phase 4 must switch to X-Forwarded-For from the trusted gateway.
- Admin revoke puts `denylist:{jti}` with TTL = access-token lifetime (upper bound; issued tokens
  aren't stored, so exact exp is unknown).
- Tests use Testcontainers (postgres:16, redis:7) with Ryuk disabled; conftest mirrors init.sh
  (identity_user + schema) and builds the schema via the real migrations. test_migrations checks
  downgrade/upgrade and `alembic check` (models == migrations). Test helpers must NOT call
  init_db/close_db while the app runs (they swap the process-wide engine).
- alembic/env.py (identity + template) skips `alembic_version` in autogenerate via
  include_object; alembic.ini has `path_separator = os`.
- Migrations are hand-written (0001 users+profiles, 0002 refresh_tokens) and applied to the dev DB.
