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
identity, job, job_enrichment, candidate, application, scoring, notification,
analytics, semantic_index, ai_assistant, bff_web. Gateway: Apache APISIX (config in infra/apisix).

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
10. The gateway (Apache APISIX) holds no business logic: routing, auth, revocation
   checks, rate limits, request IDs only. Data from several services is composed in
   bff_web or served from event-fed replicas, never joined in the gateway.

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
  NO `Co-Authored-By` trailers or any Claude attribution: the user (GitHub `baderbintahir`,
  repo-local email badarbintahir@gmail.com) is the sole author.

## Environment (verified 2026-10-08)
macOS (darwin, arm64), Docker Compose v5.5.1, Python 3.12.15, uv 0.12.23, git 2.50.1.

## Progress tracker
Update this as steps finish. Mark `[x]` only when "Done when" is verified.

- [x] 0.1 Install tools
- [x] 0.2 Learn minimum concepts (async, FastAPI, SQLAlchemy/Alembic, Docker, HTTP codes)
- [x] 0.3 Repo + CLAUDE.md
- [x] Phase 1 Foundation: [x] 1.1 compose pg+redis · [x] 1.2 chassis skeleton · [x] 1.3 db+health · [x] 1.4 service template · [x] 1.5 Makefile
- [x] Phase 2 Identity (2.1–2.11): 30 tests pass; live checks in Compose verified (register/409, login cookie flags, JWKS verify, rotation + reuse detection, 401/403, 429)
- [x] Phase 3 Shared auth in chassis: [x] 3.1 JWKS client + verify_token + deps · [x] 3.2 ensure_owner
- [x] Phase 4 Gateway (APISIX, revision 2): [x] 4.1 spike · [x] 4.2 routes/request IDs/XFF · [x] 4.3 JWT + revocation at edge · [x] 4.4 rate limiting · [x] 4.5 SSE · [x] 4.6 metrics hooks
- [x] Phase 5 Job service: [x] 5.1 scaffold + models · [x] 5.2 state machine · [x] 5.3 CRUD · [x] 5.4 publish stub
- [x] Phase 6 Candidate service: [x] 6.1 scaffold + models + MinIO · [x] 6.2 profile endpoints · [x] 6.3 resume upload (60 tests + 12 gateway tests pass; live: files in bucket, rows in DB, 415/413 at service, 413 at gateway, anonymous GET 403)
- [x] Phase 7 Kafka + outbox: [x] 7.1 kafka/registry/ui · [x] 7.2 envelope + schemas · [x] 7.3 outbox writer · [x] 7.4 relay · [x] 7.5 idempotent consumer · [x] 7.6 UserRegistered -> profile · [x] 7.7 job events (chassis 83, identity 37, job 79, candidate 67, gateway 12, events e2e 6; break-it verified)
- [x] Phase 8 Application service: [x] 8.1 models · [x] 8.2 job replica consumer · [x] 8.3 submit (idempotency, quota lock) · [x] 8.4 views · [x] 8.5 stage/notes/decision/withdraw (67 tests; gateway 13, events e2e 7; live race: 20 applies, quota 10 -> exactly 10)
- [x] Phase 9 Temporal + job publishing saga: [x] 9.1 temporal + mongo · [x] 9.2 hello world · [x] 9.3 job_enrichment · [x] 9.4 JobPublishingWorkflow · [x] 9.5 reliable start (job 89, enrichment 12, chassis 86, events e2e 8; break-it verified)
- [x] Phase 10 Resume parsing, scoring, application workflow: [x] 10.1 rabbitmq + celery · [x] 10.2 parse_resume · [x] 10.3 scoring · [x] 10.4 ApplicationWorkflow · [x] 10.5 job closure (candidate 86, application 78, scoring 13, events e2e 10; break-its verified)
- [x] Phase 11 Notification: [x] 11.1 mailpit + tables · [x] 11.2 consumers -> tasks · [x] 11.3 sending, retries, circuit breaker (notification 17, identity 38, gateway 14, events e2e 11; break-it verified)
- [x] Phase 12 Analytics: [x] 12.1 projections · [x] 12.2 consumers · [x] 12.3 metrics API · [x] 12.4 rebuild (analytics 8, events e2e 13: metrics follow actions, rebuild identical)
- [ ] Phase 12B BFF (API composition)
- [ ] Phase 13 Observability
- [ ] Phase 14 Reliability + load tests (tag v0.1-core)
- [ ] Phase 15 Semantic index
- [ ] Phase 16 AI assistant
- [ ] Phase 17 Testing, docs, polish (tag v1.0)

**Next step:** Phase 12B (BFF) if not done, then Phase 13 (Observability).

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
- Phase 3: `smarthire_common/auth.py`. Startup: `init_auth(JWKSClient(settings.jwks_url), issuer=..., audience=...)`
  (+ `close_auth()` on shutdown); needs `init_redis` too (denylist). Routes: `Depends(get_current_user)`
  -> `CurrentUser(user_id, role, jti, claims)`, `Depends(require_role("RECRUITER"))`, `ensure_owner(owner_id, user)`.
  JWKSClient: 10-min cache, refetch on unknown kid throttled to once/30 s, keeps old keys if Identity
  is down, 503 if it never got any key. Redis down during denylist check -> 503 (fail CLOSED, user's
  decision). Settings `jwks_url` (default http://identity:8001/.well-known/jwks.json), `jwt_issuer`,
  `jwt_audience` live in BaseServiceSettings.
- Identity uses `StaticKeyProvider` (its own public key, no HTTP to itself) and layers DB checks
  (status, token_version) on top via `app/api/deps.py:get_current_user`; its `require_role` passes
  `user_dependency=` to the chassis one. Identity's old `decode_access_token` was removed (single
  verifier lives in the chassis).
- Identity's REFRESH_COOKIE_PATH is `/api/identity/auth` in Compose (public path via gateway). Direct
  calls to identity:8001/auth/* won't get the cookie sent back; use the gateway.
- PLAN REVISION 2 (2026-10-14, supervisor review): `docs/implementation-plan.md` is authoritative;
  `docs/SmartHire_Implementation_Plan.md` is the pre-review original (kept unchanged for reference).
  Rationale for all changes is in Appendix F of the plan.
  - Gateway = Apache APISIX (standalone YAML in infra/apisix), NOT a custom service. A hand-written
    FastAPI gateway was built in the first Step 4.1 attempt and deleted (user's decision); its route
    table/path-rewrite rules carry over to APISIX (Phase 4.2 table).
  - Revocation enforced at the gateway: APISIX openid-connect (bearer_only, use_jwks; Identity must add
    /.well-known/openid-configuration) + custom Lua plugin checking Redis `denylist:{jti}` and NEW
    `revoked_before:{user_id}` (Identity writes it on logout-all and admin suspension; chassis
    verify_token must check it too). Redis down -> 503 at gateway and services.
  - API composition in new `services/bff_web` (Phase 12B, port 8011, no DB), forwarding the user's token.
  - Phase 13 adds APISIX telemetry and Kafka-vs-Temporal observability; Phase 16 adds online/offline/
    degraded paths for the AI Assistant.
- Step 4.1 APISIX spike (pinned `apache/apisix:3.19.0-debian`, standalone YAML): config in
  `infra/apisix/config.yaml` (node config; restart to apply) and `infra/apisix/apisix.yaml` (routes;
  hot-reloaded, must end with `#END`). Edit apisix.yaml IN PLACE (same inode): it is a single-file bind
  mount, so tools that replace the file aren't seen until `docker compose restart gateway`. Custom Lua
  plugins: `infra/apisix/plugins/<name>.lua` (mounted at /usr/local/apisix/custom/apisix/plugins,
  `extra_lua_path`), and must be listed under `plugins:` in config.yaml (that list REPLACES defaults).
  `stream_plugins: []` avoids a 3.19.0 startup error from the default stream `syslog` plugin.
  Verified on 3.19.0: proxy-rewrite (regex_uri), request-id (reuses client X-Request-ID, else generates;
  Identity logs the same id), hot reload, custom Lua plugin loading, limit-count policy=redis (key
  `plugin-limit-count:v1:/routes/<id>:...`), openid-connect bearer_only+use_jwks via Identity's NEW
  `/.well-known/openid-configuration` (setting `jwks_public_url`).
  openid-connect GOTCHAS (must use in 4.3): audience is NOT checked by default -> set
  `client_id: smarthire` + `claim_validator.audience.match_with_client_id: true` (wrong aud then returns
  403, not 401: plugin's choice); default `iat_slack` is 120 s and also applies to exp -> set
  `iat_slack: 10`; pin `claim_validator.issuer.valid_issuers: [smarthire-identity]`. Errors are nginx
  HTML unless the plugin has `_meta.error_response` (our JSON error shape works there). TLS warning for
  http discovery is expected inside Compose.
- Phase 4 (4.2-4.6) was built in one pass at the user's request (one-off, like Phase 2).
- Gateway layout: `infra/apisix/apisix.yaml` has `upstreams` (one per service, by Compose name),
  `plugin_configs` `authenticated` (openid-connect -> smarthire-denylist -> limit-count 300/min/user)
  and `public` (limit-count 120/min/IP), routes (identity-login 30/min/IP; identity-public: register,
  refresh, logout, .well-known/*; identity-protected: rest of /api/identity/*; jobs GET public
  (service does optional auth), all other /api/<svc>/* authenticated), JSON 404 catch-all (mocking),
  and `global_rules` (request-id, prometheus, smarthire-retry-after). Adding a service = add its
  upstream + route; nothing else.
- Custom plugins: `smarthire-denylist.lua` (priority 2590, after openid-connect 2599; checks
  `denylist:{jti}` and `revoked_before:{sub}` with MGET; 503 if Redis down; registers var
  `smarthire_user_id` for per-user limits) and `smarthire-retry-after.lua` (429 -> Retry-After =
  ceil(X-RateLimit-Reset)). Lua plugin CODE changes need `docker compose restart gateway`
  (apisix.yaml route changes hot-reload; config.yaml changes need restart too).
- Error shapes at the gateway: nginx error_page snippet in config.yaml -> JSON 502 (refused), 503
  (name doesn't resolve / not deployed), 504 (read timeout); plugins use `_meta.error_response`.
  Wrong-audience tokens get 403 (openid-connect's choice), body code INVALID_TOKEN.
- Revocation keys: `revoked_before:{user_id}` = unix second; tokens with iat <= value are rejected
  (chassis AND Lua; "<=" because iat has 1 s resolution: re-login in the same second needs a retry).
  Identity writes it (TTL access ttl + 60 s) on logout-all and NEW admin endpoint
  `POST /users/{id}/suspend` (status SUSPENDED, refresh tokens revoked, token_version bumped; Redis
  written BEFORE commit = safe direction). Redis write failure -> 503.
- Rate limits use `allow_degradation: true` (Redis down -> no counting, requests pass); revocation
  stays fail-closed (503) on protected routes.
- Network: Compose default network subnet 172.30.0.0/24; gateway fixed at 172.30.0.10. Services set
  `FORWARDED_ALLOW_IPS=172.30.0.10` so uvicorn takes the client IP from X-Forwarded-For only from the
  gateway (template compose.snippet updated). Changing the network needs `docker compose down` + up.
- Gateway healthcheck: bash /dev/tcp to the APISIX status API 127.0.0.1:7085/status/ready (no curl in
  image). Prometheus metrics at gateway:9091 inside the network only (not published).
- SSE streams through APISIX 3.19.0 unbuffered with no extra config (verified with a fake upstream);
  assistant upstream read timeout 300 s.
- Gateway tests: `make test-gateway` (needs `make up`): tests/gateway/test_gateway.py, 9 tests, prove
  edge rejection by checking a unique X-Request-ID never reaches Identity's logs. Login route limit is
  30/min/IP: running many logins quickly (tests + manual curls) can hit it; wait a minute.
- MIGRATION NAMING RULE (bug found in 5.1): in hand-written migrations, wrap explicit CHECK names in
  `op.f("ck_<table>_<name>")`. A plain `name="ck_users_role"` gets the naming convention applied on top
  -> `ck_users_ck_users_role` (happened in Identity 0001; fixed by Identity 0003 rename, since 0001 had
  already run). pk/fk/uq/ix names are unaffected (their convention doesn't use the given name).
  `alembic check` does NOT compare CHECK constraints, so every service has a test comparing the
  schema's EXACT constraint/index names to Appendix E (`smarthire_common.testing.schema_object_names`).
- Shared test helpers: `smarthire_common/testing.py` (`service_postgres(service)` = Postgres with the
  service's own user + schema like init.sh; `redis_container()`; `alembic_config(...)`; `truncate`,
  `flush_redis`, `run`, `schema_object_names`). Imports testcontainers/alembic lazily: services keep
  `testcontainers[postgres,redis]` in their dev group. Identity's conftest uses it.
- After a failed commit/rollback in async SQLAlchemy, ORM objects are stale and touching an attribute
  raises MissingGreenlet: copy ids into plain variables first (tests and service code alike).
- Step 5.1: `services/job` (port 8002, schema/user `job`/`job_user`), models `Job`, `JobStage`
  (app/models/job.py) per E.5, migration 0001 applied to the dev DB. No endpoints yet; auth wiring
  (init_auth + JWKSClient + init_redis for the denylist) arrives in 5.3.
- Phase 5 (5.2-5.4) was built in one pass at the user's request (one-off, like Phases 2 and 4).
- Job state machine: `app/domain/job.py` (`ALLOWED_TRANSITIONS` table = the plan's arrows; EDITABLE =
  DRAFT/FAILED/READY; editing READY -> PROCESSING (re-processing, Phase 9); publish only from DRAFT/FAILED
  (READY -> 409); close only from READY; CLOSED final). Stage rules: 1-20 stages, unique ignoring case,
  position = list index. Skills normalized lowercase/unique. Domain errors are chassis AppError subclasses
  (409 INVALID_JOB_TRANSITION / JOB_NOT_EDITABLE / VERSION_CONFLICT, 422 INVALID_STAGES).
- Job API: `POST /jobs` (RECRUITER, 201 DRAFT), `GET /jobs` (public board, READY only, keyset cursor
  pagination `?limit&cursor`, filter employment_type), `GET /jobs?mine=true[&status=]` (recruiter's own,
  any status), `GET /jobs/{id}`, `PUT /jobs/{id}` (full replace + `version` optimistic lock, stages
  replaced), `POST /jobs/{id}/publish` (202, stays PROCESSING until Phase 9), `POST /jobs/{id}/close`.
  Visibility: READY/CLOSED visible to anyone; DRAFT/PROCESSING/FAILED only to the owner, 404 for everyone
  else (ids of unpublished jobs can't be probed); writes on someone else's published job -> 403.
  `eligibility_rules` is a strict Pydantic model (only `min_experience_years` for now; Application uses
  it in Phase 8; unknown keys -> 422). TODO markers in app/services/jobs.py for Phase 7.7 (JobUpdated,
  JobClosed outbox) and Phase 9.5 (JobPublishRequested).
- Chassis additions: `auth.get_optional_user` (no header -> None, bad token -> 401, never anonymous) and
  `testing.TokenFactory` (test RSA key; `key_provider` for `create_app(settings, key_provider=...)`,
  `headers(role=..., user_id=...)`). Services' create_app takes `key_provider` for tests; production uses
  `JWKSClient(settings.jwks_url)` + `init_redis` (denylist). Job's readiness checks database + redis.
- GET /api/jobs is public at the gateway, so Job's own token check is what rejects revoked tokens there
  (verified live: rule 7 in action).
- Phase 6 (6.1-6.3) was built in one pass at the user's request (one-off, like Phases 2, 4 and 5).
- MinIO image: `pgsty/minio` (community fork, pinned RELEASE.2026-08-04T00-00-00Z). The official
  `minio/minio` images are no longer published on Docker Hub (pulls fail) and quay.io was unreachable.
  The image ships `mc`, so `minio-init` reuses it. Testcontainers' MinioContainer default image is the
  dead one: tests pass the pgsty image explicitly.
- MinIO setup: `infra/minio/init.sh` (one-shot `minio-init` container, idempotent, runs every `up`):
  private bucket `resumes` (anonymous access none) + MinIO user for Candidate with policy
  `infra/minio/candidate-resumes-policy.json` (Get/Put/Delete objects + List on `resumes` only). Root
  creds are only for init and the console (http://localhost:9001). Env vars: MINIO_ROOT_USER,
  MINIO_ROOT_PASSWORD, CANDIDATE_S3_ACCESS_KEY, CANDIDATE_S3_SECRET_KEY (values are made up by the user;
  init.sh creates the user from them). Volume `miniodata`.
  Phase 10's parser runs inside Candidate, so it reuses the same user (needs GetObject: already granted).
- Candidate service: port 8004, schema/user `candidate`/`candidate_user`, models `Candidate`, `Resume`
  (E.6; `candidates.id` = Identity user id, no FK). Settings `s3_endpoint` (minio:9000 in Compose),
  `s3_access_key`/`s3_secret_key` (REQUIRED, so alembic env.py needs them too: tests pass them via
  `alembic_config(extra_env=...)`), `resumes_bucket`, `max_resume_bytes` (5 MB). Readiness: database,
  redis, storage (bucket_exists). Storage helper: `app/repositories/files.py` (sync minio client wrapped
  in asyncio.to_thread). `app.state.settings` holds settings for routes.
- Candidate API: `GET/PUT /candidates/me` (CANDIDATE; PUT = upsert via INSERT ... ON CONFLICT, full
  replace, no version column: only the owner edits), `GET /candidates/me/resumes` (newest first, max 50),
  `POST /candidates/me/resumes` (multipart field `file`; 202 UPLOADED), `GET /candidates/{id}` (self,
  RECRUITER, ADMIN; other candidates 403 for any id). TODO(Phase 8): recruiters only for their own
  applicants. Until Phase 7 creates profiles from UserRegistered, upload without a profile is 404
  CANDIDATE_NOT_FOUND ("Create your profile first").
- Resume upload rules: type from magic bytes (`%PDF-`, or a zip containing `[Content_Types].xml` +
  `word/document.xml`), never the client's Content-Type/name -> 415 UNSUPPORTED_FILE_TYPE (also empty
  files); > 5 MB -> 413 FILE_TOO_LARGE; file_name sanitized (display only); object key
  `{candidate_id}/{resume_id}` in bucket `resumes`. Order: file to MinIO first, then one transaction
  (SELECT candidate FOR UPDATE -> deactivate old -> insert new active -> commit); on DB failure the object
  is deleted (best effort; a crash in between leaves an orphan file, harmless; cleanup job is a TODO).
  Old resumes are kept (inactive). Without the row lock, concurrent uploads hit uq_resumes_one_active
  (verified by removing the lock: IntegrityError); with it all succeed. TODO markers for Phase 7.7
  (ResumeUploaded outbox) and Phase 10.2 (enqueue parse_resume).
- Gateway: `client-control` plugin added (config.yaml plugin list); candidates route has
  `max_body_size: 6291456` (6 MB) -> JSON 413 PAYLOAD_TOO_LARGE (error_page in config.yaml). Gateway test
  for "service not deployed -> 503" now uses /api/applications.
- Step 7.1 Kafka: `apache/kafka:4.3.1` (official image, KRaft combined broker+controller, node id 1, fixed
  CLUSTER_ID in compose; data in volume `kafkadata`). Listeners: containers use `kafka:19092` (INTERNAL),
  the host uses `localhost:9092` (EXTERNAL); CONTROLLER :9093 internal only. `auto.create.topics.enable=false`:
  topics come from `infra/kafka/create-topics.sh` (one-shot `kafka-init`, idempotent, lists once then creates
  missing): the 8 Appendix A topics + a `.dlq` twin each, 3 partitions, RF 1. Producing to an unknown topic
  times out (verified). Adding a topic = edit that script. Heap 512 MB.
- Schema Registry: `confluentinc/cp-schema-registry:8.0.8` at localhost:8081 (`schema-registry:8081` inside),
  stores schemas in topic `_schemas`, global compatibility BACKWARD (set explicitly). Kafka UI:
  `kafbat/kafka-ui:v1.5.0` (maintained fork of the abandoned provectuslabs image) at http://localhost:8085,
  cluster `smarthire-local`. Kafka, Schema Registry and Kafka UI are published on 127.0.0.1 only (no auth).
- Host-side Kafka experiments: `uv run --no-project --with confluent-kafka python <script>` (bootstrap
  localhost:9092).
- Phase 7 (7.2-7.7) was built in one pass at the user's request (one-off, like Phases 2, 4, 5, 6).
- EVENTS CHASSIS (libs/smarthire_common):
  - `event_catalog.py` (STDLIB ONLY, imported by schema-init): `EVENT_TOPICS` (event type -> topic,
    Appendix A), `subject_for` (`<topic>-<EventType>`, TopicRecordNameStrategy, because topics carry several
    types), `envelope_schema`/`schema_str` (payload schema wrapped in the envelope; `title` = event type =
    record name). DEVIATION from the plan: schemas live in the chassis package
    (`smarthire_common/schemas/events/<EventType>.json`, payload only, draft-07, closed
    `additionalProperties: false`, with `examples`), not repo-root `schemas/`, so every service image ships them.
  - `events.py`: `EventEnvelope` (event_id UUIDv7, event_type, schema_version, occurred_at, producer,
    trace_context, payload) and `new_event(type, producer=, payload=)` (unknown types fail at the producer).
  - `outbox.py`: `OutboxMixin` (services declare `class OutboxEvent(OutboxMixin, Base)` in app/models/messaging.py
    so THEIR migrations own the table), `add_outbox_event(session, event, key=)` (caller's transaction; payload
    column = the whole envelope), `OutboxRelay` (200 ms poll, batch 100, FOR UPDATE SKIP LOCKED, marks only
    Kafka-confirmed rows; at-least-once), `KafkaSender` (JSONSerializer, `auto.register.schemas=False`,
    idempotent producer, message.timeout 10 s, flush 15 s), `start_outbox_relay(settings)`.
  - `kafka.py`: `ProcessedEventMixin`, `EventConsumer` (group id = consumer name; manual offsets committed AFTER
    the DB commit; `process()` = INSERT processed_events ON CONFLICT DO NOTHING + handler in one transaction;
    handler errors retried 3x with backoff then `<topic>.dlq` with x-error/x-consumer/x-original-* headers;
    undecodable/invalid messages -> DLQ at once; other errors (registry/DB/DLQ down) -> seek back, retry in 5 s),
    `start_consumer(settings, name=, topics=, handlers=)`. `background.stop_tasks(*tasks)` for shutdown.
  - Chassis code writes outbox/processed_events through LIGHTWEIGHT `table()` constructs, never models in the
    shared Base metadata (else every service's `alembic check` would expect those tables).
  - Settings (BaseServiceSettings): `kafka_bootstrap_servers` (kafka:19092), `schema_registry_url`
    (http://schema-registry:8081), `kafka_enabled` (True; ALL service tests set False and read events from the
    outbox table). Kafka is deliberately NOT a readiness check (the outbox lets the API work without it).
  - Relay/consumers run as asyncio tasks in the service's lifespan (not separate worker containers).
- schema-init: one-shot `python:3.12-slim` container running `infra/schema-registry/register.py` (stdlib, mounts
  the chassis at /chassis) on every `up`; fails on HTTP 409 (incompatible). Host: `make schemas`. Verified on the
  real registry: optional field added = allowed; required field added / field removed / type changed = rejected.
  To change a schema incompatibly in DEV only: delete the subject (and `?permanent=true`) and re-register.
- Event producers so far: Identity `UserRegistered` (register transaction, key user_id, payload user_id + role);
  Job `JobUpdated` (full snapshot on PUT) and `JobClosed` (key job_id); Candidate `ResumeUploaded` (key
  candidate_id). Creating/publishing a job emits nothing yet (JobPublishRequested/JobReady are Phase 9).
  Job: `set_status_if` now bumps `version` too, so every change (edit, publish, close) has a higher version and
  consumers can drop stale events (Phase 8 source_version guard).
- Consumer so far: Candidate group `candidate` on identity.users (`app/events/handlers.py`): CANDIDATE ->
  `repo.create_empty` (ON CONFLICT DO NOTHING keeps a profile already saved via PUT). PUT /candidates/me
  still works as a fallback if the event is late.
- Migrations: identity 0004_outbox, job 0002_outbox, candidate 0002_outbox_and_processed_events (names pk_outbox,
  uq_outbox_event_id, ix_outbox_unpublished, pk_processed_events; exact-name tests updated).
- `make test-events` (needs `make up`): tests/events/test_events.py, 6 tests (profile from event, wire format,
  recruiter ignored, replay skipped via log "skipped duplicate", poison -> DLQ, job events in order).
  Uses `docker compose exec postgres psql` for DB checks. Each run leaves test users/jobs in the dev DB.
- Known limits (documented, accepted for now): a row that can't be serialized stays unpublished and is retried
  forever (logged); with SEVERAL relay replicas, SKIP LOCKED can reorder one key's events across batches
  (consumers must use version guards); outbox/processed_events cleanup jobs (E.14) not built yet.
- Phase 8 (8.1-8.5) was built in one pass at the user's request (one-off, like Phases 2, 4-7).
- Application service: port 8005, schema/user `application`/`application_user`, models in app/models/application.py
  exactly per E.7 (+ outbox, processed_events in migration 0001). Chassis `AppError` gained `details=` (rendered
  as `error.details`).
- DECISIONS (plan was silent/ambiguous):
  - "Application limit" = per-CANDIDATE cap on ACTIVE applications (SUBMITTED/SCREENING/IN_PROGRESS), global
    setting `max_active_applications` (default 10) enforced with the `application_quotas` row lock. The job's own
    `application_limit` is stored in the replica but NOT enforced (plan exercise: "global default -> per-job").
  - `candidate_name` + eligibility data come from ONE synchronous call to Candidate (`GET /candidates/me` with the
    user's own Authorization header forwarded; app/repositories/candidate_api.py, 3 s timeout, Candidate down -> 503,
    no profile -> 422 PROFILE_INCOMPLETE). Applying also requires a full name and an active resume.
  - Recruiter board keeps the plan path `GET /jobs/{job_id}/applications`: APISIX route `job-applications`
    (`uri: /api/jobs/*` + `vars` regex `^/api/jobs/[^/]+/applications$`, priority 10, authenticated) sends it to
    Application, not Job. Board uses limit/offset (bounded per job); "my applications" uses keyset cursors.
  - Lifecycle: SUBMITTED (current_stage "SUBMITTED") -> SCREENING (Phase 10 workflow) -> IN_PROGRESS (recruiter
    PATCH stage to any of the job's stages, forward or back) -> HIRED/REJECTED (decision) | WITHDRAWN (candidate) |
    CLOSED (Phase 10.5). Recruiter edits need `version` (409 VERSION_CONFLICT); all changes are conditional UPDATEs
    `WHERE status IN ACTIVE` so the quota is released exactly once (decision vs withdraw race tested).
  - Visibility: applications are private; anyone but the applicant and the job owner (job_replica.recruiter_id)
    gets 404. Notes are recruiter-only (candidate sees `notes: null`). Audit entries for STAGE_CHANGED, NOTE_ADDED
    (note id only), DECISION_MADE. Withdraw emits NO event yet (TODO; not in the catalog).
- Apply flow (app/services/applications.py `apply` -> `submit`): Idempotency-Key header REQUIRED; Redis
  `idem:{user_id}:{key}` (app/services/idempotency.py): SET NX pending marker (60 s TTL) -> 409
  IDEMPOTENCY_IN_PROGRESS for a parallel duplicate; done -> stored 202 body replayed (24 h); same key + other
  job_id -> 422 IDEMPOTENCY_KEY_REUSED; failures release the key. Then job open (replica READY, before deadline)
  -> Candidate facts -> eligibility (min_experience_years) -> one transaction: quota row INSERT ON CONFLICT DO
  NOTHING + SELECT FOR UPDATE, check, insert application, history, quota+1, ApplicationSubmitted outbox -> commit;
  uq_applications_candidate_job -> 409 ALREADY_APPLIED. Verified: without FOR UPDATE, 15 of 20 applies passed a
  limit of 3.
- Job replica consumer (group `application`, topic jobs.lifecycle, app/events/handlers.py): JobUpdated/JobReady
  upsert with `WHERE source_version < EXCLUDED.source_version`; JobClosed updates status only for a known job with
  a newer version. TODO(Phase 10.5): close open applications on JobClosed.
- Events e2e (`make test-events`, 7 tests) simulates JobReady by producing it with the registered schema
  (`produce_event` helper, imports the stdlib-only event_catalog via sys.path). Dev DB note: a live race check left
  10 test applications whose stage-history rows were deleted by a cleanup slip (test data only).
- Phase 9 was built in one pass at the user's request (one-off, like Phases 2, 4-8).
- Temporal: `temporalio/temporal:1.9.1` DEV SERVER (`server start-dev`, SQLite at /home/temporal/temporal.db in
  volume `temporaldata`), gRPC 127.0.0.1:7233, UI http://localhost:8088 (container 8233). DEVIATION: the plan said
  own Postgres or auto-setup; `temporalio/auto-setup` is unmaintained (last tag mid-2026) and the server+Postgres
  setup needs schema tooling + new DB credentials. Production = temporalio/server on Postgres/Cassandra + UI.
- MongoDB: `mongo:8.0.34`, 127.0.0.1:27017, NO auth locally (like Redis/Kafka), volume `mongodata`. Chassis
  `smarthire_common/mongo.py` (init_mongo/get_db/close_mongo/mongo_ping_check) uses PyMongo's AsyncMongoClient
  (DEVIATION: plan says Motor, which is deprecated). Chassis extras: `smarthire-common[temporal]`, `[mongo]`.
- Chassis `temporal.py`: `connect(settings)` (cached client, retries every 5 s until Temporal is up),
  `start_worker(settings, task_queue=, workflows=, activities=)` (background task, reconnects). Settings
  `temporal_address` (temporal:7233), `temporal_namespace`, `temporal_enabled` (tests set False).
  Chassis `kafka.RetryLater`: a handler raises it when a DEPENDENCY is down -> no retries/DLQ, seek back + retry.
- `examples/temporal_hello/hello.py` (Step 9.2): worker/start demo; verified kill-worker-mid-timer -> resumes.
- job_enrichment: port 8003 (127.0.0.1 only; internal read API, NOT routed by the gateway, no auth yet:
  TODO Phase 10 service-to-service auth), Mongo db `enrichment`, collection `structured_jobs` (_id
  "<job_id>:<version>", indexes ix_job_version + text index tx_search). Activities on `enrichment-tq`
  (app/workflows/activities.py, called BY NAME): extract_skills (dictionary + aliases in app/domain/skills.py,
  required skills first, + YAKE keywords), breakdown_jd (headings, then keyword phrases; app/domain/breakdown.py),
  store_structured_job (replace_one upsert), delete_structured_job (compensation). API: GET /jobs/{id}/structured
  (latest version), GET /search/jobs?q= (text search, newest version per job). Readiness: mongo. Worker runs in the
  app's lifespan (stopping the container stops the worker).
- Job publishing saga (services/job/app/workflows/publishing.py, workflow name "JobPublishingWorkflow", task queue
  `job-tq`, id `publish-{job_id}-{version}`): set_processing (confirms PROCESSING at that version, returns content)
  -> extract_skills -> breakdown_jd -> store_structured_job -> mark_ready. Per step: 5 attempts (1,2,4,8 s back-off),
  start_to_close 20 s, schedule_to_close 60 s (so a missing worker fails the step in a minute). Failure: compensate
  completed steps in reverse (delete_structured_job, registered only AFTER store succeeds; 10 min budget), then
  mark_failed. Stale version (ApplicationError type "StaleJobVersion", non-retryable) -> "SUPERSEDED", no changes.
  Job activities (app/workflows/activities.py): mark_ready/mark_failed = `jobs_repo.finish_publishing` (PROCESSING ->
  READY/FAILED only at that version, version+1, published_at set once) + JobReady (full snapshot) / JobPublishFailed
  in the same transaction; "already done" (status matches at version+1) counts as success (idempotent).
- Reliable start (Step 9.5): publish_job and edits of READY jobs write `JobPublishRequested` {job_id, version}
  (NEW event type, topic jobs.commands, schema registered by schema-init) with PROCESSING in one transaction.
  Job consumer group `job-publisher` (app/events/handlers.py `build_handlers(settings)`) starts the workflow with
  REJECT_DUPLICATE; WorkflowAlreadyStartedError ignored; Temporal RPC errors -> RetryLater. Job migration 0003 adds
  processed_events. Old jobs left PROCESSING by earlier phases have no workflow (re-publish is impossible from
  PROCESSING; set them FAILED/DRAFT by SQL if needed).
- Tests: job's tests/test_publishing_workflow.py uses Temporal's time-skipping test server
  (WorkflowEnvironment.start_time_skipping(), downloaded on first use) with the REAL workflow + Job activities on
  Postgres and FAKE enrichment activities; module-level `pytest.mark.asyncio(loop_scope="module")`.
  Enrichment tests use a Mongo testcontainer (mongo:8.0.34) and ActivityEnvironment. e2e `published_job()` helper
  now uses the real saga. Live break-it verified: enrichment stopped -> FAILED after ~61 s (ActivityTaskTimedOut),
  no structured doc; restarted + re-published -> READY in ~2 s.
- Phase 10 was built in one pass at the user's request (with Phase 11; separate commits).
- Infra: RabbitMQ `rabbitmq:4.1.8-management` (127.0.0.1:5672, UI 127.0.0.1:15672 guest/guest; guest allowed
  from other containers via `-rabbit loopback_users []`, local dev only), volume `rabbitmqdata`. MongoDB now runs
  as a SINGLE-NODE REPLICA SET rs0 (healthcheck initiates it) because Mongo transactions need one; clients use
  `?directConnection=true`. ALL service ports are now published on 127.0.0.1 only (clients go through the gateway;
  /internal/* endpoints must not be reachable from the LAN).
- Chassis: `skills.py` (the skills dictionary MOVED here from job_enrichment: shared by Enrichment, Candidate,
  Scoring), `celery.py` (extra `celery`: `make_celery(name, broker, default_queue=)` with acks_late,
  reject_on_worker_lost, prefetch 1, json, no result backend; `run_async(coro)` = one event loop per worker
  process), `mongo_outbox.py` (Mongo `outbox` collection: `add_outbox_event(event, key=, session=)` inside a Mongo
  transaction; `MongoOutboxRelay` reuses OutboxRelay's loop + KafkaSender; no SKIP LOCKED), `mongo.get_client()`,
  `testing.mongo_replica_set()`. Setting `celery_broker_url` (amqp://guest:guest@rabbitmq:5672//).
- Candidate parsing: worker container `candidate-worker` (same image; `celery -A app.tasks.celery_app worker -Q
  resumes`). Upload -> ResumeUploaded (outbox) -> Candidate's OWN consumer (topic candidates.profile) enqueues
  `parse_resume` by name (`send_task`, app/tasks/names.py; RabbitMQ down -> RetryLater). Task (app/tasks/parsing.py)
  autoretries transient errors (backoff, max 5) and calls app/services/parsing.py: only UPLOADED resumes; download
  from MinIO; pypdf / python-docx text; app/domain/resume_parser.py (shared skills dictionary, experience = max of
  "N years" claims and merged date ranges, education levels); conditional UPDATE to PARSED + ResumeParsed outbox in
  one transaction; unreadable file -> FAILED with parse_error (not retried). celery_app.py builds Settings() at
  import: candidate tests set placeholder env vars in conftest. Internal API `GET /internal/candidates/{id}/resume`
  (profile experience + active resume status/parsed_data) for Scoring; no auth (network only; TODO Phase 14).
- Enrichment now also stores `required_skills` (normalized recruiter must-haves) in structured_jobs.
- Scoring service (port 8006, 127.0.0.1, Mongo db `scoring`, internal `GET /scores/{application_id}`): activity
  `score_application(application_id, candidate_id, job_id, min_experience_years)` on scoring-tq (4th arg is an
  addition to the plan's signature: the job's minimum comes from Application's replica). Calls Candidate and
  Enrichment internal APIs (app/repositories/peers.py). Resume UPLOADED -> retryable error; no resume / FAILED /
  no structured job -> non-retryable ApplicationError type "CannotScore". Formula (app/domain/scoring.py): 70 x
  share of required skills + 10 x share of other job skills + 20 x min(1, years/min) (no min: 20 if years known,
  else 10); breakdown matched/missing/experience_match. Score doc (_id application_id) + CandidateScored in ONE
  Mongo transaction; event only on first scoring. MongoOutboxRelay publishes it.
- ApplicationWorkflow (application/app/workflows/processing.py, application-tq, id `app-{application_id}`):
  init_stages (LOCAL activity) -> score_application (scoring-tq; retries 2 s..1 min, schedule_to_close 10 min) ->
  record_score_and_advance (score or PENDING_MANUAL; SUBMITTED -> SCREENING + history by SYSTEM_ACTOR uuid 0 +
  ApplicationStageChanged; if a recruiter already moved it only the score is saved; inactive -> nothing). Signals
  `withdraw`, `job_closed` stop it (result WITHDRAWN / JOB_CLOSED). Started by Application's consumer from its own
  ApplicationSubmitted (topics now jobs.lifecycle + applications.lifecycle; REJECT_DUPLICATE; RetryLater).
  Withdraw signals after commit (best effort, app/services/workflows.py `signal`, 3 s timeout).
- JobClosed (10.5): replica CLOSED, then SUBMITTED applications -> CLOSED (+ history, quota -1), commit, then
  `job_closed` signals. Applications already SCREENING/IN_PROGRESS stay open (the recruiter finishes them).
- Temporal test server note: it can't fast-forward while an activity waits in a queue with NO worker (even
  env.sleep times out), so the 10-minute scoring deadline is checked by config and live, not in a test.
- First start of the new consumer groups replayed old ApplicationSubmitted events: old test applications got
  workflows and became PENDING_MANUAL/CLOSED (their fake PDFs can't be parsed). Expected.
- e2e: tests/events/resume_files.py (copy of candidate's PDF/DOCX generators). Live break-its verified: worker
  stopped -> resume stays UPLOADED, restarted -> PARSED in ~4 s; Scoring stopped -> application stays SUBMITTED,
  restarted -> SCREENING with score in ~4 s.
- Phase 11 was built in one pass at the user's request (separate commit from Phase 10).
- Recipient addresses (the plan's open question): Notification LOOKS THEM UP from Identity's new internal endpoint
  `GET /internal/users/{id}/contact` ({user_id, email, role, status}) when it handles an event; events keep
  carrying IDs only (no PII in Kafka). Identity's internal path is BLOCKED at the gateway: APISIX route
  `block-internal` (uris /api/identity/internal[/*], priority 100, mocking JSON 404) because Identity's whole prefix
  is forwarded; gateway test covers it. Other services' /internal/* aren't reachable via their /api/<x>/* rewrites.
- Schema evolution done for real: optional `job_title` added to ApplicationSubmitted, ApplicationStageChanged,
  ApplicationDecisionMade (schema v2 registered; BACKWARD accepted). Application fills it (from job_replica.title).
  Notification falls back to "the job" for v1 events.
- Notification service (port 8007 on 127.0.0.1, health only, no public API, no gateway route): tables per E.8 +
  processed_events; the 5 templates (APPLICATION_RECEIVED, STAGE_CHANGED, DECISION_MADE, JOB_READY,
  JOB_PUBLISH_FAILED) are seeded by migration 0001. Consumer group `notification` on applications.lifecycle +
  jobs.lifecycle (app/events/handlers.py): route event -> (template, recipient user id, variables)
  (app/domain/routing.py) -> Identity contact (down -> RetryLater; missing/suspended user -> no email) -> INSERT
  notification_log with key "<event_id>:<template>" ON CONFLICT DO NOTHING -> commit -> send_task("send_email")
  (failure only logged: the sweeper catches it).
- Worker container `notification-worker`: `celery -A app.tasks.celery_app worker -B -Q emails` (beat embedded:
  run only ONE). Tasks (app/tasks/email.py): `send_email` (retry on SendLater, countdown 5 s doubling to 5 min,
  max_retries 20) and `requeue_stale` every 60 s (QUEUED rows older than 2 min re-enqueued). Sending
  (app/services/sending.py): SELECT ... FOR UPDATE SKIP LOCKED on the log row (parallel duplicate task -> "busy"),
  only QUEUED is sent, SENT recorded in the same transaction; pybreaker CircuitBreaker(fail_max=3,
  reset_timeout=30) per worker process around smtplib; SMTP error -> attempts+1, last_error (committed), retry;
  after MAX_ATTEMPTS=6 real attempts -> FAILED. Templates rendered with a forgiving format_map; subjects stripped
  of CR/LF (header injection). Residual risk: SMTP accepted but the DB commit failed -> could resend (rare).
- Mailpit `axllent/mailpit:v1.31.4`: SMTP 127.0.0.1:1025, UI/API http://localhost:8025. Keeps mail in memory:
  a restart empties it. The new `notification` consumer group replayed ALL past events on first start, so Mailpit
  got a burst of emails for old test users (expected).
- Live break-it verified: Mailpit stopped -> recruiter decision still 200 in 0.03 s, email row QUEUED with
  attempts=1 and the error; Mailpit restarted -> SENT (attempts=3). e2e test: JOB_READY to the recruiter, two
  candidate emails, replaying ApplicationSubmitted sends nothing new.
- Phase 12 was built in one pass at the user's request (with 12B; separate commits).
- FIXES FOUND IN PHASE 12 (affect every service):
  - Dockerfiles: `CMD ["sh", "-c", "exec uvicorn ..."]`. Without `exec`, sh was PID 1, swallowed SIGTERM, and Docker
    SIGKILLed services after 10 s: NO graceful shutdown ever (consumers never left their group: rebuild saw stale
    members for 45 s). Celery workers use exec-form commands already. Copy `exec` into any new Dockerfile.
  - APISIX `apisix.dns_resolver_valid: 10` (config.yaml): recreated containers get new IPs; the gateway cached old
    ones and returned 502 until restarted. Verified: identity moved .15 -> .21, gateway followed with no restart.
- Analytics (port 8008 127.0.0.1, schema/user analytics, Redis cache + denylist): projections exactly per E.9 PLUS
  `published_jobs(job_id, first_published_at)` (deviation: "jobs published, first version only" can't be told from
  job_application_counts, whose row may be created by an application event arriving before JobReady; topics have
  no order between them). All writes are upserts in the consumer's transaction (processed_events); timeline also
  guards with INSERT ... ON CONFLICT DO NOTHING. Times come from event.occurred_at (rebuild must be identical).
  Consumer group `analytics` on identity.users, jobs.lifecycle, applications.lifecycle, scoring.results,
  system.failures, assistant.usage. "Interview stage" = to_stage name containing "interview" (recruiters name
  stages freely). JobPublishFailed counts as a failure (source job_publishing_workflow). NEW event type
  `SystemFailure` {source, kind WORKFLOW_FAILED|DEAD_LETTER|ERROR, detail?} on system.failures (registered;
  producers come in Phase 13).
- Metrics API `GET /analytics/metrics` (RECRUITER/ADMIN; via gateway /api/analytics/metrics): all 9 metrics in one
  response (counters, applications_over_time last 30 days, application_to_interview_rate, average_days_to_hire,
  most_applied_jobs top 5, average_applications_per_candidate, failures_last_30_days), cached in Redis
  `metrics:dashboard` for `metrics_cache_seconds` (10).
- Rebuild (12.4): `docker compose stop analytics && docker compose run --rm analytics python -m app.cli rebuild &&
  docker compose start analytics`. It waits (<= 60 s) for the group to be empty, truncates projections +
  processed_events, replays every topic from the start with chassis `EventConsumer.replay_from_beginning()`
  (on_assign -> OFFSET_BEGINNING, stops at the high watermarks captured at start, commits offsets), clears the cache.
  e2e test proves identical numbers.
- Counters vs source DBs: equal for everything that produced events. Users registered before Phase 7 (no outbox
  then) and SQL-forced job states are invisible to Analytics by design; a backfill would need synthetic events.
- e2e changes: the DLQ test now expects one DLQ copy per consumer group on identity.users (candidate, analytics);
  `wait_for` ignores httpx errors (services restarting); "service not deployed" gateway test uses /api/assistant.
