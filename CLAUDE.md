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
- [ ] Phase 7 Kafka + outbox
- [ ] Phase 8 Application service
- [ ] Phase 9 Temporal + job publishing saga
- [ ] Phase 10 Resume parsing, scoring, application workflow
- [ ] Phase 11 Notification
- [ ] Phase 12 Analytics
- [ ] Phase 12B BFF (API composition)
- [ ] Phase 13 Observability
- [ ] Phase 14 Reliability + load tests (tag v0.1-core)
- [ ] Phase 15 Semantic index
- [ ] Phase 16 AI assistant
- [ ] Phase 17 Testing, docs, polish (tag v1.0)

**Next step:** commit Phase 6 if not done, then Phase 7 (Kafka + outbox), step by step again.

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
