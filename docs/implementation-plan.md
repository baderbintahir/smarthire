# SmartHire — Step-by-Step Implementation Plan

A beginner-friendly guide to building the SmartHire microservice backend from an empty folder to a working, observable, AI-enabled system, using Claude in VS Code as your pair programmer.

> **Revision 2 (2026-10-14), after the week-1 supervisor review.** Changes: the gateway is a
> prebuilt product (**Apache APISIX**) instead of a hand-written FastAPI proxy (Phase 4 rewritten);
> API composition lives in a **BFF service** (new Phase 12B), never in the gateway; revoked tokens
> are **blocked at the gateway** using Redis (Phase 4.3, Appendix F.3); explicit guidance on
> **Kafka vs Temporal** including observability (Appendix F.2, Phase 13); **online vs offline** paths
> in the AI Assistant (Appendix F.5, Phase 16). Rationale for every change: **Appendix F**.

---

## How to use this plan

### The golden rule
**You drive, Claude assists.** Do one step at a time. Never ask Claude to build a whole service in one go. Small steps mean you read, run, and understand every line before moving on.

### The loop for every step
1. **Read the step**: goal, concept, and tasks.
2. **Learn the concept first.** Ask Claude: *"Before writing code, explain [concept] in simple terms and how it applies to this step."*
3. **Ask Claude to implement** using the prompt given in the step (adapt as needed).
4. **Read the generated code.** Ask Claude to explain any line you don't understand.
5. **Run it and verify** using the "Done when" checklist.
6. **Break it on purpose** (stop a container, send bad input) and watch what happens.
7. **Commit** with the suggested message.
8. **Answer the "Check your understanding" questions** without looking. If you can't, ask Claude.

### Prompt habits that make you learn faster
- *"Explain what you are about to do and why, then wait for my OK."*
- *"Keep it minimal. Only what this step needs."*
- *"Add short comments explaining non-obvious lines."*
- *"What would break if I removed this line?"*
- *"Show me how to test this manually with curl."*
- *"I got this error: [paste]. Explain the cause before fixing it."*

### Timeline mapping

| Week | Phases | Outcome |
| --- | --- | --- |
| 1 | 0–6 | Foundation, chassis library, Identity, Gateway (APISIX), Job, Candidate |
| 2 | 7–9 | Kafka + outbox, Application Service, Temporal + Job Publishing workflow |
| 3 | 10–14 | Scoring, Application workflow, Notification, Analytics, BFF (12B), Observability, Reliability |
| 4 | 15 | Semantic Index (embeddings, vector search) |
| 5 | 16–17 | AI Assistant, testing, documentation |

If a phase takes longer, that is fine. Understanding matters more than speed.

---

## Technology choices

| Concern | Tool | Why this choice |
| --- | --- | --- |
| Language / framework | Python 3.12, FastAPI | Async, type hints, auto-generated API docs |
| Dependency management | `uv` | Fast, simple, one tool for venvs and packages |
| Relational DB | PostgreSQL 16 | Transactions, constraints, row locks |
| ORM / migrations | SQLAlchemy 2.0 (async) + asyncpg, Alembic | Industry standard for Python |
| Validation / config | Pydantic v2, pydantic-settings | Request models and env-based config |
| Document DB (NoSQL) | MongoDB + Motor | Flexible nested documents |
| Cache / rate limits / denylist | Redis | Fast key-value with expiry |
| Task queue | Celery + RabbitMQ | Background tasks with retries |
| Event streaming | Kafka (KRaft mode) + Confluent Schema Registry, `confluent-kafka` client | Durable events with schema contracts |
| Workflows | Temporal + `temporalio` SDK | Durable sagas with retries and compensation |
| Object storage | MinIO (S3-compatible) | Resume files |
| Email testing | Mailpit | Catches emails locally with a web UI |
| API gateway | Apache APISIX (standalone mode, declarative YAML) | Open source; JWT via JWKS, Redis rate limiting, request IDs, path rewrite, SSE; config lives in git (Appendix F.4) |
| API composition | BFF service (FastAPI) + event-fed replicas | Gateway stays free of business logic (Appendix F.1) |
| Vector DB (Part B) | Qdrant | Simple, runs in Docker, metadata filters |
| Embeddings (Part B) | `sentence-transformers` (local) or provider API | Local is free; provider is higher quality |
| AI agents (Part B) | LangGraph + LangChain chat model (OpenAI / Groq / Anthropic) | Agent graphs with tools |
| Observability | OpenTelemetry, Jaeger, Prometheus, Grafana | Traces, metrics, dashboards |
| Testing | pytest, pytest-asyncio, httpx, Testcontainers | Unit, component, integration |
| Load testing | Locust | Simulate hiring spikes in Python |
| Containers | Docker, Docker Compose | One command to run everything |

---

## Final repository structure

You will build towards this layout. Do not create it all at once; each phase adds its part.

```
smarthire/
├── CLAUDE.md                     # Project context for Claude (Phase 0)
├── docs/
│   └── implementation-plan.md    # This plan (Claude reads schemas from Appendix E)
├── README.md
├── docker-compose.yml            # Infrastructure + services
├── .env.example
├── Makefile                      # Shortcuts: make up, make test, make logs
├── libs/
│   └── smarthire_common/         # Shared chassis library
│       ├── pyproject.toml
│       └── smarthire_common/
│           ├── config.py
│           ├── logging.py
│           ├── db.py
│           ├── errors.py
│           ├── health.py
│           ├── auth.py           # JWT verification, role/ownership helpers
│           ├── outbox.py         # Outbox model + relay
│           ├── kafka.py          # Producer, consumer, idempotency
│           ├── telemetry.py      # OpenTelemetry setup
│           └── metrics.py
├── schemas/                      # Event JSON Schemas (registered in Schema Registry)
├── services/
│   ├── identity/
│   ├── job/
│   ├── job_enrichment/
│   ├── candidate/
│   ├── application/
│   ├── scoring/
│   ├── notification/
│   ├── analytics/
│   ├── semantic_index/
│   ├── ai_assistant/
│   └── bff_web/                  # Backend for Frontend: API composition (Phase 12B)
├── infra/
│   ├── apisix/                   # Gateway: config.yaml, apisix.yaml (routes), plugins/ (Phase 4)
│   ├── prometheus/prometheus.yml
│   ├── grafana/dashboards/
│   └── postgres/init.sql         # One schema + user per service
├── loadtests/locustfile.py
└── tests/e2e/
```

Every service follows the same internal layout (created from the service template in Phase 1):

```
services/<name>/
├── Dockerfile
├── pyproject.toml
├── alembic.ini / alembic/        # Only if it uses PostgreSQL
├── app/
│   ├── main.py                   # FastAPI app, startup/shutdown
│   ├── config.py                 # Service settings
│   ├── api/                      # Routers (HTTP layer only)
│   ├── domain/                   # Business rules, no frameworks
│   ├── models/                   # SQLAlchemy / document models
│   ├── schemas/                  # Pydantic request/response models
│   ├── repositories/             # Database access
│   ├── services/                 # Use cases: orchestrate domain + repos
│   ├── events/                   # Producers / consumers
│   └── workflows/                # Temporal workflows / activities (if any)
└── tests/
```

**Why this layering?** Routers only handle HTTP. Domain code holds business rules and can be tested without a database. Repositories hide SQL. This separation is what lets you change one part without breaking others.

---

## Port map (local)

| Component | Port |
| --- | --- |
| Gateway (APISIX proxy; container port 9080) | 8000 |
| Identity | 8001 |
| Job | 8002 |
| Job Enrichment | 8003 |
| Candidate | 8004 |
| Application | 8005 |
| Scoring | 8006 |
| Notification | 8007 |
| Analytics | 8008 |
| Semantic Index | 8009 |
| AI Assistant | 8010 |
| BFF (web) | 8011 |
| PostgreSQL | 5432 |
| MongoDB | 27017 |
| Redis | 6379 |
| RabbitMQ / management UI | 5672 / 15672 |
| Kafka | 9092 |
| Schema Registry | 8081 |
| Kafka UI | 8085 |
| Temporal / Temporal UI | 7233 / 8088 |
| MinIO / console | 9000 / 9001 |
| Mailpit SMTP / UI | 1025 / 8025 |
| Qdrant | 6333 |
| Jaeger UI | 16686 |
| Prometheus | 9090 |
| Grafana | 3000 |

In Docker Compose, services reach each other by name (for example `http://identity:8001`), not `localhost`.

---

# Phase 0 — Preparation (half a day)

### Step 0.1 — Install tools
Install: Docker Desktop, Python 3.12, `uv`, Git, VS Code with the Python and Docker extensions, and Claude in VS Code. Optional: a database GUI (DBeaver or TablePlus) and an HTTP client (Bruno, Insomnia, or Postman).

**Done when:** `docker compose version`, `python3.12 --version`, `uv --version`, `git --version` all work.

### Step 0.2 — Learn the minimum concepts
Ask Claude to explain each of these with a tiny example. Spend 20–30 minutes on each; you will deepen them during the build.
- Python `async`/`await` and why FastAPI uses it
- FastAPI: routers, dependencies (`Depends`), Pydantic models
- SQLAlchemy 2.0 models and sessions; what a migration is (Alembic)
- Docker: image vs container, Dockerfile, volumes, Compose networks
- HTTP status codes: 200, 201, 202, 400, 401, 403, 404, 409, 422, 429, 500

### Step 0.3 — Create the repository and CLAUDE.md
Create the `smarthire/` folder, `git init`, a Python `.gitignore`, and a `CLAUDE.md` at the root. Claude in VS Code reads `CLAUDE.md` automatically, so every session starts with full project context.

Also copy this plan into the repository as `docs/implementation-plan.md` so Claude can read any step or schema directly.

Paste this into `CLAUDE.md` and update it as the project evolves:

```markdown
# SmartHire — project context for Claude

## What this is
Microservice backend for a recruitment platform. I am a beginner in backend
microservices and I am learning while building. Explain before coding, keep
changes small, and add comments on non-obvious lines.

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
- Config via pydantic-settings and environment variables. No secrets in code.
- JSON structured logging with trace_id.
- Every service exposes /health/live, /health/ready, /metrics.
- Tests in services/<name>/tests, run with pytest.
```

**Commit:** `chore: initialize repository with CLAUDE.md`

---

# Phase 1 — Foundation: infrastructure, chassis, service template (1–2 days)

**Goal:** a running local infrastructure and a shared library that every service will reuse, so you write logging, config, DB, and health code once.

**Concepts to learn first:** Docker Compose networks and volumes; healthchecks; what a "microservice chassis" is (shared cross-cutting code); externalized configuration.

### Step 1.1 — Docker Compose with PostgreSQL and Redis
Start small: only Postgres and Redis. Add `infra/postgres/init.sql` that creates one **schema and one DB user per service** (identity, job, candidate, application, notification, analytics), each user allowed only on its own schema.

> **Prompt:** "Create a docker-compose.yml with PostgreSQL 16 and Redis 7, with named volumes and healthchecks. Add infra/postgres/init.sql that creates a schema and a dedicated user for each of: identity, job, candidate, application, notification, analytics. Each user should only have privileges on its own schema. Explain how this enforces database-per-service while using one Postgres container."

**Done when:** `docker compose up -d` shows both healthy; you can connect as `identity_user` and cannot create tables in the `job` schema.

**Check your understanding:** Why one schema per service instead of one shared schema? What happens to data when you run `docker compose down` vs `down -v`?

**Commit:** `infra: add postgres and redis with per-service schemas`

### Step 1.2 — Create the chassis library skeleton
Create `libs/smarthire_common` as an installable Python package.

> **Prompt:** "Create a Python package libs/smarthire_common with pyproject.toml (uv). Add modules: config.py (base pydantic-settings class with service_name, environment, log_level), logging.py (JSON structured logging with service name and a request_id field), errors.py (a base AppError with code and http_status, plus a FastAPI exception handler returning a consistent JSON error body). Keep each module small and explain each."

**Done when:** you can `uv pip install -e libs/smarthire_common` and import it in a Python shell.

**Commit:** `feat(common): add config, logging, and error handling`

### Step 1.3 — Add database helpers and health checks to the chassis
> **Prompt:** "Add to smarthire_common: db.py (async SQLAlchemy engine and session factory from a DATABASE_URL setting, a FastAPI dependency get_session that commits or rolls back), and health.py (a router with /health/live returning ok, and /health/ready that runs registered check functions such as a DB ping). Explain the difference between liveness and readiness."

Then add the database conventions from Appendix E.1–E.3:

> **Prompt:** "Read Appendix E.1–E.3 of docs/implementation-plan.md. Add to smarthire_common: a SQLAlchemy Base whose MetaData uses the naming convention from E.2, a TimestampMixin (created_at, updated_at as timestamptz), and a new_id() helper that returns a UUIDv7 (uuid6 package). Explain why UUIDv7 and why constraint names matter."

**Check your understanding:** Why does readiness check the DB but liveness doesn't? What does the session dependency do if your endpoint raises an exception? Why is UUIDv7 better than UUIDv4 for primary keys?

**Commit:** `feat(common): add async db session, base model conventions, and health endpoints`

### Step 1.4 — Create a service template
Create a minimal "hello" service in `services/_template/` with the standard layout, a Dockerfile, and a `main.py` that wires logging, error handler, and health router from the chassis. Every future service starts as a copy of this.

> **Prompt:** "Create services/_template using the layout in CLAUDE.md: a FastAPI app that uses smarthire_common for config, logging, error handling, and health routes; a Dockerfile (python:3.12-slim, uv, non-root user) that installs the shared library from libs/; and an Alembic setup that uses the service's own schema and version table. Explain each Dockerfile line."

**Done when:** the template runs in Compose and `curl localhost:<port>/health/ready` returns ok.

**Commit:** `feat: add service template`

### Step 1.5 — Add a Makefile
Shortcuts: `make up`, `make down`, `make logs s=identity`, `make migrate s=identity`, `make test s=identity`.

**Commit:** `chore: add Makefile shortcuts`

---

# Phase 2 — Identity Service (2 days)

**Goal:** users can register, log in, refresh, and log out. Identity issues RS256-signed JWTs and publishes its public keys.

**Concepts to learn first:** password hashing (argon2/bcrypt); JWT structure (header, payload, signature); asymmetric keys (private signs, public verifies); access vs refresh tokens; JWKS; 401 vs 403.

### Step 2.1 — Scaffold the service
Copy the template to `services/identity`, rename, set port 8001, add to Compose, point it at the `identity` schema.

**Done when:** `/health/ready` is ok through Compose.

### Step 2.2 — User model and migration
Tables: `users` and `recruiter_profiles` exactly as in **Appendix E.4**.

> **Prompt:** "In the identity service, create SQLAlchemy models for users and recruiter_profiles exactly matching Appendix E.4 (including the lower(email) unique index, CHECK constraints, and token_version). Generate the Alembic migration and explain every constraint and index."

**Done when:** `make migrate s=identity` creates the tables in the identity schema.

**Commit:** `feat(identity): add user and recruiter profile models`

### Step 2.3 — Password hashing
> **Prompt:** "Add app/domain/passwords.py with hash_password and verify_password using argon2 (passlib or argon2-cffi). Write unit tests. Explain why we hash instead of encrypt, and what a salt is."

**Commit:** `feat(identity): add password hashing`

### Step 2.4 — Register endpoint
`POST /auth/register` with email, password, role, and (for recruiters) display_name and company. Validate input, reject duplicate email with 409, create the user (and recruiter profile) in one transaction, return 201 without the password hash.

> **Prompt:** "Implement POST /auth/register following the layering: Pydantic schemas, a repository for users, and a service function that creates the user and recruiter profile in one transaction. Duplicate email returns 409 via AppError. Do not return the password hash. Add tests."

**Done when:** you register a candidate and a recruiter with curl; a duplicate email returns 409.

**Commit:** `feat(identity): add registration`

### Step 2.5 — RSA keys and JWT creation
Generate an RSA key pair for local development (store the private key path or PEM in env, never commit it). Create a `kid` for the key.

> **Prompt:** "Add app/domain/tokens.py that creates RS256 access tokens with claims sub, role, iss, aud, iat, exp (15 minutes), and jti, signed with a private key loaded from settings, with kid in the header. Add a script to generate a dev key pair. Explain each claim."

**Check your understanding:** Why RS256 and not HS256? Why is the payload not secret? What does `kid` let us do?

**Commit:** `feat(identity): add RS256 access token creation`

### Step 2.6 — JWKS endpoint
`GET /.well-known/jwks.json` returns the public key(s) in JWK format.

**Done when:** you can paste a token into jwt.io with the public key and see "signature verified".

**Commit:** `feat(identity): expose JWKS`

### Step 2.7 — Login with refresh tokens
- Table `refresh_tokens` exactly as in **Appendix E.4**.
- `POST /auth/login`: verify password, return access token in the body and set refresh token as an httpOnly, Secure, SameSite cookie. Store only the hash of the refresh token.

> **Prompt:** "Implement login with refresh tokens: create a refresh_tokens table, generate a random refresh token, store its SHA-256 hash with a family_id, return the access token in JSON and the refresh token in an httpOnly cookie. Wrong credentials return 401 with a generic message. Explain why we store a hash and why the message is generic."

**Commit:** `feat(identity): add login with refresh tokens`

### Step 2.8 — Refresh with rotation and reuse detection
`POST /auth/refresh`: look up by hash; if valid, mark used, issue new access + refresh tokens (same family). If a **used** token is presented again, revoke the entire family.

**Done when:** refreshing twice with the same old cookie revokes the session and forces login.

**Commit:** `feat(identity): add refresh token rotation and reuse detection`

### Step 2.9 — Logout and immediate revocation
- `POST /auth/logout`: revoke the refresh token, clear the cookie.
- `POST /auth/logout-all`: revoke all refresh tokens for the user.
- Admin endpoint `POST /auth/revoke/{jti}`: put the jti in Redis `denylist:{jti}` with TTL equal to the token's remaining lifetime.

**Check your understanding:** After normal logout, why does the old access token still work for a few minutes, and why is that acceptable?

**Commit:** `feat(identity): add logout, logout-all, and token denylist`

### Step 2.10 — Current user endpoint and rate limiting on login
- `GET /users/me` returns the caller's profile (verify the token locally for now).
- Limit failed login attempts per email and IP using Redis counters.

**Commit:** `feat(identity): add /users/me and login rate limiting`

### Step 2.11 — Tests
Unit tests for passwords and tokens; component tests for register, login, refresh, logout using a test database (Testcontainers or a dedicated test schema).

**Done when:** `make test s=identity` passes.

**Commit:** `test(identity): add component tests`

---

# Phase 3 — Shared auth in the chassis (half a day)

**Goal:** every service can verify JWTs and enforce roles and ownership with two lines of code.

### Step 3.1 — JWKS client and token verification
> **Prompt:** "Add smarthire_common/auth.py: a JWKS client that fetches and caches public keys from the identity service (refresh every 10 minutes and on unknown kid), a verify_token function that checks signature, exp, iss, aud, and a Redis denylist lookup on jti. Add FastAPI dependencies: get_current_user returning a CurrentUser(user_id, role), and require_role(*roles). Explain each check and the 401 vs 403 difference."

### Step 3.2 — Ownership helper pattern
Document a simple pattern: services load the resource and compare its owner field to `current_user.user_id`, raising 403 if different. Add a helper `ensure_owner(resource_owner_id, current_user)`.

**Done when:** Identity's `/users/me` uses the shared dependency, and a tampered token returns 401.

**Commit:** `feat(common): add JWT verification and role dependencies`

---

# Phase 4 — API Gateway with Apache APISIX (1–1.5 days)

**Goal:** a single, prebuilt entry point that routes requests, verifies tokens, blocks revoked tokens, rate-limits, adds request IDs, and streams responses. No business logic and no data composition in the gateway (composition is the BFF's job, Phase 12B).

**Design note (revision 2):** we use a production gateway instead of writing one. Apache APISIX was chosen over Kong OSS (its JWT plugin can't fetch JWKS, so key rotation is manual), Traefik (no built-in JWT verification) and Envoy (powerful but heavy to configure). See Appendix F.4. APISIX runs in **standalone mode**: routes live in a YAML file in git, no etcd.

**Concepts to learn first:** reverse proxy; edge concerns vs business logic; hop-by-hop headers and `X-Forwarded-*`; 502 vs 504; JWKS and key rotation; fail-closed; token bucket / fixed-window rate limiting; why gateway readiness must not depend on upstream services.

### Step 4.1 — Spike: APISIX in Compose
Run APISIX (pinned version) in standalone mode with one route to Identity. Prove the features this phase relies on, on the pinned version, before building on them: `proxy-rewrite`, `request-id`, `openid-connect` with `bearer_only` + `use_jwks`, `limit-count` with `policy: redis`, SSE pass-through.

> **Prompt:** "Add Apache APISIX (pinned version, standalone mode) to docker-compose with config in infra/apisix/ (config.yaml + apisix.yaml). Expose it on host port 8000. Add one route /api/identity/* -> identity:8001 with proxy-rewrite stripping the prefix. Explain standalone vs etcd mode and how APISIX reloads the YAML."

**Done when:** `curl localhost:8000/api/identity/.well-known/jwks.json` returns Identity's keys through APISIX.

### Step 4.2 — Routes, request IDs, forwarding headers, timeouts
- Route table (same rewriting rules as before):

| Public prefix | Upstream | Path sent upstream |
| --- | --- | --- |
| `/api/identity/*` | identity:8001 | strip `/api/identity` |
| `/api/jobs/*` | job:8002 | strip `/api` |
| `/api/candidates/*` | candidate:8004 | strip `/api` |
| `/api/applications/*` | application:8005 | strip `/api` |
| `/api/analytics/*` | analytics:8008 | strip `/api` |
| `/api/assistant/*` | ai_assistant:8010 | strip `/api` |
| `/api/bff/*` | bff_web:8011 | strip `/api` (Phase 12B) |

- `request-id` plugin: reuse the client's `X-Request-ID` or generate one; forward it upstream (services log it).
- Upstream timeouts (connect 2 s, read 30 s); APISIX returns 502 when a service is unreachable and 504 when it is too slow.
- Identity: `REFRESH_COOKIE_PATH=/api/identity/auth`; read the client IP for login rate limiting from `X-Forwarded-For` **only when the request comes from the gateway's network** (trusted proxy), otherwise the header is forgeable.

**Done when:** register/login/refresh work through `localhost:8000/api/identity/...`; the same request ID appears in APISIX and Identity logs; Identity's login limiter sees the real client IP; `/api/jobs/...` returns 502 (no Job service yet).

### Step 4.3 — Authentication and revocation at the edge
- Identity adds a minimal OIDC discovery document `GET /.well-known/openid-configuration` (`issuer`, `jwks_uri`) so APISIX can find the keys.
- `openid-connect` plugin on protected routes: `bearer_only: true`, `use_jwks: true`, issuer/audience pinned. Public routes (register, login, refresh, logout, JWKS, discovery) have no auth plugin.
- **Revocation at the edge (Appendix F.3):** a small custom APISIX plugin (`infra/apisix/plugins/smarthire-denylist.lua`) checks Redis after signature verification:
  - `denylist:{jti}` (single token revoked by an admin), and
  - `revoked_before:{user_id}` (user suspended or "logout everywhere": tokens issued before this timestamp are rejected).
  - Redis unavailable → **503** (fail closed), never let an unchecked token through.
- Identity writes `revoked_before:{user_id}` (TTL = access-token lifetime) on logout-all and when an admin suspends a user. The chassis `verify_token` checks the same key, so services keep enforcing it too (rule 7: defence in depth).

> **Prompt:** "Configure APISIX openid-connect (bearer_only, use_jwks) against Identity's discovery document, and write a minimal Lua plugin that rejects tokens whose jti is in Redis `denylist:{jti}` or whose iat is before `revoked_before:{user_id}`, returning 503 if Redis is unreachable. Explain where in the request lifecycle (phase) the plugin runs and why it must run after JWT verification."

**Done when:** no token → 401 at the gateway; tampered token → 401; an admin revokes a jti → the next request is 401 **at the gateway** (Identity never sees it); admin suspends a user → all their tokens are rejected immediately; Redis stopped → 503.

**Check your understanding:** Why check the denylist at the gateway if services check it too? Why does `revoked_before` only need a TTL of 15 minutes?

### Step 4.4 — Rate limiting
`limit-count` with `policy: redis` so all APISIX instances share counters. Per user (JWT `sub`) on protected routes, per IP on public routes, stricter on `/api/identity/auth/login`. Return 429 with `Retry-After`.

**Done when:** hammering an endpoint returns 429; two APISIX replicas share the same limit.

### Step 4.5 — Streaming pass-through
Disable response buffering on `/api/assistant/*` so Server-Sent Events stream through (needed in Phase 16).

**Done when:** a test SSE endpoint streams events through APISIX one by one, not all at the end.

### Step 4.6 — Gateway observability hooks
Enable the APISIX `prometheus` plugin (scraped in Phase 13) and leave `opentelemetry` ready to point at Jaeger in Phase 13.

**Check your understanding:** Why must the gateway's readiness not depend on Identity? Why is joining data from several services not the gateway's job?

**Commit:** `feat(gateway): add APISIX gateway with JWT, revocation, rate limiting, request IDs`

---

# Phase 5 — Job Service (1–1.5 days)

**Goal:** recruiters manage jobs and hiring stages; the Job aggregate enforces its lifecycle rules.

**Concepts to learn first:** aggregates and invariants; state machines; optimistic versioning.

### Step 5.1 — Scaffold and models
Tables: `jobs` and `job_stages` exactly as in **Appendix E.5**.

> **Prompt:** "Scaffold services/job from the template. Create models for jobs and job_stages exactly matching Appendix E.5, including the partial index on READY jobs and the unique stage constraints. Generate the migration and explain why skills are jsonb but stages are a table."

**Commit:** `feat(job): add job and stage models`

### Step 5.2 — Domain state machine
> **Prompt:** "Create app/domain/job.py with pure functions or a class that enforces allowed status transitions (DRAFT→PROCESSING, PROCESSING→READY/FAILED, FAILED→PROCESSING, READY→PROCESSING on update, READY→CLOSED). Invalid transitions raise a domain error. Unit-test every transition without a database."

**Check your understanding:** Why put this logic in the domain layer instead of the router?

**Commit:** `feat(job): add job lifecycle state machine`

### Step 5.3 — CRUD endpoints
- `POST /jobs` (RECRUITER only) creates a DRAFT with stages.
- `GET /jobs` (public: only READY jobs; recruiters: their own jobs in any status), with pagination and filters.
- `GET /jobs/{id}`, `PUT /jobs/{id}` (owner only). Use optimistic locking: the client sends the `version` it read; `UPDATE … WHERE id = :id AND version = :expected`; 0 rows updated returns 409. Editing a READY job increments `version`.
- `POST /jobs/{id}/close` (owner only).

**Done when:** a recruiter can create and edit jobs; another recruiter gets 403; a candidate gets 403 on create.

**Commit:** `feat(job): add job CRUD with ownership checks`

### Step 5.4 — Publish endpoint (temporary version)
`POST /jobs/{id}/publish` sets status PROCESSING and returns 202. For now, leave it at PROCESSING; Phase 9 adds the workflow that completes it.

**Commit:** `feat(job): add publish endpoint stub`

---

# Phase 6 — Candidate Service (1 day)

**Goal:** candidates manage profiles and upload resumes.

### Step 6.1 — Scaffold, MinIO, and models
Add MinIO to Compose with a `resumes` bucket. Tables: `candidates` and `resumes` exactly as in **Appendix E.6**, including the partial unique index that allows only one active resume per candidate.

**Commit:** `feat(candidate): add models and MinIO`

### Step 6.2 — Profile endpoints
`GET /candidates/me`, `PUT /candidates/me` (CANDIDATE only, own profile). Recruiters can `GET /candidates/{id}` only through the application flow later (for now, allow recruiters read access and note it as a rule to tighten).

**Commit:** `feat(candidate): add profile endpoints`

### Step 6.3 — Resume upload
`POST /candidates/me/resumes` accepts PDF or DOCX (size limit, content type check), stores the file in MinIO with a private key, creates a resume row, marks it active, deactivates older ones. Return 202 with status UPLOADED. Parsing comes in Phase 10.

> **Prompt:** "Implement resume upload: validate file type and size, upload to MinIO with boto3 or the minio client under resumes/{candidate_id}/{uuid}, create the resume row, set it active and deactivate previous ones in one transaction. Explain why we never expose public bucket URLs."

**Done when:** an uploaded file appears in the MinIO console and the resume row exists.

**Commit:** `feat(candidate): add resume upload`

> **Temporary note:** right now a candidate profile must be created manually. Phase 7 creates it automatically from the `UserRegistered` event.

---

# Phase 7 — Kafka, Schema Registry, and the Transactional Outbox (2 days)

**Goal:** reliable event publishing and idempotent consumption, built once in the chassis and reused everywhere. This is the heart of the event-driven design.

**Concepts to learn first:** topics, partitions, keys, consumer groups, offsets; at-least-once delivery; the dual-write problem; transactional outbox; idempotent consumers; schema compatibility.

### Step 7.1 — Add Kafka, Schema Registry, and Kafka UI
Kafka in KRaft mode (no ZooKeeper), Confluent Schema Registry, and Kafka UI for browsing topics.

**Done when:** Kafka UI shows the cluster; you can create a test topic.

**Commit:** `infra: add kafka, schema registry, kafka ui`

### Step 7.2 — Event envelope and schemas
Define a standard envelope: `event_id`, `event_type`, `schema_version`, `occurred_at`, `producer`, `trace_context`, `payload`. Write JSON Schemas in `schemas/` for: `UserRegistered`, `JobReady`, `JobUpdated`, `JobClosed`, `JobPublishFailed`, `ResumeUploaded`, `ResumeParsed`, `ApplicationSubmitted`, `ApplicationStageChanged`, `ApplicationDecisionMade`, `CandidateScored`, `AssistantQueryCompleted`.

> **Prompt:** "Create the event envelope as a Pydantic model in smarthire_common and JSON Schema files for these events. Add a script that registers them in Schema Registry with BACKWARD compatibility. Explain what BACKWARD compatibility allows and forbids."

**Commit:** `feat(events): add envelope and event schemas`

### Step 7.3 — Outbox table and writer
> **Prompt:** "Add smarthire_common/outbox.py: a SQLAlchemy mixin or function that creates the outbox table exactly as in Appendix E.10 (including the partial index on unpublished rows) in each service's schema, and a function add_outbox_event(session, topic, key, event) that inserts a row using the caller's session so it commits in the same transaction. Explain the dual-write problem this solves."

### Step 7.4 — Polling publisher (outbox relay)
> **Prompt:** "Add an outbox relay to smarthire_common: a background asyncio task that every 200 ms selects unpublished rows with FOR UPDATE SKIP LOCKED in batches, publishes them to Kafka with confluent-kafka (key = aggregate id, JSON Schema serializer), waits for delivery confirmation, then sets published_at. Explain SKIP LOCKED and why delivery is at-least-once."

**Check your understanding:** What happens if the relay crashes after publishing but before marking `published_at`? Why is that acceptable?

### Step 7.5 — Idempotent consumer base
> **Prompt:** "Add the processed_events table from Appendix E.10 and smarthire_common/kafka.py with a consumer runner: subscribes to topics with a consumer group, deserializes the envelope, opens a DB transaction, inserts (consumer_name, event_id) into processed_events (skip if it already exists), calls the handler with the same session, commits, then commits the Kafka offset. On handler failure, retry with backoff N times, then send to <topic>.dlq with the error. Explain why the offset is committed after the DB commit."

**Commit:** `feat(common): add outbox, relay, and idempotent consumer`

### Step 7.6 — First end-to-end event: UserRegistered → candidate profile
- Identity writes `UserRegistered` to its outbox in the register transaction.
- Candidate runs a consumer that creates an empty profile when `role == CANDIDATE`.

**Done when:** registering a candidate automatically creates a Candidate profile; replaying the same event (reset offsets in Kafka UI) does **not** create a duplicate.

**Break it:** stop Kafka, register a user, start Kafka again. The event should still arrive (it waited in the outbox).

**Commit:** `feat: publish UserRegistered and create candidate profiles from events`

### Step 7.7 — Job events
Job Service writes `JobUpdated` and `JobClosed` to its outbox. (`JobReady` is added with the workflow in Phase 9.)

**Commit:** `feat(job): publish job events via outbox`

---

# Phase 8 — Application Service (2 days)

**Goal:** candidates apply with all rules enforced in one transaction; recruiters manage the pipeline. This is the core subdomain, so take your time.

**Concepts to learn first:** self-contained service; local replicas from events (CQRS); unique constraints; `SELECT … FOR UPDATE`; idempotency keys.

### Step 8.1 — Scaffold and models
Tables exactly as in **Appendix E.7**: `job_replica`, `applications`, `application_stage_history`, `application_notes`, `application_quotas`, `audit_entries`, plus `outbox` and `processed_events` from E.10.

> **Prompt:** "Create the application service models exactly matching Appendix E.7. Before writing code, explain: why the quota table exists instead of counting rows, what the unique constraint protects against, why job_id has a foreign key to job_replica, and which query each index serves."

**Commit:** `feat(application): add models`

### Step 8.2 — Job replica consumer
Consume `JobReady`, `JobUpdated`, `JobClosed` and upsert `job_replica`, applying only newer versions with the `source_version` guard from Appendix E.7.

**Done when:** publishing or closing a job (simulated by producing an event) updates the replica.

**Commit:** `feat(application): maintain job replica from events`

### Step 8.3 — Submit application
`POST /applications` with an `Idempotency-Key` header. Follow the "Inside the apply endpoint" diagram exactly:
1. Idempotency key seen in Redis? Return the cached response.
2. Role must be CANDIDATE.
3. Job in replica, READY, before deadline; else 422.
4. Eligibility rules pass; else 422.
5. One transaction: lock quota row FOR UPDATE, check limit, insert application, insert history (SUBMITTED), increment quota, add `ApplicationSubmitted` to outbox, commit.
6. Unique violation returns 409.
7. Cache the response under the idempotency key; return 202.

> **Prompt:** "Implement POST /applications exactly following these 7 steps. Put eligibility and quota rules in app/domain, database access in repositories, and orchestration in a service function. Explain how each step prevents a specific failure."

**Done when:** applying twice with the same key returns the same response; applying twice with different keys returns 409; exceeding the limit returns 422.

**Break it:** fire 20 concurrent applies for the same candidate with Locust or a small asyncio script. The quota must never be exceeded.

**Commit:** `feat(application): add application submission with all rules`

### Step 8.4 — Candidate and recruiter views
- `GET /applications/me` (candidate: own applications).
- `GET /jobs/{job_id}/applications` (recruiter: only if they own the job, checked through `job_replica.recruiter_id`), with filters by stage and sorting by score.
- `GET /applications/{id}` (candidate owner or job owner).

**Commit:** `feat(application): add candidate and recruiter views`

### Step 8.5 — Stage changes, notes, decisions, withdrawal
- `PATCH /applications/{id}/stage` (recruiter owner): validate the transition against the job's stages, write history, add `ApplicationStageChanged` to outbox.
- `POST /applications/{id}/notes`.
- `POST /applications/{id}/decision` (HIRED / REJECTED): final, writes `ApplicationDecisionMade`, decrements quota.
- `POST /applications/{id}/withdraw` (candidate): decrements quota.
- Write audit entries for notes, stage changes, and decisions.

**Commit:** `feat(application): add stage changes, notes, decisions, withdrawal`

---

# Phase 9 — Temporal, Job Enrichment, and the Job Publishing workflow (2 days)

**Goal:** publishing a job runs a durable saga: extraction → breakdown → structured storage → READY, with retries and compensation.

**Concepts to learn first:** Temporal workflows vs activities; determinism (why workflows cannot call the network or use random/time directly); task queues; retry policies; compensation; workflow IDs.

### Step 9.1 — Add Temporal and MongoDB
Add Temporal server (with its own Postgres database or the auto-setup image), Temporal UI, and MongoDB.

**Done when:** Temporal UI opens at port 8088.

**Commit:** `infra: add temporal and mongodb`

### Step 9.2 — Temporal hello world
Before touching SmartHire, ask Claude for a tiny workflow with two activities and run it. Watch it in the Temporal UI. Kill the worker mid-run, restart it, and watch it resume.

> **Prompt:** "Show me a minimal temporalio example: one workflow calling two activities with a retry policy, a worker, and a starter script. Then explain determinism rules and what replay means."

### Step 9.3 — Job Enrichment Service
Scaffold `services/job_enrichment` (MongoDB, no Postgres). Implement activities on task queue `enrichment-tq`:
- `extract_skills(job_id, version, description, required_skills)`: rule-based extraction with a skills dictionary plus keyword extraction (for example YAKE).
- `breakdown_jd(description)`: split into requirements, responsibilities, benefits (heading and keyword based).
- `store_structured_job(job_id, version, data)`: upsert into the `structured_jobs` collection from **Appendix E.11** (deterministic `_id` = `job_id:version`).
- `delete_structured_job(job_id, version)`: compensation.
- Read API: `GET /jobs/{id}/structured`, `GET /search/jobs?q=` (Mongo text index).

> **Prompt:** "Create the job_enrichment service with a Temporal worker on enrichment-tq exposing these four activities, and the read API. Activities must be idempotent upserts. Explain why idempotency matters for activities."

**Commit:** `feat(enrichment): add enrichment activities and read API`

### Step 9.4 — Job Publishing workflow in the Job Service
- Workflow `JobPublishingWorkflow(job_id, version)` on `job-tq`.
- Steps: `set_processing` → `extract_skills` → `breakdown_jd` → `store_structured_job` → `mark_ready` (which writes `JobReady` to the outbox).
- Retry policy per activity (e.g., 5 attempts, exponential backoff).
- On final failure: run compensations in reverse (`delete_structured_job`), then `mark_failed` (writes `JobPublishFailed`).
- Workflow ID: `publish-{job_id}-{version}`.

### Step 9.5 — Start the workflow reliably
Change `POST /jobs/{id}/publish` to: set PROCESSING and write a `JobPublishRequested` outbox row in one transaction. A small internal consumer (or the relay hook) starts the workflow with the deterministic ID; a duplicate start is rejected by Temporal and safely ignored.

**Done when:** publishing a job ends in READY with a structured document in Mongo and `JobReady` in Kafka; the Application replica updates.

**Break it:** stop the enrichment service, publish a job, watch retries in the Temporal UI, then let retries run out and confirm the job is FAILED and the structured document is removed. Start enrichment again and re-publish.

**Commit:** `feat(job): add job publishing saga with compensation`

---

# Phase 10 — Resume parsing, Scoring, and the Application workflow (1.5 days)

**Goal:** resumes are parsed in the background; every application is scored and advanced by a durable workflow.

**Concepts to learn first:** Celery tasks, `acks_late`, retries; Temporal signals; when to use Celery vs Temporal.

### Step 10.1 — Add RabbitMQ and Celery to the Candidate Service
Add RabbitMQ to Compose. Create a Celery app in the Candidate Service and a worker container.

### Step 10.2 — Resume parsing task
On upload, enqueue `parse_resume(resume_id)`:
- Download from MinIO, extract text (pypdf / python-docx).
- Extract skills (reuse the same skills dictionary approach), experience years, education keywords.
- Save `parsed_data`, set status PARSED, add `ResumeParsed` to the outbox.
- Idempotent: if already PARSED, do nothing. Use `acks_late=True` and autoretry with backoff.

**Done when:** uploading a resume leads to PARSED status and a `ResumeParsed` event.

**Check your understanding:** Why is parsing a Celery task and not a Temporal activity?

**Commit:** `feat(candidate): add background resume parsing`

### Step 10.3 — Scoring Service
Scaffold `services/scoring` (MongoDB). Temporal worker on `scoring-tq` with activity `score_application(application_id, candidate_id, job_id)`:
- Fetch the candidate's parsed resume (Candidate API) and the structured job (Enrichment API).
- If the resume isn't parsed yet, raise a retryable error.
- Score = weighted skills overlap (required vs candidate skills) plus experience match. Store a breakdown (matched skills, missing skills) for later explanations.
- Add `CandidateScored` to the outbox (Mongo-based outbox collection, same pattern).
- Store in the `scores` collection from **Appendix E.11** (`_id` = application_id).
- Read API: `GET /scores/{application_id}`.

**Commit:** `feat(scoring): add rule-based scoring activity`

### Step 10.4 — Application workflow
In the Application Service, `ApplicationWorkflow(application_id)` on `application-tq`:
1. `init_stages` (local activity in Application).
2. `score_application` (on `scoring-tq`), with retry policy and an overall timeout; on timeout mark score `PENDING_MANUAL`.
3. `record_score_and_advance`: save score, move to SCREENING, write history and `ApplicationStageChanged` to the outbox.
- Signals: `withdraw` and `job_closed` stop further steps.
- Start it from `ApplicationSubmitted` with workflow ID `app-{application_id}`.

**Done when:** a new application reaches SCREENING with a score visible on the recruiter view.

**Break it:** stop the Scoring Service, apply, then start it again. The workflow resumes and completes.

**Commit:** `feat(application): add application processing workflow`

### Step 10.5 — Close-job propagation
When `JobClosed` arrives, the Application Service updates the replica, marks open SUBMITTED applications CLOSED, and signals their running workflows.

**Commit:** `feat(application): handle job closure`

---

# Phase 11 — Notification Service (half to 1 day)

**Goal:** emails are sent reliably, never twice, and an email outage never affects hiring flows.

### Step 11.1 — Scaffold with Celery and Mailpit
Add Mailpit to Compose. Tables: `notification_templates` and `notification_log` exactly as in **Appendix E.8**.

### Step 11.2 — Event consumers → Celery tasks
Consume `ApplicationSubmitted` ("Application received"), `ApplicationStageChanged` ("Your application moved to …"), `ApplicationDecisionMade`, `JobReady` / `JobPublishFailed` (to the recruiter). The consumer creates a log row with a deterministic `notification_key` (for example `event_id + template`) and enqueues `send_email(notification_id)`.

### Step 11.3 — Sending with retries and a circuit breaker
`send_email` checks the log (skip if SENT), renders the template, sends via SMTP, updates status. Autoretry with backoff. Wrap SMTP in a circuit breaker (`pybreaker`).

> **Note:** emails need recipient addresses. Include the candidate email in the relevant event payloads, or have the Notification Service fetch contact info from Identity using a service token. Ask Claude to explain the tradeoff and choose one.

**Done when:** emails appear in Mailpit; replaying events doesn't send duplicates; stopping Mailpit queues retries without affecting applications.

**Commit:** `feat(notification): add idempotent email notifications`

---

# Phase 12 — Analytics Service (1 day)

**Goal:** all 9 metrics served from a read model built purely from events (CQRS).

### Step 12.1 — Projection tables
Create the projection tables from **Appendix E.9** (`metric_counters`, `applications_daily`, `job_application_counts`, `candidate_application_counts`, `application_timeline`, `failures_daily`, `ai_usage_daily`). All updates are upserts in the same transaction as the `processed_events` insert.

### Step 12.2 — Consumers per event
Subscribe to `identity.users`, `jobs.lifecycle`, `applications.lifecycle`, `scoring.results`, and a `system.failures` topic (Phase 13). Each handler updates projections idempotently (processed_events).

### Step 12.3 — Metrics API
| Metric | Source |
| --- | --- |
| Total Candidates / Recruiters | UserRegistered by role |
| Total Jobs Published | JobReady (first version only) |
| New Applications Over Time | applications_daily |
| Application-to-Interview Rate | applications reaching an interview stage / total |
| Average Time to Hire | hired_at − submitted_at |
| Most Applied Jobs | job_application_counts top N |
| Average Applications per Candidate | total applications / distinct candidates |
| Failed Workflows / System Errors | system.failures events |

Cache hot metrics in Redis for a few seconds.

### Step 12.4 — Rebuild capability
Add an admin command that resets the projections and replays topics from the beginning. This proves the read model can always be rebuilt from events.

**Done when:** metrics match what you created manually; a full rebuild produces identical numbers.

**Commit:** `feat(analytics): add event-driven metrics`

---

# Phase 12B — Backend for Frontend: API composition (1 day)

**Goal:** screens that need data from several services get it in one call, without putting business logic in the gateway and without breaking database-per-service. See Appendix F.1.

**Concepts to learn first:** API composition vs CQRS replicas; BFF pattern; fan-out with timeouts; partial responses and graceful degradation.

### Step 12B.1 — Scaffold `services/bff_web`
From the template, port 8011, **no database** (optionally a short Redis cache). Route `/api/bff/*` through APISIX (Phase 4.2 table).

### Step 12B.2 — Composed endpoints
Only for screens that replicas don't already serve:
- `GET /bff/applications/{id}` (recruiter or the candidate): application (Application) + candidate profile (Candidate) + score breakdown (Scoring) + a short-lived resume download link (Candidate).
- `GET /bff/recruiter/dashboard`: the recruiter's jobs (Job) + application counts per stage (Application) + headline metrics (Analytics).

Rules:
- Forward the **user's token** to every service: each service still checks role and ownership (rule 7). The BFF adds no permissions of its own.
- Call services **in parallel** with per-call timeouts and circuit breakers.
- **Partial responses:** if a non-essential call fails (e.g. Scoring), return the rest with `"score": null` and a `degraded` list, instead of failing the whole page.
- No business rules: the BFF only fetches and shapes.

> **Prompt:** "Implement GET /bff/applications/{id} in services/bff_web: call Application, Candidate and Scoring in parallel with httpx (forwarding the caller's Authorization header), each with a timeout and circuit breaker, and return a composed response that marks failed optional parts as degraded. Explain why this lives in a BFF and not in the gateway."

**Done when:** one call returns the composed application view; stopping Scoring returns the view with `score: null` and `degraded: ["score"]`; a recruiter who doesn't own the job gets 403 (from Application, passed through).

**Check your understanding:** When would you choose a replica (CQRS) over a BFF call? What breaks if the BFF used its own service token instead of the user's?

**Commit:** `feat(bff): add web BFF with composed application and dashboard views`

---

# Phase 13 — Observability (1.5 days)

**Goal:** you can follow one application from the HTTP request to the email in a single trace, and see the health of every workflow on a dashboard.

**Concepts to learn first:** traces, spans, context propagation; metric types (counter, gauge, histogram); RED metrics (rate, errors, duration).

### Step 13.1 — Add Jaeger, Prometheus, Grafana
Add containers and `infra/prometheus/prometheus.yml` scraping every service's `/metrics`.

### Step 13.2 — OpenTelemetry in the chassis
> **Prompt:** "Add smarthire_common/telemetry.py: configure OpenTelemetry tracing with an OTLP exporter to Jaeger, auto-instrument FastAPI, httpx, SQLAlchemy, Redis, and pymongo; add trace_id to every log line. Call it from each service's startup."

### Step 13.3 — Propagate context through async boundaries
- **Kafka:** inject trace context into message headers on publish (outbox stores it); extract on consume.
- **Celery:** pass context in task headers.
- **Temporal:** add the Temporal OpenTelemetry interceptor to clients and workers.

**Done when:** in Jaeger, one trace shows gateway (APISIX) → application → outbox relay → Kafka → notification → Celery → SMTP, and the workflow activities.

### Step 13.4 — Metrics
In the chassis: HTTP request count, latency histogram, error count. Domain metrics: workflows started/completed/failed (by type), outbox backlog, consumer lag, DLQ size, Celery queue depth, applications submitted.

### Step 13.5 — Failures topic
Workflow failure handlers and DLQ writers publish a `SystemFailure` event to `system.failures` (consumed by Analytics).

### Step 13.6 — Grafana dashboards
Create dashboards: Service overview (RED per service), Workflows (started/failed, durations), Messaging (lag, DLQ, outbox backlog), Business (applications per minute). Export them as JSON into `infra/grafana/dashboards/`.

### Step 13.7 — Gateway telemetry
APISIX `prometheus` plugin scraped by Prometheus (requests, latency, status codes per route); `opentelemetry` plugin exporting spans to Jaeger so every trace starts at the gateway.

### Step 13.8 — Observing Kafka vs Temporal (see Appendix F.2)
- **Temporal (orchestration):** answer "where is this business process stuck?" in the Temporal UI by workflow ID (`publish-{job_id}-{version}`, `app-{application_id}`). Metrics: workflows started/completed/failed per type, activity retries, schedule-to-start latency (are workers keeping up?), workflow duration. Alert on failed workflows and rising schedule-to-start latency.
- **Kafka (choreography):** no central view, so rely on: consumer lag per group, DLQ size, outbox backlog, events/s per topic, and traces that follow `event_id` + trace context in headers. Alert on lag growth, any DLQ message, outbox backlog older than N seconds.
- Dashboard "Async processing": one row per workflow type, one row per consumer group.

**Done when:** you can show, for one application, its Temporal history AND the Kafka events it produced, linked by one trace ID.

**Commit:** `feat(observability): add tracing, metrics, and dashboards`

---

# Phase 14 — Reliability hardening and load testing (1 day)

**Goal:** prove the system survives a hiring spike and partial failures.

### Step 14.1 — Review every failure path
Ask Claude: *"List every place in the code where a network call happens. For each, show the timeout, retry policy, and what happens on final failure."* Fix any gaps.

### Step 14.2 — Circuit breakers on synchronous calls
Apply breakers to BFF → service calls (API composition, Phase 12B) and to the AI Assistant's tool calls; configure APISIX upstream timeouts and passive health checks.

### Step 14.3 — Backpressure settings
Tune: APISIX rate limits, Celery prefetch (1–4), Kafka consumer `max.poll.records`, Temporal worker `max_concurrent_activities`, DB pool sizes per service.

### Step 14.3b — Data retention jobs
Implement the scheduled maintenance jobs from **Appendix E.14** (outbox, processed_events, refresh tokens, notification log cleanup).

**Commit:** `feat: add data retention jobs`

### Step 14.4 — Load test with Locust
Script: register many candidates, publish a few jobs, then 500–2000 concurrent users applying. Watch Grafana.

**Done when:** no lost applications (DB count equals successful 202s), no duplicates, all workflows eventually complete, error rate stays near zero apart from expected 429s.

### Step 14.5 — Chaos checks
During the load test, stop and restart one at a time: Kafka, Scoring, Notification, a Temporal worker. Verify everything converges after recovery.

**Commit:** `test: add load tests and reliability fixes`

### Step 14.6 — Part A checkpoint
Write `docs/part-a-checkpoint.md`: what works, how you tested it, screenshots of Jaeger and Grafana. Tag the commit `v0.1-core`.

---

# Phase 15 — Semantic Index Service (Week 4, 2–3 days)

**Goal:** jobs and resumes become searchable by meaning, and the data is ready for AI ranking and the assistant.

**Concepts to learn first:** embeddings; cosine similarity; vector databases; metadata filtering; chunking.

### Step 15.1 — Add Qdrant and scaffold the service
Add Qdrant to Compose. Scaffold `services/semantic_index`. Create collections `jobs` and `resumes` with the point IDs, payload fields, and payload indexes from **Appendix E.12**.

### Step 15.2 — Embedding provider abstraction
> **Prompt:** "Create an EmbeddingProvider interface with two implementations: local sentence-transformers (all-MiniLM-L6-v2) and an API provider. Choose via config. Explain embedding dimensions and why the collection size must match the model."

Start with the local model (free, no API key).

### Step 15.3 — Job embedding as a saga step
- Activities on `semantic-tq`: `embed_and_index_job(job_id, version)` (reads the structured job from Enrichment, builds the text, upserts the vector) and `delete_job_vector(job_id, version)` (compensation).
- Add the step to `JobPublishingWorkflow` **before** `mark_ready`, and its compensation before `delete_structured_job`.

**Done when:** a job is READY only after its vector exists; failing the embedding step compensates both the vector and the structured document.

**Commit:** `feat(semantic): add job embedding to publishing saga`

### Step 15.4 — Resume embedding from events
Consume `ResumeParsed`, build text from the parsed data, upsert into `resumes`. Idempotent by `(candidate_id, resume_id)`.

**Commit:** `feat(semantic): index resumes from events`

### Step 15.5 — Search and retrieval APIs
- `POST /search/semantic` (recruiter): find candidates matching a job or free text, filtered to applicants of the recruiter's jobs where relevant.
- `POST /retrieve` (internal, service token): top-k with **mandatory filters** passed by the caller (candidate_id or recruiter_id). Reject requests without a filter.

### Step 15.6 — Semantic score in the Scoring Service
Add a semantic similarity component (resume vector vs job vector) to the score with a configurable weight, keeping the rule-based breakdown for explanations.

**Commit:** `feat: add semantic search, retrieval, and semantic scoring`

---

# Phase 16 — AI Assistant Service (Week 5, 3–4 days)

**Goal:** a recruiter assistant and a candidate guidance assistant that stream answers, never block users, and never leak data.

**Concepts to learn first:** LLM basics (tokens, context window, temperature); RAG (retrieval-augmented generation); LangGraph nodes, edges, state, tools; Server-Sent Events.

### Online vs offline paths (see Appendix F.5)
| Path | When | How |
| --- | --- | --- |
| **Online** | A user is waiting for an answer | SSE streaming (Steps 16.1, 16.5); retrieval only, no heavy work; strict LLM timeout + circuit breaker |
| **Offline: background jobs** | Work too big to wait for (rank 400 applicants) | 202 + job id; Temporal workflow in batches (Step 16.6); client polls |
| **Offline: prepared ahead** | Make online answers fast | Embeddings computed when data changes (Phase 15: publishing saga, `ResumeParsed` consumer), never at question time |
| **Degraded** | LLM provider down | Breaker opens: fail fast with 503 + clear message; fall back to non-LLM answers where possible (e.g. ranking explained from the Scoring breakdown); hiring flows unaffected |

### Step 16.1 — Scaffold and LLM client
Scaffold `services/ai_assistant` (MongoDB for conversations). Create an LLM client factory (OpenAI / Groq / Anthropic via LangChain chat models) chosen by config, with the API key from env. Wrap calls in a circuit breaker and timeouts.

> **Prompt:** "Create an LLM client factory supporting OpenAI, Groq, and Anthropic chat models via LangChain, selected by env config, wrapped with a timeout and a circuit breaker. Add a /assistant/ping endpoint that streams a short reply over SSE. Explain how SSE works."

**Commit:** `feat(assistant): add LLM client and SSE streaming`

### Step 16.2 — Tools
Implement tools as plain async functions that call services with the **user's token** forwarded:
- `get_job(job_id)`, `get_structured_job(job_id)`
- `get_applicants(job_id)` (recruiter only)
- `get_candidate_profile(candidate_id)`
- `get_score(application_id)`
- `semantic_retrieve(query, filters)`

Every tool enforces filters from the JWT (candidate_id for candidates, recruiter_id for recruiters), never from the user's message.

### Step 16.3 — Recruiter agent (LangGraph)
Graph: classify intent → (shortlist | summarize resume | optimize JD | explain ranking) → gather context with tools → generate → stream.
- Ranking explanations use the Scoring breakdown (matched and missing skills), not free invention.

### Step 16.4 — Candidate guidance agent
Intents: fit check, application improvement, requirement explanation. Context: the job (public data) and the candidate's **own** profile only.

**Break it:** as a candidate, ask "Show me the other applicants for this job." Confirm no other candidate data is retrievable.

### Step 16.5 — Endpoints and conversation storage
- `POST /assistant/recruiter` and `POST /assistant/candidate`: SSE streaming responses.
- Store conversations and messages in the separate `conversations` and `messages` collections from **Appendix E.11** (no unbounded arrays).
- After each answer, add `AssistantQueryCompleted` (user role, intent, tokens, latency) to the outbox.

### Step 16.6 — Long-running assistant jobs
`POST /assistant/jobs` (for example, "rank all 400 applicants for job X") returns 202 + job_id and runs as a Temporal workflow (batching scoring and summarization). `GET /assistant/jobs/{id}` returns progress and results.

### Step 16.7 — AI usage metrics and tracing
- Analytics consumes `AssistantQueryCompleted`: recruiter queries, candidate queries, summaries generated.
- Spans for each graph node and LLM call with token counts and model name; Grafana panel for LLM latency and tokens.

**Done when:** both assistants stream answers through the gateway, long jobs run in the background, and AI usage metrics appear. **Break it:** block the LLM provider (wrong API key or no network): the assistant answers 503 within its timeout, the ranking explanation still works from the Scoring breakdown, and applications keep flowing.

**Commit:** `feat(assistant): add recruiter and candidate agents`

---

# Phase 17 — Testing, documentation, and final polish (2 days)

### Step 17.1 — Contract tests
- CI script checks every schema change against Schema Registry compatibility.
- Consumer tests validate sample events against the registered schemas.

### Step 17.2 — End-to-end tests
In `tests/e2e/`: recruiter registers → creates and publishes a job → candidate registers → uploads resume → applies → application reaches SCREENING with a score → email in Mailpit → metrics updated → assistant answers a fit question.

### Step 17.3 — README
Include: overview, architecture diagram, how to run (`make up`), service list with ports, how to run tests, how to view Jaeger, Grafana, Temporal UI, Kafka UI, Mailpit, and the main design decisions.

### Step 17.4 — Final review with Claude
Ask Claude to review the whole repository against `CLAUDE.md`'s architecture rules and list violations. Fix them.

**Commit and tag:** `v1.0`

---

# Final mastery checklist

You understand your system when you can do each of these **yourself**, then confirm with Claude:

**Explain without notes**
- [ ] The full path of an application from HTTP request to email, naming every component.
- [ ] Why the outbox exists and what happens if Kafka is down.
- [ ] How a duplicate application is prevented at three different layers.
- [ ] How the Job Publishing saga compensates, step by step.
- [ ] How a JWT is signed, verified, refreshed, and revoked, and where a revoked token is stopped (gateway vs service).
- [ ] When to compose data in the BFF vs keep a replica, and why never in the gateway.
- [ ] For a given flow, whether it belongs in Kafka or Temporal, and what you'd look at to debug it.
- [ ] Why Analytics can be deleted and rebuilt with identical results.
- [ ] Every constraint and index in the Application schema, and the query or rule each one protects.
- [ ] Why the quota row lock prevents the application-limit race and why counting rows does not.

**Change things on your own**
- [ ] Add a new field to the Job (model, migration, schema, event, replica) without breaking consumers.
- [ ] Add a new hiring metric to Analytics.
- [ ] Add a new notification for "Job closed" to candidates who applied.
- [ ] Add a new step to the Application workflow (for example, an automatic knockout question check).
- [ ] Change the application limit rule from a global default to per-job.
- [ ] Swap the LLM provider through configuration only.
- [ ] Add a new event consumer in a brand-new service.

**Database skills**
- [ ] Run `EXPLAIN ANALYZE` on the recruiter board query with 100k fake applications and confirm an index scan.
- [ ] Remove the quota lock, reproduce the race with concurrent applies, then restore it.
- [ ] Write an expand → migrate → contract migration that renames a column with zero downtime.

**Debug on your own**
- [ ] Find why a specific application is stuck using Temporal UI and Jaeger.
- [ ] Reprocess messages from a DLQ.
- [ ] Find which service is slow during a load test using Grafana.

---

# Appendix A — Kafka topics

| Topic | Key | Events | Consumers |
| --- | --- | --- | --- |
| identity.users | user_id | UserRegistered | candidate, analytics |
| jobs.lifecycle | job_id | JobUpdated, JobReady, JobPublishFailed, JobClosed | application, notification, analytics |
| jobs.commands | job_id | JobPublishRequested | job (workflow starter) |
| candidates.profile | candidate_id | ResumeUploaded, ResumeParsed | semantic_index, analytics |
| applications.lifecycle | application_id | ApplicationSubmitted, ApplicationStageChanged, ApplicationDecisionMade | application (workflow starter), notification, analytics |
| scoring.results | application_id | CandidateScored | analytics |
| assistant.usage | user_id | AssistantQueryCompleted | analytics |
| system.failures | source | SystemFailure | analytics |
| *.dlq | same | failed messages | manual reprocessing |

# Appendix B — Temporal task queues

| Task queue | Worker lives in | Runs |
| --- | --- | --- |
| job-tq | job | JobPublishingWorkflow, set_processing, mark_ready, mark_failed |
| enrichment-tq | job_enrichment | extract_skills, breakdown_jd, store_structured_job, delete_structured_job |
| semantic-tq | semantic_index | embed_and_index_job, delete_job_vector |
| application-tq | application | ApplicationWorkflow, init_stages, record_score_and_advance |
| scoring-tq | scoring | score_application |
| assistant-tq | ai_assistant | long-running assistant jobs |

# Appendix C — Reusable prompts for Claude

**Start of a step**
> "We are on Step X.Y of the implementation plan: [paste step]. Read CLAUDE.md. First explain the concept and your plan in simple terms, list the files you will create or change, and wait for my OK."

**After code is generated**
> "Walk me through the code file by file. For each function, tell me what it does, why it exists, and what would break without it."

**Testing**
> "Give me curl commands to test this manually, including failure cases, and write pytest tests for the same cases."

**Debugging**
> "Here is the error and the relevant logs: [paste]. Explain the root cause before changing anything. Then propose the smallest fix."

**Review**
> "Review this service against the architecture rules in CLAUDE.md. List any violations and explain each."

**Understanding check**
> "Quiz me with 5 questions about what we built in this step. Don't give answers until I reply."

# Appendix D — Common problems

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| Service can't reach `localhost:5432` inside Docker | Containers use service names | Use `postgres:5432` in DATABASE_URL |
| Alembic creates tables in `public` | Schema not set | Set `version_table_schema` and `search_path` per service |
| Kafka consumer receives nothing | Wrong group/offset or topic name | Check Kafka UI; set `auto.offset.reset=earliest` for new groups |
| Same event processed twice | Missing idempotency | Insert into processed_events in the same transaction as the handler |
| Temporal workflow fails with non-determinism error | Network call, `datetime.now()`, or random inside workflow code | Move it into an activity; use `workflow.now()` |
| Celery task runs twice after a crash | `acks_late` redelivery | Make the task check state before acting |
| Traces break at Kafka | Context not in headers | Inject on publish, extract on consume |
| JWT verification fails after key change | Cached JWKS | Refresh JWKS on unknown `kid` |
| 401 vs 403 confusion | — | 401 = bad or missing token; 403 = valid token, not allowed |

# Appendix E — Database design reference

This is the source of truth for every schema. When a step says "create the models," point Claude to the matching section here:

> "Create SQLAlchemy models and an Alembic migration exactly matching Appendix E.<n> of the plan. Explain every constraint and index before writing code."

## E.1 Design principles (apply everywhere)

1. **Database per service.** No foreign keys or queries across services. Other services' entities are referenced by ID only; consistency comes from events.
2. **Rules live in the database too.** Use `NOT NULL`, `UNIQUE`, `CHECK`, foreign keys within a service, and partial unique indexes. Code validates for friendly errors; the database is the last line of defense.
3. **Normalize first; denormalize only on purpose.** Every copied field must have a named query that needs it and a documented way it stays in sync.
4. **Data types:**
   - Public IDs: **UUIDv7** generated in the app (the `uuid6` package on Python 3.12). Time-ordered, so indexes stay compact.
   - Internal append-only tables: `bigint GENERATED ALWAYS AS IDENTITY`.
   - Time: `timestamptz`, always UTC.
   - Statuses: `text` + `CHECK (... IN (...))` instead of Postgres `ENUM`.
   - Emails: unique index on `lower(email)`.
   - `jsonb` only for data read and written as a whole, never for columns you filter or join on.
5. **Indexes come from queries.** For every endpoint query, index the `WHERE` columns, then the `ORDER BY` columns. No speculative indexes.
6. **Concurrency is designed, not hoped for.** Default isolation is READ COMMITTED. Use a row lock (`SELECT … FOR UPDATE`) for read-then-write rules, and a `version` column for optimistic locking on user edits.
7. **History is append-only.** Never update or delete history or audit rows.
8. **Every growing table has a retention or partitioning plan.**
9. **Files never go in the database.** Store them in object storage and keep only the key.
10. **Events carry IDs, not personal data,** except where a consumer genuinely needs a field.

## E.2 Conventions

- Tables are plural and snake_case; columns are snake_case; references are `<entity>_id`.
- Every mutable table has `created_at` and `updated_at` (`timestamptz NOT NULL DEFAULT now()`); `updated_at` is set in the repository layer on every update.
- Explicit constraint names, configured once in the chassis with SQLAlchemy `MetaData(naming_convention=...)`:
  - primary key `pk_<table>`, foreign key `fk_<table>_<column>_<referred_table>`, unique `uq_<table>_<columns>`, check `ck_<table>_<name>`, index `ix_<table>_<columns>`.
- Each service uses its own schema and its own Alembic version table.

## E.3 Migration rules

- Never edit a migration that has already run; write a new one.
- Every migration has a working downgrade. Test upgrade → downgrade → upgrade locally.
- Use **expand → migrate → contract** for breaking changes: add a nullable column, deploy code that writes both, backfill, then add `NOT NULL` and drop the old column.
- On large tables, create indexes with `CREATE INDEX CONCURRENTLY` (in a migration without a transaction).

## E.4 Identity (PostgreSQL, schema `identity`)

```sql
CREATE TABLE users (
  id             uuid PRIMARY KEY,
  email          text NOT NULL,
  password_hash  text NOT NULL,
  role           text NOT NULL CHECK (role IN ('CANDIDATE','RECRUITER','ADMIN')),
  status         text NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE','SUSPENDED')),
  token_version  int  NOT NULL DEFAULT 1,
  created_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX uq_users_email_lower ON users (lower(email));

CREATE TABLE recruiter_profiles (
  user_id       uuid PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
  display_name  text NOT NULL,
  company       text,
  created_at    timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE refresh_tokens (
  id          uuid PRIMARY KEY,
  user_id     uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  family_id   uuid NOT NULL,
  token_hash  bytea NOT NULL UNIQUE,
  user_agent  text,
  expires_at  timestamptz NOT NULL,
  used_at     timestamptz,
  revoked_at  timestamptz,
  created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_refresh_tokens_user_id   ON refresh_tokens (user_id);
CREATE INDEX ix_refresh_tokens_family_id ON refresh_tokens (family_id);
```

| Decision | Reason |
| --- | --- |
| One `users` table for all roles | One account store; role is an attribute |
| `recruiter_profiles.user_id` as primary key | Database enforces one profile per user |
| `lower(email)` unique | Case-insensitive uniqueness |
| `token_version` | "Log out everywhere" by bumping one number; included as a JWT claim |
| `token_hash` (SHA-256), unique | Leaked database rows can't be used; lookup is one index hit |
| `family_id` index | Reuse detection revokes a whole session family |
| Retention | Delete rows expired or revoked more than 30 days ago (daily job) |

Plus the shared `outbox` table (E.10).

## E.5 Job (PostgreSQL, schema `job`)

```sql
CREATE TABLE jobs (
  id                 uuid PRIMARY KEY,
  recruiter_id       uuid NOT NULL,
  title              text NOT NULL CHECK (length(title) BETWEEN 3 AND 200),
  description        text NOT NULL,
  location           text,
  employment_type    text NOT NULL CHECK (employment_type IN ('FULL_TIME','PART_TIME','CONTRACT','INTERNSHIP')),
  required_skills    jsonb NOT NULL DEFAULT '[]',
  eligibility_rules  jsonb NOT NULL DEFAULT '{}',
  application_limit  int CHECK (application_limit > 0),
  deadline           timestamptz,
  status             text NOT NULL DEFAULT 'DRAFT'
                     CHECK (status IN ('DRAFT','PROCESSING','READY','FAILED','CLOSED')),
  version            int  NOT NULL DEFAULT 1,
  published_at       timestamptz,
  created_at         timestamptz NOT NULL DEFAULT now(),
  updated_at         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_jobs_recruiter_status_created ON jobs (recruiter_id, status, created_at DESC);
CREATE INDEX ix_jobs_ready_created ON jobs (created_at DESC) WHERE status = 'READY';

CREATE TABLE job_stages (
  id        uuid PRIMARY KEY,
  job_id    uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  name      text NOT NULL,
  position  int  NOT NULL CHECK (position >= 0),
  CONSTRAINT uq_job_stages_job_position UNIQUE (job_id, position),
  CONSTRAINT uq_job_stages_job_name     UNIQUE (job_id, name)
);
```

| Decision | Reason |
| --- | --- |
| `recruiter_id` without a foreign key | The user lives in Identity |
| Skills as `jsonb`, stages as a table | Skills are read as one list; stages need order and uniqueness rules |
| Partial index on READY jobs | The public listing only shows READY jobs |
| `version` | Content version for the publishing workflow, and an optimistic lock: `UPDATE … WHERE id = :id AND version = :expected`; 0 rows updated → 409 |

Plus `outbox` (E.10) and `processed_events` (E.10) if the service consumes events.

## E.6 Candidate (PostgreSQL, schema `candidate` + MinIO)

```sql
CREATE TABLE candidates (
  id                uuid PRIMARY KEY,               -- equals Identity user_id
  full_name         text NOT NULL DEFAULT '',
  headline          text,
  skills            jsonb NOT NULL DEFAULT '[]',
  experience_years  numeric(4,1) CHECK (experience_years >= 0),
  education         jsonb NOT NULL DEFAULT '[]',
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE resumes (
  id            uuid PRIMARY KEY,
  candidate_id  uuid NOT NULL REFERENCES candidates(id) ON DELETE CASCADE,
  object_key    text NOT NULL UNIQUE,
  file_name     text NOT NULL,
  content_type  text NOT NULL CHECK (content_type IN
                  ('application/pdf','application/vnd.openxmlformats-officedocument.wordprocessingml.document')),
  size_bytes    int  NOT NULL CHECK (size_bytes > 0 AND size_bytes <= 5242880),
  status        text NOT NULL DEFAULT 'UPLOADED' CHECK (status IN ('UPLOADED','PARSED','FAILED')),
  parsed_data   jsonb,
  parse_error   text,
  is_active     boolean NOT NULL DEFAULT true,
  uploaded_at   timestamptz NOT NULL DEFAULT now(),
  parsed_at     timestamptz
);
CREATE UNIQUE INDEX uq_resumes_one_active ON resumes (candidate_id) WHERE is_active;
CREATE INDEX ix_resumes_candidate_uploaded ON resumes (candidate_id, uploaded_at DESC);
```

| Decision | Reason |
| --- | --- |
| `candidates.id` = `user_id` | Shared identifier across contexts, not a foreign key |
| Files in MinIO, key in DB | Keeps the database small and backups fast |
| Partial unique index on `is_active` | At most one active resume per candidate, even under concurrent uploads. Deactivate the old row before activating the new one in the same transaction. |
| Size and type `CHECK`s | Defense in depth behind API validation |

Plus `outbox` and `processed_events` (E.10).

## E.7 Application (PostgreSQL, schema `application`) — the core

```sql
CREATE TABLE job_replica (
  job_id             uuid PRIMARY KEY,
  recruiter_id       uuid NOT NULL,
  title              text NOT NULL,
  status             text NOT NULL,
  stages             jsonb NOT NULL,
  eligibility_rules  jsonb NOT NULL DEFAULT '{}',
  application_limit  int,
  deadline           timestamptz,
  source_version     int NOT NULL,
  updated_at         timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE applications (
  id              uuid PRIMARY KEY,
  job_id          uuid NOT NULL REFERENCES job_replica(job_id),
  candidate_id    uuid NOT NULL,
  candidate_name  text NOT NULL,
  status          text NOT NULL DEFAULT 'SUBMITTED' CHECK (status IN
                    ('SUBMITTED','SCREENING','IN_PROGRESS','HIRED','REJECTED','WITHDRAWN','CLOSED')),
  current_stage   text NOT NULL,
  score           numeric(5,2) CHECK (score BETWEEN 0 AND 100),
  score_status    text NOT NULL DEFAULT 'PENDING'
                    CHECK (score_status IN ('PENDING','SCORED','PENDING_MANUAL')),
  version         int NOT NULL DEFAULT 1,
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT uq_applications_candidate_job UNIQUE (candidate_id, job_id)
);
CREATE INDEX ix_applications_job_stage_score   ON applications (job_id, current_stage, score DESC NULLS LAST);
CREATE INDEX ix_applications_candidate_created ON applications (candidate_id, created_at DESC);

CREATE TABLE application_stage_history (
  id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  application_id  uuid NOT NULL REFERENCES applications(id),
  from_stage      text,
  to_stage        text NOT NULL,
  changed_by      uuid NOT NULL,
  reason          text,
  changed_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_stage_history_app_changed ON application_stage_history (application_id, changed_at);

CREATE TABLE application_notes (
  id              uuid PRIMARY KEY,
  application_id  uuid NOT NULL REFERENCES applications(id),
  recruiter_id    uuid NOT NULL,
  body            text NOT NULL CHECK (length(body) > 0),
  created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_application_notes_app_created ON application_notes (application_id, created_at);

CREATE TABLE application_quotas (
  candidate_id  uuid PRIMARY KEY,
  active_count  int NOT NULL DEFAULT 0 CHECK (active_count >= 0)
);

CREATE TABLE audit_entries (
  id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  actor_id     uuid NOT NULL,
  action       text NOT NULL,             -- STAGE_CHANGED, NOTE_ADDED, DECISION_MADE
  entity_type  text NOT NULL,
  entity_id    uuid NOT NULL,
  before       jsonb,
  after        jsonb,
  created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_audit_entries_entity ON audit_entries (entity_type, entity_id, created_at);
```

| Decision | Reason |
| --- | --- |
| `job_replica` | Self-contained apply: eligibility checked locally, no call to Job |
| `source_version` | Upsert only newer events: `ON CONFLICT (job_id) DO UPDATE … WHERE job_replica.source_version < EXCLUDED.source_version` |
| FK `applications.job_id → job_replica` | Legitimate: the replica is a local table |
| `UNIQUE (candidate_id, job_id)` | Final guard against duplicates under races (409). Business rule: no reapplying to the same job. If reapplying after withdrawal is ever allowed, replace with a partial unique index `WHERE status <> 'WITHDRAWN'`. |
| `application_quotas` + `FOR UPDATE` | Counting rows can't be locked (phantom rows); one quota row can. `CHECK (active_count >= 0)` catches decrement bugs. |
| `candidate_name` snapshot | Recruiter board in one query; snapshot at apply time is acceptable for display |
| Board index `(job_id, current_stage, score DESC)` | Serves filter and sort of the main recruiter query |
| `version` | Optimistic lock for recruiter edits (two recruiters moving the same application) |
| History and audit append-only, `bigint` keys | Internal, high-volume, never updated |
| Partitioning (later) | `application_stage_history` and `audit_entries` by month once they reach tens of millions of rows |

Plus `outbox` and `processed_events` (E.10). Idempotency-Key responses live in Redis (`idem:{user_id}:{key}`, 24 h TTL), not in Postgres.

## E.8 Notification (PostgreSQL, schema `notification`)

```sql
CREATE TABLE notification_templates (
  code        text PRIMARY KEY,           -- APPLICATION_RECEIVED, STAGE_CHANGED, ...
  subject     text NOT NULL,
  body        text NOT NULL,
  updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE notification_log (
  id                uuid PRIMARY KEY,
  notification_key  text NOT NULL UNIQUE,  -- event_id + ':' + template code
  recipient         text NOT NULL,
  template_code     text NOT NULL REFERENCES notification_templates(code),
  payload           jsonb NOT NULL,
  status            text NOT NULL DEFAULT 'QUEUED' CHECK (status IN ('QUEUED','SENT','FAILED')),
  attempts          int NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  last_error        text,
  created_at        timestamptz NOT NULL DEFAULT now(),
  sent_at           timestamptz
);
CREATE INDEX ix_notification_log_status_created ON notification_log (status, created_at);
```

`notification_key` unique makes each email idempotent at the database level. Retention: delete SENT rows after 90 days.

## E.9 Analytics (PostgreSQL, schema `analytics` + Redis cache)

```sql
CREATE TABLE metric_counters (
  name   text PRIMARY KEY,                 -- total_candidates, total_recruiters, total_jobs_published
  value  bigint NOT NULL DEFAULT 0 CHECK (value >= 0)
);
CREATE TABLE applications_daily (
  day    date PRIMARY KEY,
  count  int NOT NULL DEFAULT 0
);
CREATE TABLE job_application_counts (
  job_id  uuid PRIMARY KEY,
  title   text NOT NULL,
  count   int NOT NULL DEFAULT 0
);
CREATE INDEX ix_job_application_counts_count ON job_application_counts (count DESC);
CREATE TABLE candidate_application_counts (
  candidate_id  uuid PRIMARY KEY,
  count         int NOT NULL DEFAULT 0
);
CREATE TABLE application_timeline (
  application_id  uuid PRIMARY KEY,
  job_id          uuid NOT NULL,
  candidate_id    uuid NOT NULL,
  submitted_at    timestamptz NOT NULL,
  interview_at    timestamptz,
  hired_at        timestamptz
);
CREATE TABLE failures_daily (
  day     date NOT NULL,
  source  text NOT NULL,
  count   int NOT NULL DEFAULT 0,
  PRIMARY KEY (day, source)
);
CREATE TABLE ai_usage_daily (
  day    date NOT NULL,
  role   text NOT NULL,                    -- RECRUITER, CANDIDATE
  kind   text NOT NULL,                    -- QUERY, SUMMARY
  count  int NOT NULL DEFAULT 0,
  PRIMARY KEY (day, role, kind)
);
```

All updates are upserts (`INSERT … ON CONFLICT DO UPDATE SET count = count + 1`) inside the same transaction as the `processed_events` insert. This schema is a **disposable read model**: it can be dropped and rebuilt by replaying events.

## E.10 Shared tables (every PostgreSQL service that publishes or consumes)

```sql
CREATE TABLE outbox (
  id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  event_id      uuid NOT NULL UNIQUE,
  topic         text NOT NULL,
  message_key   text NOT NULL,
  event_type    text NOT NULL,
  payload       jsonb NOT NULL,
  headers       jsonb NOT NULL DEFAULT '{}',   -- trace context
  created_at    timestamptz NOT NULL DEFAULT now(),
  published_at  timestamptz
);
CREATE INDEX ix_outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

CREATE TABLE processed_events (
  consumer_name  text NOT NULL,
  event_id       uuid NOT NULL,
  processed_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_name, event_id)
);
```

| Decision | Reason |
| --- | --- |
| Partial index on unpublished rows | The relay query stays fast however large the table grows |
| Composite primary key on `processed_events` | The idempotency check itself: duplicate insert = already processed |
| Outbox retention | Delete published rows older than 7 days (daily job) |
| `processed_events` retention | Keep longer than Kafka topic retention (e.g., Kafka 7 days → keep 30 days) |

## E.11 MongoDB collections

**Job Enrichment — `structured_jobs`**
```js
{ _id: "<job_id>:<version>", job_id, version,
  skills: [...], keywords: [...],
  sections: { requirements: [...], responsibilities: [...], benefits: [...] },
  created_at }
// indexes: { job_id: 1, version: -1 }; text index on keywords and sections
```

**Scoring — `scores`**
```js
{ _id: "<application_id>", job_id, candidate_id,
  score, rule_score, semantic_score,
  breakdown: { matched_skills: [...], missing_skills: [...], experience_match },
  scored_at }
// indexes: { job_id: 1, score: -1 }
```

**AI Assistant — `conversations`, `messages`, `assistant_jobs`**
```js
conversations:  { _id, user_id, role, agent, title, created_at, updated_at }
messages:       { _id, conversation_id, sender, content, tokens_in, tokens_out, created_at }
assistant_jobs: { _id, user_id, kind, params, status, progress, result, created_at, finished_at }
// indexes: conversations { user_id: 1, updated_at: -1 }; messages { conversation_id: 1, created_at: 1 };
//          assistant_jobs { user_id: 1, created_at: -1 }
```

**Shared in Mongo services:** an `outbox` collection with the same fields as E.10 and an index on `{ published_at: 1 }`, and a `processed_events` collection with a unique index on `{ consumer_name: 1, event_id: 1 }`.

| Decision | Reason |
| --- | --- |
| Deterministic `_id` | Upserts are idempotent: a Temporal retry rewrites the same document |
| Messages in their own collection | Avoids unbounded arrays and the 16 MB document limit |
| Indexes match read paths | Conversation list by user, messages in order, jobs by user |

## E.12 Qdrant (Semantic Index)

| Collection | Point ID | Payload | Payload indexes |
| --- | --- | --- | --- |
| `jobs` | UUID derived from `job_id + version` | job_id, version, recruiter_id, status, skills | job_id, recruiter_id, status |
| `resumes` | UUID derived from `resume_id` | resume_id, candidate_id, skills | candidate_id |

Vector size must match the embedding model (384 for all-MiniLM-L6-v2). Retrieval always includes a filter on `candidate_id` or `recruiter_id`; the payload indexes keep those filters fast and enforce data isolation.

## E.13 Redis keys

| Key pattern | Purpose | TTL |
| --- | --- | --- |
| `idem:{user_id}:{key}` | Cached response for an Idempotency-Key | 24 h |
| `denylist:{jti}` | Revoked access token | Remaining token lifetime |
| `revoked_before:{user_id}` | Reject this user's tokens issued before this timestamp (suspension, logout everywhere) | Access-token lifetime (15 min): older tokens have expired by then |
| `ratelimit:{scope}:{id}` | Token bucket state | Short (seconds to minutes) |
| `login_fail:{email}` / `login_fail_ip:{ip}` | Failed login counters | 15 min |
| `jwks:cache` | Cached public keys (optional) | 10 min |
| `metrics:{name}` | Hot analytics results | 5–30 s |

## E.14 Scheduled maintenance jobs

| Job | Service | Frequency |
| --- | --- | --- |
| Delete published outbox rows older than 7 days | All publishers | Daily |
| Delete processed_events older than 30 days | All consumers | Daily |
| Delete expired or revoked refresh tokens older than 30 days | Identity | Daily |
| Delete SENT notification_log rows older than 90 days | Notification | Weekly |

Run them as Celery beat tasks or simple scheduled scripts in each service.

# Appendix F — Architecture decisions (week-1 review)

## F.1 API composition: BFF + replicas, never the gateway
| Option | Verdict |
| --- | --- |
| Gateway joins data | **No.** Puts business logic (which fields, how to merge, what to do on partial failure) into infrastructure, couples the gateway to every service's schema, and rules out stock gateways. |
| BFF service (`bff_web`) | **Yes, for screens that need several services and fresh data.** Ordinary, testable code; forwards the user's token so services still enforce permissions. |
| Local replicas (CQRS) | **Yes, for hot paths.** E.g. Application keeps `job_replica` and a `candidate_name` snapshot so the recruiter board is one query. |

## F.2 Kafka vs Temporal
| | Temporal (orchestration) | Kafka (choreography) |
| --- | --- | --- |
| Use when | A process with an owner must finish or compensate: ordered steps, retries, timeouts, rollback | A fact happened; any number of independent consumers react; nobody waits |
| Examples | Job publishing saga; application processing; long AI jobs | UserRegistered → candidate profile; notifications; analytics; replicas; semantic indexing |
| Debugging | Temporal UI by workflow ID: every step, input/output, retries, current step, error | Traces (event_id + trace context), consumer lag, DLQ, processed_events |
| Key metrics | Started/completed/failed per type, activity retries, schedule-to-start latency, duration | Consumer lag, DLQ size, outbox backlog, throughput |

Rule of thumb: if you'd need to answer "which step is this process on, and why?", use Temporal. If you're announcing something others may care about, use Kafka. They chain: an outbox event on Kafka can start a workflow (`JobPublishRequested`). One OpenTelemetry trace ID links both.

## F.3 Token revocation: who writes, who enforces
1. Admin calls Identity: `POST /auth/revoke/{jti}` (one token), or suspends a user / the user does logout-all.
2. **Identity is the only writer:** `denylist:{jti}` and `revoked_before:{user_id}` in Redis, TTL = access-token lifetime.
3. **The gateway enforces at the edge:** after verifying the signature locally (JWKS, no call to Identity) it checks Redis; a revoked token gets 401 at the gateway and never reaches any service.
4. **Services check again** (chassis `verify_token`), for traffic that bypasses the gateway (rule 7).
5. Redis unavailable → 503 at both layers (fail closed).
6. Refresh tokens are revoked in Identity's database (logout, reuse detection), so a revoked session can't mint new access tokens.

## F.4 Gateway product
Apache APISIX (open source, Apache 2.0), standalone mode with YAML config in git. Chosen because its open-source edition verifies JWTs against a JWKS endpoint (`openid-connect`, `bearer_only`, `use_jwks`), shares rate-limit counters in Redis (`limit-count`, `policy: redis`), supports custom Lua plugins (denylist), request IDs, path rewriting and SSE. Alternatives considered: Kong OSS (JWT plugin needs manually uploaded keys; JWKS requires the paid OIDC plugin), Traefik (no built-in JWT verification; needs ForwardAuth to a verifier service), Envoy (capable but verbose, separate rate-limit service). Plugin behaviour differs between APISIX versions: pin the version and verify in the Step 4.1 spike.

## F.5 AI Assistant: online vs offline
- **Online:** interactive questions, streamed over SSE; only retrieval at question time; strict timeout and circuit breaker.
- **Offline (background):** long jobs return 202 + job id and run as Temporal workflows in batches.
- **Offline (prepared ahead):** embeddings are computed when jobs/resumes change (Phase 15), not when someone asks.
- **Degraded:** LLM provider down → breaker opens, 503 with a clear message, non-LLM fallbacks where possible; hiring flows never depend on the LLM.
