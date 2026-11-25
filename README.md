# SmartHire

A recruitment platform built as a set of microservices: recruiters publish jobs, candidates apply, and the
system scores, advances, notifies and reports. It is a learning project, built step by step to practise
distributed backends: database per service, workflow-first writes on Temporal, idempotent steps, sagas, and
observability.

## Documentation

Read the docs as a website: **https://baderbintahir.github.io/smarthire/** (architecture and API walkthrough, published from `docs/` by GitHub Pages).

| Document | What it is |
| --- | --- |
| [`docs/architecture.html`](docs/architecture.html) | **The design**: diagrams, the 17 end-to-end flows, events, schemas, Redis keys, API reference. Open it in a browser (works offline). |
| [`docs/implementation-plan.md`](docs/implementation-plan.md) | The step-by-step build plan, the database design (Appendix E) and the architecture decisions (Appendix F). |
| [`docs/api-walkthrough.md`](docs/api-walkthrough.md) ([web version](https://baderbintahir.github.io/smarthire/api-walkthrough.html)) | **Every API call in order** to test the whole flow: method, path, token, body, what each step saves. |
| [`docs/part-a-checkpoint.md`](docs/part-a-checkpoint.md) | Part A checkpoint: what works, how it was tested, load and chaos results. |
| [`CLAUDE.md`](CLAUDE.md) | Working agreement, architecture rules, progress tracker and decisions log. |
| [`postman/`](postman) | Postman collection for the public API (folders follow the demo flow) and a fictional sample resume. |

## Architecture in one paragraph

Clients call **Apache APISIX** (`:8000`), which verifies the JWT, checks revocation in Redis and applies rate
limits. Behind it, nine FastAPI services each own their data (one Postgres schema and DB user per service, or a
MongoDB database): identity, job, job_enrichment, candidate, application, scoring, notification, analytics and
bff_web. A tenth service, the **orchestrator**, runs every workflow on **Temporal** and is the only part that
talks to it: a change other services react to (register, publish, apply, ...) is the first step of a workflow, the
caller waits for it, and every later step (profiles, scores, emails, analytics facts) is an HTTP call to the owning
service's `/internal/*` endpoint, retried until done. If the orchestrator or Temporal is down, the request gets 503
and nothing is written. No Kafka, no Celery (design v2.0/v2.1). Traces go to Jaeger, metrics to Prometheus and
Grafana.

## Run it locally

Prerequisites: Docker with Compose, Python 3.12 and [uv](https://docs.astral.sh/uv/).

```bash
cp .env.example .env      # then fill in the passwords (placeholders only in the example)
make keys                 # Identity's token key + the orchestrator's token and payload keys (secrets/, gitignored)
make up                   # build and start everything; creates topics and registers event schemas
make ps                   # wait until the services are healthy
```

Admins can't self-register. Create one with:

```bash
docker compose exec -it identity python -m app.cli create-admin admin@example.com
```

## Walk through the whole flow

**Step-by-step list of all 44 requests: [`docs/api-walkthrough.md`](docs/api-walkthrough.md), or the [web version](https://baderbintahir.github.io/smarthire/api-walkthrough.html) with search.**

The Postman collection (`postman/SmartHire.postman_collection.json`) is the runnable version of the flows in
[`docs/architecture.html`](docs/architecture.html). Its folders are in demo order, and test scripts save tokens,
ids and versions into variables, so you can click through it or use **Run collection**.

Before you start:

1. `make up` and wait until `make ps` shows the services healthy.
2. Create an admin whose password matches the collection's `password` variable (`ChangeMe123!`), or change the variable:
   `docker compose exec -it identity python -m app.cli create-admin admin@example.com`
3. Import the collection. For **Run collection**, set a delay of about 2500 ms between requests: publishing,
   resume parsing and scoring happen in the background.
4. Running it again? Change `candidateEmail` and `recruiterEmail` first (an email can register only once: 409).

| Folder | What happens | Where to see it |
| --- | --- | --- |
| 1 Auth | Register a candidate and a recruiter (RegistrationWorkflow: Candidate creates the profile), log in, update a name, the recruiter adds the CANDIDATE role and refreshes (RoleChangeWorkflow) | Temporal UI, workflows `register-<hash>` and `role-<user>-CANDIDATE-grant` |
| 2 Jobs | Create and update a job, publish it: 202, PROCESSING, then READY when the publishing saga finishes | Temporal UI, workflow `publish-<job_id>-<version>`; Mailpit (JOB_READY to the recruiter) |
| 3 Candidate | Profile, resume upload (202); parsed in the background | "List my resumes" shows PARSED |
| 4 Applications | Apply (202), the same Idempotency-Key returns the same application, the recruiter can't apply to their own job (403); "Get application" shows SCREENING and a score once the application workflow ran | Temporal UI, workflow `application-<job_id>-<candidate_id>`; Mailpit |
| 5 Recruiter | Board, move a stage, add a note, hire | Mailpit (stage and decision emails) |
| 6 BFF | One screen composed from Application, Candidate and Scoring | `degraded` is empty while everything is up |
| 7 Analytics | Metrics from the read model, fed by facts the workflows record (cached 10 s) | Grafana, Business dashboard |
| 8 Wrap up | Close the job, try to withdraw (409: a decided application is final), log out | |
| 9 Admin | Log in as admin, revoke one access token, suspend the candidate, grant and revoke a role | the candidate's old token now gets 401 |

Every request is traced: in Jaeger, pick the `gateway` service to follow one request through the services, the
orchestrator's workflow and every step it called. Folder 10 calls internal endpoints directly (not through the gateway).

## Tools

| Tool | URL |
| --- | --- |
| API (through the gateway) | http://localhost:8000/api |
| Temporal UI | http://localhost:8088 |
| MinIO console | http://localhost:9001 |
| Mailpit (sent emails) | http://localhost:8025 |
| Jaeger (traces) | http://localhost:16686 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3001 |

The gateway (`:8000`) is the one public entry point; every other port is published on `127.0.0.1` only.

## Tests

```bash
make test s=identity      # one service's tests (real Postgres/Redis in throwaway containers)
make test s=common        # the shared library, libs/smarthire_common
make test-gateway         # gateway checks against the running stack
make test-workflows       # workflows end to end against the running stack
```

`make help` lists everything else (migrations, logs, load and chaos tests).

## Repository layout

```
services/<name>/        one folder per service: app/{api,domain,services,repositories,models,schemas,events,workflows},
                        alembic/ migrations, tests/; services/_template is the copy-me starting point
libs/smarthire_common/  the shared chassis: auth, service tokens, db, orchestrator client, Temporal, telemetry
infra/                  APISIX, Postgres and MinIO init, Prometheus, Grafana
tests/                  cross-service suites: gateway, workflows (end to end), load (Locust) and chaos
examples/               small standalone demos (Temporal hello world)
docs/                   design and plan (see above)
postman/                API collection
```
