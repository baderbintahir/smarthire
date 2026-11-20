# SmartHire

A recruitment platform built as a set of microservices: recruiters publish jobs, candidates apply, and the
system scores, advances, notifies and reports. It is a learning project, built step by step to practise
event-driven backends: database per service, transactional outbox, idempotent consumers, sagas, and
observability.

## Documentation

| Document | What it is |
| --- | --- |
| [`docs/architecture.html`](docs/architecture.html) | **The design**: diagrams, the 17 end-to-end flows, events, schemas, Redis keys, API reference. Open it in a browser (works offline). |
| [`docs/implementation-plan.md`](docs/implementation-plan.md) | The step-by-step build plan, the database design (Appendix E) and the architecture decisions (Appendix F). |
| [`docs/part-a-checkpoint.md`](docs/part-a-checkpoint.md) | Part A checkpoint: what works, how it was tested, load and chaos results. |
| [`CLAUDE.md`](CLAUDE.md) | Working agreement, architecture rules, progress tracker and decisions log. |
| [`postman/`](postman) | Postman collection for the public API (folders follow the demo flow) and a fictional sample resume. |

## Architecture in one paragraph

Clients call **Apache APISIX** (`:8000`), which verifies the JWT, checks revocation in Redis and applies rate
limits. Behind it, nine FastAPI services each own their data (one Postgres schema and DB user per service, or a
MongoDB database): identity, job, job_enrichment, candidate, application, scoring, notification, analytics and
bff_web. A state change and its event are committed together (outbox); a relay publishes to **Kafka** (JSON
Schema, BACKWARD compatibility). Multi-step processes that must finish or compensate run on **Temporal**; one-off
background tasks (resume parsing, email) run on **Celery** over RabbitMQ. Traces go to Jaeger, metrics to
Prometheus and Grafana.

## Run it locally

Prerequisites: Docker with Compose, Python 3.12 and [uv](https://docs.astral.sh/uv/).

```bash
cp .env.example .env      # then fill in the passwords (placeholders only in the example)
make keys                 # RSA key pair Identity signs tokens with (written to secrets/, gitignored)
make up                   # build and start everything; creates topics and registers event schemas
make ps                   # wait until the services are healthy
```

Admins can't self-register. Create one with:

```bash
docker compose exec -it identity python -m app.cli create-admin admin@example.com
```

Then import `postman/SmartHire.postman_collection.json` and run the folders in order.

| Tool | URL |
| --- | --- |
| API (through the gateway) | http://localhost:8000/api |
| Kafka UI | http://localhost:8085 |
| Schema Registry | http://localhost:8081 |
| Temporal UI | http://localhost:8088 |
| RabbitMQ | http://localhost:15672 |
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
make test-events          # events end to end against the running stack
```

`make help` lists everything else (migrations, logs, load and chaos tests).

## Repository layout

```
services/<name>/        one folder per service: app/{api,domain,services,repositories,models,schemas,events,workflows},
                        alembic/ migrations, tests/; services/_template is the copy-me starting point
libs/smarthire_common/  the shared chassis: auth, db, events, outbox, Kafka consumer, Temporal, Celery, telemetry
infra/                  APISIX, Kafka topics, Schema Registry, Postgres and MinIO init, Prometheus, Grafana
tests/                  cross-service suites: gateway, events (end to end), load (Locust) and chaos
examples/               small standalone demos (Temporal hello world)
docs/                   design and plan (see above)
postman/                API collection
```
