# SmartHire shortcuts. Usage: make <target> [s=<service>]   e.g. make logs s=identity
# NOTE: recipe lines must start with a TAB, not spaces.

.DEFAULT_GOAL := help

# Targets are actions, not files: always run them even if a file/folder of that name exists
.PHONY: help up down ps logs migrate test test-gateway test-events schemas keys check-s

# Where a service's code lives: s=common is the shared library, anything else is services/<s>
SERVICE_DIR = $(if $(filter common,$(s)),libs/smarthire_common,services/$(s))

help: ## Show this help
	@grep -hE '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  make %-9s %s\n", $$1, $$2}'

up: ## Build and start everything in the background
	docker compose up -d --build

down: ## Stop everything (data volumes are kept)
	docker compose down

ps: ## Show container status
	docker compose ps

logs: check-s ## Follow a service's logs:    make logs s=identity
	docker compose logs -f --tail=100 $(s)

migrate: check-s ## Run DB migrations inside the running container:  make migrate s=identity
	docker compose exec $(s) alembic upgrade head

test: check-s ## Run a service's tests on the host:  make test s=identity  (s=common for the library)
	cd $(SERVICE_DIR) && uv run pytest

test-gateway: ## Check the gateway (APISIX) config against the RUNNING stack (make up first)
	uv run --no-project --with pytest --with httpx --with pyjwt pytest tests/gateway -q

test-events: ## Check events end to end against the RUNNING stack (make up first)
	uv run --no-project --with pytest --with httpx --with "confluent-kafka[schemaregistry,json]" pytest tests/events -q

schemas: ## Register the event schemas in Schema Registry (also runs on every `make up`)
	SCHEMA_REGISTRY_URL=http://localhost:8081 PYTHONPATH=libs/smarthire_common python3 infra/schema-registry/register.py

keys: ## Generate the dev RSA key pair Identity signs JWTs with (secrets/identity, gitignored)
	cd services/identity && uv run python scripts/generate_dev_keys.py

# Internal: fail early with a clear message when s=... is missing
check-s:
	@test -n "$(s)" || { echo "Missing service name. Usage: make $(MAKECMDGOALS) s=<service>  (e.g. s=identity)"; exit 1; }
