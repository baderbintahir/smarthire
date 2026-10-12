# Build context is the REPO ROOT (see docker-compose.yml), because we need libs/ too.
FROM python:3.12-slim

# uv: installed from PyPI at a pinned version (same as local dev). The official
# ghcr.io/astral-sh/uv image is the usual alternative, but pulls from ghcr.io stalled here.
RUN pip install --no-cache-dir uv==0.12.23

# PYTHONUNBUFFERED: logs appear immediately; no .pyc files in the image
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 \
    UV_LINK_MODE=copy UV_COMPILE_BYTECODE=0

# Same relative layout as the repo, so the "../../libs/..." path in pyproject.toml resolves
WORKDIR /app/services/_template

# Layer 1 (changes rarely): the shared library and dependency manifests
COPY libs/smarthire_common /app/libs/smarthire_common
COPY services/_template/pyproject.toml services/_template/uv.lock ./
# --frozen: install exactly what uv.lock says; --no-dev: no test tools in the image
RUN uv sync --frozen --no-dev

# Layer 2 (changes often): the service code. Editing code does not reinstall packages.
COPY services/_template/ ./

# Run as an unprivileged user: a compromised app is not root inside the container
RUN useradd --system --uid 10001 --no-create-home app
USER app

ENV PATH="/app/services/_template/.venv/bin:$PATH" PORT=8099
EXPOSE 8099

# --factory: call create_app() to build the app (so tests can pass their own settings)
CMD ["sh", "-c", "uvicorn app.main:create_app --factory --host 0.0.0.0 --port ${PORT}"]
