# syntax=docker/dockerfile:1.7
#
# Lightweight image for the non-Spark services (MQTT bridge, fleet simulator).
# Build context: the repository root.

FROM ghcr.io/astral-sh/uv:0.10 AS uv

FROM python:3.11-slim-bookworm

COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_PREFERENCE=only-system \
    UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dependencies first (cached unless the lock file changes), then the project itself.
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable
# Runtime data the simulator needs (contracts are the runtime source of truth).
COPY contracts ./contracts

RUN useradd --uid 1001 --create-home app
USER app
