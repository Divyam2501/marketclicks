# alpaca_feed image. Two stages: uv builds the venv, the final image runs it.
# Build for GKE (x86) from an ARM Mac:
#   docker build --platform linux/amd64 -t tickstore-feed:dev .

# --- Stage 1: build the venv from uv.lock --------------------------------
FROM python:3.13-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.13.7 /uv /bin/uv

# Compile .pyc at build time (faster start); copy files instead of hardlinking
# from uv's cache; use the image's Python, never download one.
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0

WORKDIR /app

# Dependencies first, in their own layer: code changes don't reinstall them.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-install-project

# Then the project itself, installed into the venv (not editable), so the
# final stage needs only /app/.venv and no source tree.
COPY README.md ./
COPY src/ src/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable

# --- Stage 2: runtime: slim Python + venv, no uv ----------------------------
FROM python:3.13-slim

# Fixed numeric UID/GID: Kubernetes runAsNonRoot can only verify a numeric
# USER. Must match runAsUser/runAsGroup in k8s/deployment.yaml.
RUN groupadd --system --gid 999 app && \
    useradd --system --uid 999 --gid 999 --no-create-home app

# Same path as the builder: the venv's python symlinks to /usr/local/bin/python,
# which exists at the same place in both stages (same base image).
COPY --from=builder --chown=app:app /app/.venv /app/.venv

USER 999:999
ENV PYTHONUNBUFFERED=1

# Exec form: python is PID 1 and receives SIGTERM directly. No shell, no uv.
CMD ["/app/.venv/bin/python", "-m", "tickstore.ingest.alpaca_feed"]
