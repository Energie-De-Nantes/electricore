# syntax=docker/dockerfile:1.7

# =============================================================================
# Stage 1 — builder : résolution du venv avec uv (etl extra inclus, dev/test exclus)
# =============================================================================
FROM python:3.13-slim AS builder

# Dépendances système requises pour compiler pycryptodome et lxml
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        build-essential \
        libxml2-dev \
        libxslt1-dev \
 && rm -rf /var/lib/apt/lists/*

# uv depuis l'image officielle Astral (binaire statique)
COPY --from=ghcr.io/astral-sh/uv:0.5-python3.13-bookworm-slim /usr/local/bin/uv /usr/local/bin/uv

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app/.venv

WORKDIR /app

# Couche cache deps : on copie d'abord les manifestes pour profiter du cache Docker
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --extra etl --no-dev

# Couche projet : on copie le code et on l'installe dans le venv
COPY electricore/ ./electricore/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --extra etl --no-dev

# =============================================================================
# Stage 2 — runtime : image légère sans outils de build
# =============================================================================
FROM python:3.13-slim AS runtime

# Runtime libs uniquement (pas de -dev, pas de compilateur)
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        libxml2 \
        libxslt1.1 \
        tini \
        ca-certificates \
        curl \
 && rm -rf /var/lib/apt/lists/*

# supercronic : cron container-friendly (un seul binaire Go, logs sur stdout)
ARG SUPERCRONIC_VERSION=0.2.33
ARG SUPERCRONIC_SHA1SUM=71b0d58cc53f6bd72cf2f293e09e294b79c666d8
RUN curl -fsSL -o /usr/local/bin/supercronic \
        "https://github.com/aptible/supercronic/releases/download/v${SUPERCRONIC_VERSION}/supercronic-linux-amd64" \
 && echo "${SUPERCRONIC_SHA1SUM}  /usr/local/bin/supercronic" | sha1sum -c - \
 && chmod +x /usr/local/bin/supercronic

# duckdb CLI pour les sauvegardes EXPORT DATABASE
ARG DUCKDB_VERSION=1.1.3
RUN curl -fsSL -o /tmp/duckdb.zip \
        "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-linux-amd64.zip" \
 && cd /tmp && python -c "import zipfile; zipfile.ZipFile('duckdb.zip').extractall('/usr/local/bin/')" \
 && chmod +x /usr/local/bin/duckdb \
 && rm /tmp/duckdb.zip

# Utilisateur non-root
RUN groupadd --system --gid 1000 electricore \
 && useradd --system --uid 1000 --gid electricore --home-dir /app --shell /usr/sbin/nologin electricore

WORKDIR /app

# Récupérer le venv déjà résolu depuis le builder + le code source
COPY --from=builder --chown=electricore:electricore /app/.venv /app/.venv
COPY --chown=electricore:electricore electricore/ ./electricore/
COPY --chown=electricore:electricore deploy/docker/backup_duckdb.sh /app/deploy/docker/backup_duckdb.sh
RUN chmod +x /app/deploy/docker/backup_duckdb.sh

# Volumes pour les données (DuckDB et backups)
RUN mkdir -p /data /backups \
 && chown -R electricore:electricore /data /backups

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DUCKDB_PATH=/data/flux_enedis_pipeline.duckdb \
    TZ=Europe/Paris

USER electricore

# tini comme PID 1 — propagation correcte des signaux (SIGTERM lors d'un compose down)
ENTRYPOINT ["/usr/bin/tini", "--"]

# Pas de CMD : chaque service docker-compose définit son propre `command`.
# Exemples :
#   - API : ["uvicorn", "electricore.api.main:app", "--host", "0.0.0.0", "--port", "8001"]
#   - ETL : ["supercronic", "/etc/cron/crontab"]
