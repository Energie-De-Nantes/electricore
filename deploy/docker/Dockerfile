# syntax=docker/dockerfile:1.7

# =============================================================================
# Stage 1 — builder : résolution du venv avec uv (extra ingestion inclus, dev/test exclus)
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

# Couche cache deps : manifestes + source du client. `electricore-client` est un membre
# du workspace uv dont le moteur dépend ; uv en a besoin pour résoudre le lock gelé, même
# avec --no-install-project. (Source minuscule : l'impact sur le cache de couche est négligeable.)
COPY pyproject.toml uv.lock README.md ./
COPY packages/ ./packages/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --extra ingestion --extra dbt --no-dev

# Couche projet : on copie le code et on l'installe dans le venv
COPY electricore/ ./electricore/
# Les notebooks opérateur sont force-inclus dans le wheel electricore (pont #414,
# → electricore/_operator_notebooks/) : hatchling exige leur source présente à l'install
# de la racine. Sans eux, `uv sync` casse (FileNotFoundError: Forced include not found).
COPY notebooks/accueil.py notebooks/facturation.py notebooks/injection_rsc.py ./notebooks/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --extra ingestion --extra dbt --no-dev

# `uv sync` a installé `electricore-client` en ÉDITABLE (membre du workspace, pointant vers
# /app/packages/…). On le remplace par un vrai WHEEL installé dans le venv : ainsi l'image
# runtime n'embarque PAS le source `packages/` (seul le moteur reste servi depuis son source).
RUN --mount=type=cache,target=/root/.cache/uv \
    uv build packages/electricore-client --wheel --out-dir /tmp/wheels \
 && uv pip install --python /app/.venv/bin/python --no-deps --reinstall \
        /tmp/wheels/electricore_client-*.whl

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

# sops + age : déchiffrement des secrets à l'entrypoint (ADR-0044). Binaires statiques
# pinnés + vérifiés par checksum, même motif que supercronic/duckdb ci-dessus.
ARG SOPS_VERSION=3.9.4
ARG SOPS_SHA256=5488e32bc471de7982ad895dd054bbab3ab91c417a118426134551e9626e4e85
RUN curl -fsSL -o /usr/local/bin/sops \
        "https://github.com/getsops/sops/releases/download/v${SOPS_VERSION}/sops-v${SOPS_VERSION}.linux.amd64" \
 && echo "${SOPS_SHA256}  /usr/local/bin/sops" | sha256sum -c - \
 && chmod +x /usr/local/bin/sops

ARG AGE_VERSION=1.2.1
ARG AGE_SHA256=7df45a6cc87d4da11cc03a539a7470c15b1041ab2b396af088fe9990f7c79d50
RUN curl -fsSL -o /tmp/age.tgz \
        "https://github.com/FiloSottile/age/releases/download/v${AGE_VERSION}/age-v${AGE_VERSION}-linux-amd64.tar.gz" \
 && echo "${AGE_SHA256}  /tmp/age.tgz" | sha256sum -c - \
 && tar -xzf /tmp/age.tgz -C /tmp \
 && mv /tmp/age/age /tmp/age/age-keygen /usr/local/bin/ \
 && chmod +x /usr/local/bin/age /usr/local/bin/age-keygen \
 && rm -rf /tmp/age /tmp/age.tgz

# Utilisateur non-root
RUN groupadd --system --gid 1000 electricore \
 && useradd --system --uid 1000 --gid electricore --home-dir /app --shell /usr/sbin/nologin electricore

WORKDIR /app

# Récupérer le venv déjà résolu depuis le builder + le code source
COPY --from=builder --chown=electricore:electricore /app/.venv /app/.venv
COPY --chown=electricore:electricore electricore/ ./electricore/
COPY --chown=electricore:electricore deploy/docker/backup_duckdb.sh /app/deploy/docker/backup_duckdb.sh
RUN chmod +x /app/deploy/docker/backup_duckdb.sh

# Entrypoint de déchiffrement SOPS+age (ADR-0044) — installé sur le PATH comme
# `electricore-entrypoint`. Il s'intercale après tini : `tini -- electricore-entrypoint <cmd…>`.
COPY deploy/docker/entrypoint.sh /usr/local/bin/electricore-entrypoint
RUN chmod +x /usr/local/bin/electricore-entrypoint

# Volumes pour les données (DuckDB et backups)
RUN mkdir -p /data /backups \
 && chown -R electricore:electricore /data /backups

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DUCKDB_PATH=/data/flux_enedis_pipeline.duckdb \
    TZ=Europe/Paris

USER electricore

# tini comme PID 1 — propagation correcte des signaux (SIGTERM lors d'un compose down).
# Puis electricore-entrypoint déchiffre les secrets SOPS+age (ADR-0044) avant d'exec
# la commande du service. Échappatoire dev/test : ELECTRICORE_DECRYPT=off.
ENTRYPOINT ["/usr/bin/tini", "--", "electricore-entrypoint"]

# Pas de CMD : chaque service docker-compose définit son propre `command`.
# Exemples (la commande est exec'd APRÈS déchiffrement par electricore-entrypoint) :
#   - API : ["uvicorn", "electricore.api.main:app", "--host", "0.0.0.0", "--port", "8001"]
#   - ETL : ["supercronic", "/etc/cron/crontab"]
