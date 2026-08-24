# syntax=docker/dockerfile:1.7

# =============================================================================
# Image electricore-kiosque (#706, ADR-0057) — UNE image, N configs : tout le
# catalogue de notebooks embarqué, la personnalisation par entité (KIOSQUE__APPS,
# KIOSQUE__TITRE, KIOSQUE__API_URL) reste de la config au lancement, pas du build.
#
# Contexte de build = RACINE du repo (nécessaire : `packages/electricore-kiosque`
# dépend de `packages/electricore-client`, tous deux membres du workspace uv résolus
# par le uv.lock de la racine — même trick que deploy/docker/Dockerfile).
#
# JAMAIS le moteur `electricore` : ni installé (scope `--package electricore-kiosque`
# ci-dessous), ni copié dans le runtime. Aucun secret : la clé API `electricore` est
# saisie côté navigateur, jamais côté serveur (voir README).
# =============================================================================

# =============================================================================
# Stage 1 — builder : résolution du venv avec uv, SCOPE electricore-kiosque seul
# =============================================================================
FROM python:3.13-slim AS builder

# uv depuis l'image officielle Astral (binaire statique) — même pin que le moteur.
COPY --from=ghcr.io/astral-sh/uv:0.5-python3.13-bookworm-slim /usr/local/bin/uv /usr/local/bin/uv

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app/.venv

WORKDIR /app

# Manifestes du WORKSPACE (racine, pour le uv.lock partagé) + le dossier `packages/`
# entier (les deux membres : electricore-client dont le Kiosque dépend, et lui-même).
# JAMAIS `electricore/` (le moteur, membre racine du workspace) : pas copié ici, donc
# indisponible pour l'étape suivante — la scope `--package` ci-dessous n'irait de
# toute façon pas le chercher, mais l'absence physique le garantit à double titre.
COPY pyproject.toml uv.lock README.md ./
COPY packages/ ./packages/

# `--package electricore-kiosque` : résout et installe SEULEMENT le sous-arbre de
# dépendances du Kiosque (marimo, uvicorn, electricore-client[arrow] → polars) —
# jamais le projet racine du workspace (le moteur `electricore`), jamais DuckDB.
# `--no-editable` : electricore-client ET electricore-kiosque installés en vrais
# WHEELS dans le venv (pas de .pth pointant vers /app/packages/…) — le runtime peut
# donc copier SEULEMENT le venv, sans jamais embarquer `packages/` en source ni
# `electricore/` (le moteur n'a même pas besoin d'être présent dans ce contexte).
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --package electricore-kiosque --no-dev --no-editable

# =============================================================================
# Stage 2 — runtime : image légère, sans outils de build ni source `packages/`
# =============================================================================
FROM python:3.13-slim AS runtime

# tini uniquement : pas de libxml2/supercronic/duckdb/sops/age — le Kiosque ne fait
# ni parsing XML Enedis, ni cron, ni DuckDB, ni déchiffrement de secrets (aucun secret
# côté serveur, ADR-0057).
RUN apt-get update \
 && apt-get install -y --no-install-recommends tini \
 && rm -rf /var/lib/apt/lists/*

# Utilisateur non-root
RUN groupadd --system --gid 1000 kiosque \
 && useradd --system --uid 1000 --gid kiosque --home-dir /app --shell /usr/sbin/nologin kiosque

WORKDIR /app

# Uniquement le venv déjà résolu (wheels, pas d'éditable) — aucun source `packages/`,
# aucun `electricore/` : l'image runtime ne peut PAS accidentellement importer le moteur.
COPY --from=builder --chown=kiosque:kiosque /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Europe/Paris

USER kiosque

EXPOSE 8765

# tini comme PID 1 (propagation correcte de SIGTERM). Pas d'entrypoint de déchiffrement
# de secrets (contrairement à l'image moteur) : rien à déchiffrer ici. Configuration
# entière par variables d'environnement au lancement — voir README (KIOSQUE__APPS,
# KIOSQUE__TITRE, KIOSQUE__API_URL).
ENTRYPOINT ["/usr/bin/tini", "--", "electricore-kiosque"]
