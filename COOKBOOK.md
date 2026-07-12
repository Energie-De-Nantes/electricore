# Cookbook

Registre des routines du repo, lu à la demande par `how-to` (format : une recette
par section `##`, champs `mode`/`when`/`do` + `look`+`expect` ou `observe`).
Convention : les commandes `afk` tournent depuis la racine du repo ; `git push`,
`gh` et tout ce qui touche `~/.cache/uv` se lancent **hors sandbox** (voir CLAUDE.md).
Les secrets ne vivent jamais ici : une recette nomme la variable (`.env`, trousseau),
jamais sa valeur — ce fichier est commité.

## tests

mode: afk
when: tout changement de code Python (core, api, bot, ingestion)
do: uv run --group test pytest -q  # ciblé : uv run --group test pytest {chemin_test} -v
observe: sortie pytest — référence saine ~1180 passed, 3 skipped (juillet 2026) ; tout failed/error est à examiner

## lint

mode: afk
when: avant tout commit / push d'une PR
do:
```
uvx ruff format
uvx ruff check --fix
```
look: sortie des deux commandes
expect: exit 0, aucun fichier reformaté restant ni erreur lint (miroir du pre-commit ruff ; `uvx pre-commit run --all-files` ajoute gitleaks)

## typecheck

mode: afk
when: changement sous electricore/core/loaders, core/writers ou core/builds (scope mypy strict)
do: uv run --group typecheck mypy
look: sortie mypy
expect: Success, no issues found (le scope est pinné dans pyproject [tool.mypy])

## api-up

mode: afk
when: exercer l'API en local (changement d'endpoint, de service, de sérialiseur)
do:
```
uv run uvicorn electricore.api.main:app --reload --port 8001 &
curl -s http://localhost:8001/health
```
look: la réponse de /health
expect: HTTP 200 JSON ; les endpoints métier exigent le header X-API-Key — valeur : API__TROUSSEAU__{consommateur}__KEY du .env  # TODO(you): le NOM du consommateur de dev (ex. librewatt, bot — la clé reste dans .env)

## api-endpoint

mode: human
when: vérifier à l'œil un endpoint nouveau ou modifié
do: lancer l'API (voir `api-up`), ouvrir Swagger, Authorize avec la valeur de API__TROUSSEAU__{consommateur}__KEY lue dans ton .env, puis exécuter {endpoint}  # TODO(you): le nom du consommateur de dev (jamais la clé ici)
look: http://localhost:8001/docs
expect: l'endpoint apparaît sous le bon tag, répond 200 avec la forme attendue (les endpoints legacy sont balisés comme tels)

## ingestion

mode: afk
when: changement sous electricore/ingestion (sources, transformers, modèles dbt)
do:
```
uv sync --extra ingestion --extra dbt
uv run python -m electricore.ingestion test   # 2 fichiers ~3s ; {flux} pour un flux complet ; all = prod
```
look: la sortie du runner et la base electricore/ingestion/flux_enedis_pipeline.duckdb
expect: job completed (landing → dbt build) ; exige dans .env les secrets SFTP__* et le trousseau AES__TROUSSEAU__* (noms des variables : deploy/providers/example/secrets.env.example ; sans eux l'ingestion réseau échoue)

## client-purity

mode: afk
when: changement sous packages/electricore-client (garantie polars-free, miroir CI)
do:
```
uv venv /tmp/claude/client-venv
uv pip install --python /tmp/claude/client-venv ./packages/electricore-client pytest
/tmp/claude/client-venv/bin/python -m pytest packages/electricore-client/tests -q
```
look: sortie pytest du venv isolé
expect: tests verts sans que polars ni le moteur soient installés (c'est le venv isolé qui prouve la pureté, pas le venv workspace)

## docs-build

mode: afk
when: changement sous docs/ ou mkdocs.yml
do: uv run --group docs mkdocs build --strict  # hors sandbox (cache uv) ; liens morts bloquants sauf docs/adr/** et docs/agents/**
look: sortie mkdocs
expect: exit 0 sans warning (--strict transforme tout warning en échec, garde-fou CI #560)

## deploy-tests

mode: afk
when: changement sous deploy/ (install.sh, lib/, docker/)
do: bash deploy/tests/unit.sh
look: sortie du script
expect: exit 0 (même gate que le job CI deploy ; secrets_roundtrip.sh en plus côté CI)

## client-release

mode: human
when: publier electricore-client sur PyPI (après merge dans main du bump de version : pyproject + `__version__` + uv.lock)
do:
```
git fetch origin main
git tag client-v{version} origin/main   # {version} = celle du pyproject mergé, ex. 0.4.0 (pré-releases PEP 440 : a1/b1/rc1/.devN)
git push origin client-v{version}       # hors sandbox ; le tag déclenche .github/workflows/release-client.yml
gh run list --workflow release-client.yml --limit 1
```
look: le run "Release electricore-client" et https://pypi.org/project/electricore-client/
expect: job vert (build uv → vérif artefacts vs tag → Trusted Publishing OIDC, aucun token PyPI) ; la version apparaît sur PyPI, puis souscriptions_odoo peut bumper son pin

## docs-eyeball

mode: human
when: vérifier le rendu d'une page de doc (palette cozy, nav, admonitions)
do: uv run --group docs mkdocs serve  # hors sandbox
look: http://localhost:8000/{page}
expect: la page rend ton changement, thème Warm Cosy appliqué, nav et liens internes OK

## bot-eyeball

mode: human
when: changement d'un handler bot ou du client HTTP bot→API
do: lancer l'API avec BOT__TOKEN et BOT__ALLOWED_USERS dans .env (le bot vit dans le lifespan FastAPI, voir `api-up`), puis envoyer {commande} au bot  # TODO(you): le @handle du bot de dev (le token reste BOT__TOKEN dans .env)
look: le chat Telegram du bot
expect: la commande répond (clavier inline sans argument, action directe avec), pas de trace d'erreur côté uvicorn

## notebooks

mode: human
when: changement des notebooks opérateur (accueil, facturation, injection RSC) — pont transitoire #414
do: uv run --extra notebooks electricore-notebooks  # exige dans .env ELECTRICORE_API_URL (dev : http://localhost:8001, voir `api-up`) et ELECTRICORE_API_KEY (une clé du trousseau, reste dans .env)
look: l'URL affichée par le lanceur marimo
expect: le notebook charge et parle à l'API (pas de connexion refusée)
