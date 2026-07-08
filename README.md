# ⚡ ElectriCore — moteur de traitement des données énergétiques

[![CI](https://github.com/Energie-De-Nantes/electricore/actions/workflows/ci.yml/badge.svg)](https://github.com/Energie-De-Nantes/electricore/actions/workflows/ci.yml)
[![License: AGPL-3.0](https://img.shields.io/badge/license-AGPL--3.0-blue.svg)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](pyproject.toml)
[![Polars](https://img.shields.io/badge/data-Polars%20%2B%20DuckDB-9C27B0.svg)](https://energie-de-nantes.github.io/electricore/adr/0002-polars-uniquement/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Docs](https://img.shields.io/badge/docs-site-blue)](https://energie-de-nantes.github.io/electricore/)

**ElectriCore** est un outil libre pour reprendre le contrôle des données du réseau électrique
français. Il transforme les flux bruts **Enedis** (le distributeur) en données structurées,
calculées et exploitables — facturation calendaire, abonnements, énergies par cadran, TURPE,
taxes — exposées via une **API REST** pour Odoo, des notebooks et tout outil de suivi énergétique.

> **Pour qui ?** Un fournisseur d'électricité (EDN, Enargia…) qui veut **recalculer** lui-même
> ce qu'il facture, à partir de la donnée brute Enedis, sans dépendre des agrégats
> « moisniversaire » du distributeur — inexploitables pour une facturation au mois calendaire.

**Stack** : Polars + DuckDB (zéro pandas), ingestion ELT dlt + dbt, FastAPI, Pandera. Tout le code
et la doc sont en français — le domaine (Enedis, CRE, TURPE, accise) l'est intrinsèquement.

---

## 📖 Documentation — 5 portes, une par rôle

Le [site de documentation](https://energie-de-nantes.github.io/electricore/) est rangé par rôle
de lecture, en escalier (quoi → comment s'en servir → comment c'est fait → comment le faire
évoluer) :

- **[Comprendre](https://energie-de-nantes.github.io/electricore/comprendre/)** — ce qu'ElectriCore
  calcule et pourquoi, pour un public non technique.
- **[Facturer](https://energie-de-nantes.github.io/electricore/facturiste/)** —
  le cycle de facturation au quotidien, bot Telegram et notebooks.
- **[Déployer](https://energie-de-nantes.github.io/electricore/deployer/)** — installer et
  administrer une instance (VPS, Docker Compose, secrets).
- **[Contribuer](https://energie-de-nantes.github.io/electricore/contribuer/)** — architecture,
  exemples de code, patterns, installation dev et tests.
- **[Maintenir](https://energie-de-nantes.github.io/electricore/maintenir/)** — décisions
  d'architecture, releases, suivi réglementaire.

## 🚀 Démarrage rapide

```bash
git clone https://github.com/Energie-De-Nantes/electricore.git
cd electricore
uv sync   # runtime : core + API + bot (voir la section Contribuer pour les extras)

uv run uvicorn electricore.api.main:app --reload
curl http://localhost:8000/health
```

Détails d'installation, ingestion, pipelines et API : voir
[Contribuer → Développer](https://energie-de-nantes.github.io/electricore/contribuer/developper/).

## 📄 Licence

[AGPL-3.0](LICENSE).

## 🙏 Remerciements

[Polars](https://pola.rs) · [DuckDB](https://duckdb.org) · [dlt](https://dlthub.com) ·
[dbt](https://www.getdbt.com) · [FastAPI](https://fastapi.tiangolo.com) ·
[Pandera](https://pandera.readthedocs.io) · [Marimo](https://marimo.io) ·
[httpx](https://www.python-httpx.org) · [pydantic](https://docs.pydantic.dev) · [uv](https://docs.astral.sh/uv/).
