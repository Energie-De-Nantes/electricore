# ⚡ ElectriCore — moteur de traitement des données énergétiques

[![CI](https://github.com/Energie-De-Nantes/electricore/actions/workflows/ci.yml/badge.svg)](https://github.com/Energie-De-Nantes/electricore/actions/workflows/ci.yml)
[![License: AGPL-3.0](https://img.shields.io/badge/license-AGPL--3.0-blue.svg)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](pyproject.toml)
[![Polars](https://img.shields.io/badge/data-Polars%20%2B%20DuckDB-9C27B0.svg)](docs/adr/0002-polars-uniquement.md)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

**ElectriCore** est un outil libre pour reprendre le contrôle des données du réseau électrique
français. Il transforme les flux bruts **Enedis** (le distributeur) en données structurées,
calculées et exploitables — facturation calendaire, abonnements, énergies par cadran, TURPE,
taxes — exposées via une **API REST** pour Odoo, des notebooks et tout outil de suivi énergétique.

> **Pour qui ?** Un fournisseur d'électricité (EDN, Enargia…) qui veut **recalculer** lui-même
> ce qu'il facture, à partir de la donnée brute Enedis, sans dépendre des agrégats
> « moisniversaire » du distributeur — inexploitables pour une facturation au mois calendaire.

**Stack** : Polars + DuckDB (zéro pandas, [ADR-0002](docs/adr/0002-polars-uniquement.md)),
ingestion ELT dlt + dbt, FastAPI, Pandera pour la validation.
**Langue** : tout le code et la doc sont en français — le domaine (Enedis, CRE, TURPE, accise)
l'est intrinsèquement ([ADR-0004](docs/adr/0004-langue-francaise.md)).

---

## 🏗️ Architecture

```mermaid
graph TB
    SFTP[/SFTP Enedis<br/>flux chiffrés AES\]
    ODOO[(Odoo ERP)]

    SFTP -->|dlt : déchiffre · dézippe · parse| RAW[(DuckDB · flux_raw<br/>brut JSON, 1 ligne/fichier)]
    RAW -->|dbt : linéarisation SQL| FLUX[(DuckDB · flux_enedis<br/>flux_* + marts spine/releves)]

    FLUX -->|loaders SELECT *| CORE[Pipelines core<br/>Polars LazyFrame]
    ODOO -->|OdooReader · lecture| CORE
    CORE -->|builds| API
    FLUX --> API[API REST<br/>FastAPI]

    API -->|JSONL · Arrow · XLSX| CLIENT[electricore-client<br/>notebooks · souscriptions_odoo]
    API <-->|in-process| BOT[Bot Telegram]
    NB[Notebooks opérateur] -->|écriture validée à la main| ODOO

    style API fill:#4CAF50,stroke:#2E7D32,color:#fff
    style FLUX fill:#1976D2,stroke:#0D47A1,color:#fff
    style RAW fill:#1976D2,stroke:#0D47A1,color:#fff
    style ODOO fill:#FF9800,stroke:#E65100,color:#fff
    style CORE fill:#9C27B0,stroke:#4A148C,color:#fff
```

Deux principes structurants :

- **L'API est le hub.** Bot, notebooks, intégrations et clients externes passent tous par l'API
  REST ; DuckDB, les pipelines `core/` et les adaptateurs ERP sont des composants internes
  ([ADR-0009](docs/adr/0009-architecture-api-centrique.md)). L'API est en **lecture seule** vis-à-vis
  d'Odoo : toute écriture est appliquée à la main par un opérateur, depuis un notebook
  ([ADR-0012](docs/adr/0012-api-read-only-odoo.md)).
- **`core/` est strictement ERP-agnostique.** Il ne dépend que de Polars/DuckDB/Pandera/stdlib —
  garanti par un test de pureté CI. Tout adaptateur ERP vit dans `integrations/`
  ([ADR-0016](docs/adr/0016-core-erp-agnostique.md)).

### Les modules en un coup d'œil

| Module | Rôle | Doc |
|--------|------|-----|
| 📥 **`ingestion/`** | ELT dlt + dbt : SFTP Enedis → DuckDB (déchiffrement AES, landing brut, linéarisation SQL) | [README](electricore/ingestion/README.md) · [docs/ingestion.md](docs/ingestion.md) |
| 🧮 **`core/`** | Calculs énergétiques en Polars pur (`loaders` · `pipelines` · `builds` · `models`), ERP-agnostique | [CONTEXT.md](electricore/core/CONTEXT.md) |
| 🔌 **`integrations/odoo/`** | Adaptateur Odoo : `OdooReader` / `OdooQuery` / `OdooWriter` | [Query Builder Odoo](docs/odoo-query-builder.md) |
| 🌐 **`api/`** | API REST FastAPI : flux, relevés, taxes, facturation, chronologie, ingestion | [README](electricore/api/README.md) |
| 🤖 **`bot/`** | Bot Telegram : UI opérationnelle, client de l'API (zéro logique métier) | [README](electricore/bot/README.md) |
| ⚙️ **`config/`** | Registre runtime pydantic-settings + règles tarifaires CSV (TURPE, accise, CTA) | [ADR-0024](docs/adr/0024-trois-registres-de-savoir.md)/[0025](docs/adr/0025-registre-runtime-pydantic-settings.md) |
| 📦 **`packages/electricore-client/`** | **Client léger distribué séparément** (PyPI) : httpx + pydantic, flux JSONL typé | [README](packages/electricore-client/README.md) · [ADR-0043](docs/adr/0043-electricore-client-paquet-separe.md) |

> `electricore/operator_launcher.py` (commande `electricore-notebooks`) est un **pont
> transitoire** vers les notebooks opérateur (voir la section Notebooks plus bas).
> Le dossier `electricore/client/` est un résidu vide : le vrai client est
> `packages/electricore-client/`.

---

## 🚀 Démarrage rapide

### Prérequis

- Python 3.12+ et [uv](https://docs.astral.sh/uv/getting-started/installation/)

```bash
git clone https://github.com/Energie-De-Nantes/electricore.git
cd electricore

uv sync                                   # runtime : core + API + bot
uv sync --extra ingestion --extra dbt     # + ingestion SFTP (dlt + dbt)
uv sync --extra viz                        # + libs notebooks (marimo, altair, plotly)
uv sync --extra ingestion --extra dbt --extra viz   # dev local complet
```

### 1. Ingérer les flux Enedis → DuckDB

```bash
uv run --extra ingestion --extra dbt python -m electricore.ingestion test   # smoke : 2 fichiers/flux
uv run --extra ingestion --extra dbt python -m electricore.ingestion r151    # un flux
uv run --extra ingestion --extra dbt python -m electricore.ingestion all     # production : tous les flux
uv run --extra ingestion --extra dbt python -m electricore.ingestion rebuild # dbt seul (zéro réseau, ~13 s)
```

Résultat : la base `electricore/ingestion/flux_enedis_pipeline.duckdb` (schéma `flux_raw` pour le
brut, `flux_enedis` pour les tables linéarisées et les marts). Voir [docs/ingestion.md](docs/ingestion.md).

### 2. Calculer une facturation mensuelle (core)

```python
from electricore.core.builds.contexte_mensuel import contexte_du_mois

# Compose : spine/chronologie (dbt) → historique → abonnements + énergie → facturation
ctx = contexte_du_mois("2026-05-01")   # None → dernier mois disponible

ctx.facturation_mensuelle   # méta-périodes mensuelles agrégées (validé Pandera)
ctx.abonnements             # périodes d'abonnement + TURPE fixe
ctx.energie                 # périodes d'énergie par cadran + TURPE variable
ctx.releves_utilises        # relevés tracés = relevés utilisés (traçabilité d'index, ADR-0038)
ctx.historique_enrichi      # substrat d'événements filtré sur l'horizon
```

Les loaders DuckDB sont des **query builders fluides immuables** ([ADR-0007](docs/adr/0007-query-builders.md))
qui poussent les filtres dans le `WHERE` :

```python
from electricore.core.loaders import c15, r151, releves, spine, chronologie

historique = c15().filter({"Date_Evenement": ">= '2024-01-01'"}).limit(100).collect()
tous_releves = releves().filter({"prm": ["PDL123"]}).collect()   # mart canonique (ADR-0029)
```

> Fonctions disponibles : `c15()`, `r151()`, `r15()`, `f15()`, `r64()` (flux Enedis bruts) et
> `releves()`, `spine()`, `chronologie()`, `affaires()` (marts transverses).

### 3. Consommer l'API

```bash
uv run uvicorn electricore.api.main:app --reload
curl http://localhost:8000/health
curl -H "X-API-Key: votre_cle" "http://localhost:8000/releves?prm=12345678901234&limit=10"
open http://localhost:8000/docs   # Swagger interactif
```

Depuis un consommateur externe, le **client léger** typé (paquet PyPI distinct) évite de tirer
polars/duckdb/fastapi :

```bash
pip install electricore-client            # base : httpx + pydantic
pip install "electricore-client[arrow]"   # + client Arrow (DataFrames polars)
```

```python
from electricore_client import ElectricoreClient

client = ElectricoreClient(url="https://electricore.example", api_key="…")

# Méta-périodes : flux JSONL typé, sans enveloppe ni pagination
with client.meta_periodes(mois="2026-05-01", rsc=["RSC1"]) as flux:
    for periode in flux:        # PeriodeMeta (releves_utilises imbriqués, source_hash)
        ...

# Chronologie facturiste : faits + verdicts, sans montant
with client.chronologie(pdl="12345678901234") as flux:
    lignes = flux.collect()
```

### 4. Piloter via le bot Telegram

L'exploitation quotidienne (ingestion, exports, taxes, contrôles pré-facturation) passe par un
bot Telegram ([ADR-0010](docs/adr/0010-bot-telegram-ui-operationnelle.md)) démarré dans le process
de l'API quand `BOT__TOKEN` est défini. Cinq domaines : `/ingestion`, `/flux`,
`/perimetre`, `/taxes`, `/facturation` (sans argument = clavier découvrable ; avec argument =
action directe). Voir le [README du bot](electricore/bot/README.md).

---

## 🗃️ Modèle de données — la spine assemblée en dbt

Le changement structurant récent ([ADR-0041](docs/adr/0041-chronologie-contrat-spine-relationnelle-dbt.md),
[ADR-0045](docs/adr/0045-relations-spine-reference-cle-normalisation-chronologie-releves.md)) tient
en une phrase : **le cœur consomme, dbt assemble**. La *Chronologie du contrat* — la séquence
ordonnée des faits d'une situation contractuelle — n'est plus reconstruite dans des DataFrames
Polars ; c'est une **spine relationnelle assemblée entièrement en dbt**, que le cœur se contente
de **filtrer et découper**.

Trois marts dbt forment le substrat (schéma `flux_enedis`, exposés hors du namespace `/flux`,
[ADR-0032](docs/adr/0032-modeles-marts-hors-flux-namespace.md)) :

- **`releves`** — la ligne de temps **canonique** des relevés : union arbitrée des sources
  (priorité `C15 > R64 > R151`), dédupliquée par **identité métier** du relevé, harmonisation
  R151 J→J+1 portée ici, `releve_id` = clé courte stable. Source de vérité unique des valeurs
  d'index ([ADR-0028](docs/adr/0028-identite-releve-cle-metier-priorite-sources.md)/[0029](docs/adr/0029-modele-releves-canonique-dbt-assemble-coeur-arbitre.md)).
- **`spine_contrat`** — l'épine `(pdl, ref_situation_contractuelle, date, source, type_fait)` :
  événements C15 ∪ grille FACTURATION calendaire (1ᵉʳ de chaque mois), avec les **attributs de
  situation forward-fillés en SQL** (FTA, puissance, niveau d'ouverture…).
- **`chronologie_releves`** — la projection énergie de la spine : bornes FACTURATION appariées
  aux relevés périodiques au **grain jour** (equi-join, l'ancien `join_asof` ±4 h a disparu du cœur).

Les marts sont **indépendants de l'horizon** ; l'horizon n'est qu'un **filtre** posé au boundary du
cœur, ce qui préserve la pureté (deux runs à horizon fixe découpent identiquement). Une seule
spine, **N frises** : le substrat est partagé, mais les découpages **abonnement** et **énergie**
restent séparés ([ADR-0023](docs/adr/0023-periodisations-separees-abonnement-energie.md)) — ils se
croisent, ils ne s'imbriquent pas.

### Vocabulaire essentiel

- **PDL** : point de livraison physique (14 chiffres, un Linky). **RSC** : situation contractuelle
  d'un PDL — c'est le **grain de facturation** (un PDL qui change de RSC en cours de mois porte deux
  méta-périodes). **FTA** : formule tarifaire d'acheminement (sélectionne la grille TURPE et le
  nombre de cadrans).
- **Chronologie du contrat (RSC)** vs **Chronologie du point (PDL)** vs **Chronologie des relevés**
  (projection énergie). Chaque périodisation est `filtre(spine) ⨝ relation`.
- **Cadrans** (convention `grandeur_cadran_unité`) : `base` ; `hp`/`hc` ; `hph`/`hch`/`hpb`/`hcb`
  (4 quadrants saison × heures). Index en kWh **entiers** (floor au boundary dbt,
  [ADR-0034](docs/adr/0034-index-kwh-entiers-floor-au-boundary-dbt.md)).
- **TURPE fixe** (part puissance) vs **variable** (part énergie). **Accise** (TICFE) et **CTA**
  (taxe sur le TURPE fixe). Règle d'intégration : electricore livre le **montant €** quand il
  possède l'assiette, le **taux** sinon ([ADR-0027](docs/adr/0027-endpoint-lecture-meta-periodes-odoo-tire.md)).
- **Qualité de période** (`réelle`/`estimée`/`incalculable`, [ADR-0033](docs/adr/0033-qualite-periode-remplace-data-complete-coverage.md))
  et **statut de communication** (`communicante`/`non_communicante`, [ADR-0036](docs/adr/0036-statut-communication-routage-energie-grain-meta.md)) :
  l'effet et la cause, deux axes jumeaux remplaçant les anciens `data_complete`/`coverage`.

Détails : [docs/contrat-meta-periodes.md](docs/contrat-meta-periodes.md),
[docs/conventions-dates-enedis.md](docs/conventions-dates-enedis.md),
[docs/qualite-donnees-r151.md](docs/qualite-donnees-r151.md), et le glossaire
[`electricore/core/CONTEXT.md`](electricore/core/CONTEXT.md).

---

## 📡 API REST

Authentification par en-tête **`X-API-Key`** (endpoints publics : `/`, `/health`, `/docs`,
`/redoc`, `/openapi.json`). Pendant une ingestion, les routes de lecture renvoient `503` le temps
que le writer DuckDB relâche le verrou.

| Route | Rôle |
|-------|------|
| `GET /health`, `GET /` | Statut, fraîcheur de la base, tables disponibles (public) |
| `GET /flux/{table}` · `/info` · `.xlsx` · `.arrow` | Flux Enedis bruts (JSON paginé, Arrow, XLSX) |
| `GET /releves` · `/info` · `.xlsx` · `.arrow` | Mart canonique des relevés (ADR-0029) |
| `GET /perimetre/affaires` | Cockpit des affaires SGE ouvertes (X12/X13) |
| `POST /ingestion/run` · `GET /ingestion/jobs` · `/jobs/{id}` | Déclencher et suivre les jobs d'ingestion |
| `GET /taxes/millesimes` · `/peremption` | Millésimes et péremption des taux régulés (sans ERP) |
| `GET /taxes/accise/*` · `/cta/*` | Rapports & détails Accise / CTA (XLSX, Arrow) — *requiert Odoo* |
| **`GET /facturation/meta-periodes`** | Méta-périodes mensuelles en **flux JSONL typé** |
| **`GET /facturation/chronologie`** | Frise facturiste d'un `pdl` ou `rsc` (faits + verdicts, **sans montant**) |
| **`POST /facturation/turpe-variable`** | Calculateur TURPE variable (RPC typé, l'appelant fournit l'assiette) |
| `GET /facturation/rapport.xlsx` · `documents.xlsx` · `check/odoo` | Rapports & contrôles pré-facturation — *requiert Odoo* |
| `GET /admin/api-keys` | Configuration des clés API |

Les routes **JSONL** (`meta-periodes`, `chronologie`) répondent une ligne JSON par objet, **sans
enveloppe ni pagination** ; les métadonnées (version de contrat, mois, grain) voyagent en en-têtes
HTTP. Chaque ligne est validée en construisant le modèle pydantic — impossible d'émettre une ligne
hors contrat. Les routes marquées *requiert Odoo* renvoient `501` et sont masquées du bot sur une
instance sans ERP. Détails : [README de l'API](electricore/api/README.md).

---

## 📓 Notebooks & lanceur opérateur

Les notebooks [Marimo](notebooks/) (réactifs, Polars, [ADR-0002](docs/adr/0002-polars-uniquement.md))
servent deux publics. En **dev** :

```bash
uv run marimo edit notebooks/   # édition complète (pipelines, validations TURPE, exploration Odoo)
```

Pour un **opérateur non-dev**, un pont transitoire ([#414](https://github.com/Energie-De-Nantes/electricore/issues/414))
sert les notebooks Odoo opérationnels (`accueil`, `injection_rsc`, `facturation`) en applications
marimo **lecture seule**, sans git ni code :

```bash
uv sync --extra notebooks
uv run electricore-notebooks   # valide l'env, sert les apps sur localhost, ouvre le navigateur
```

Variables requises : creds Odoo (`ODOO__*`) + `ELECTRICORE_API_URL` + `ELECTRICORE_API_KEY`
(voir [docs/configuration.md](docs/configuration.md)). Les notebooks qui écrivent dans Odoo gardent un **mode
simulation activé par défaut** et un bouton « Injecter dans Odoo » explicite. Ce lanceur est voué à
disparaître à l'arrivée de `souscriptions_odoo`.

---

## 📦 Déploiement en production

ElectriCore tourne en **stack Docker Compose** sur un VPS, une instance par fournisseur
([ADR-0011](docs/adr/0011-deploiement-vps-docker.md)/[0015](docs/adr/0015-deploiement-multi-instance.md)) :
trois conteneurs (API + bot in-process, scheduler d'ingestion cron, reverse-proxy Caddy avec TLS
automatique). L'ingestion n'est **pas** continue — c'est un cron nocturne qui appelle l'API.

Un script provisionne tout depuis un VPS Ubuntu/Debian frais (~5-10 min), durcissement SSH inclus
([ADR-0031](docs/adr/0031-durcissement-ssh-vps-utilisateur-ops.md)) :

```bash
ssh root@<vps>
curl -fsSL https://raw.githubusercontent.com/Energie-De-Nantes/electricore/main/deploy/install.sh -o install.sh
sudo bash install.sh \
    --slug <slug> --domain <slug>.electricore.fr --email ops@example.com \
    --deploy-repo git@github.com:Energie-De-Nantes/electricore-secrets.git
```

Le script crée un user système dédié, installe Docker, configure UFW, télécharge la stack
tag-pinnée sous `/srv/<slug>/` ([ADR-0017](docs/adr/0017-layout-deploiement-srv-slug.md)), génère
l'identité **age** de la box puis pull la config (`config.env` clair + `secrets.env` chiffré
SOPS+age) depuis le dépôt de déploiement privé, **valide le split et déchiffre**, vérifie le DNS,
démarre les conteneurs et lance une ingestion test. Sauvegardes DuckDB nocturnes (`EXPORT DATABASE`, rétention 14 jours).
Relancer avec le même `--slug` = mode reconfiguration (rotation des clés AES, bump de version…),
sans toucher à la base.

Guide complet : [docs/deploiement.md](docs/deploiement.md).

### Configuration & secrets-as-code

La config runtime est un **registre pydantic-settings** par domaine (`sftp`/`aes`/`duckdb`/`api`/
`bot`/`odoo`), avec validation fail-fast par point d'entrée
([ADR-0024](docs/adr/0024-trois-registres-de-savoir.md)/[0025](docs/adr/0025-registre-runtime-pydantic-settings.md)).
En **dev** (`uv run`), elle se lit depuis un simple `.env`.

En **production**, les secrets passent en **secrets-as-code**
([ADR-0044](docs/adr/0044-secrets-as-code-sops-age.md), récemment adopté — bascule de l'instance
vivante à la prochaine release). Le `.env` se scinde en deux fichiers versionnés dans un dépôt de
déploiement **privé**, et chaque box génère sa propre identité **age** :

- **`config.env`** — config CLAIRE (substitutions compose `ELECTRICORE_VERSION`/`BACKUPS_PATH`,
  `INSTANCE_SLUG`, `BOT__NOTIFY_CHAT_ID`) ;
- **`secrets.env`** — credentials **chiffrés SOPS + age**, déchiffrés à l'**entrypoint de l'image**
  (`sops exec-env` — jamais de fichier en clair). Le layout `providers/<slug>/` isole chaque
  fournisseur cryptographiquement (une box ne déchiffre que les siens).

```bash
# config.env — CLAIR, versionné (substitutions compose + config non-secrète)
INSTANCE_SLUG=monfournisseur
ELECTRICORE_VERSION=latest
BACKUPS_PATH=/srv/monfournisseur/backups
BOT__NOTIFY_CHAT_ID=-1001234567890                # canal d'alerte du bot (optionnel)

# secrets.env — CHIFFRÉ (SOPS + age) : uniquement des credentials
API__TROUSSEAU__librewatt__KEY=cle_consommateur_32_caracteres   # 1 clé/consommateur, label dynamique
SFTP__URL=sftp://utilisateur:mot_de_passe@hote:22/chemin
BOT__TOKEN=token_botfather                        # bot = process de l'API
ODOO__URL=https://votre-instance.odoo.com         # + __DB / __USERNAME / __PASSWORD
# Trousseau de clés AES (ADR-0037/0040) : un <label> parlant par clé, sélection par essai.
# __IV optionnel : absent ⇒ schéma IV-préfixé (AES-256) ; présent ⇒ IV-fixe (AES-128).
AES__TROUSSEAU__aes256_2026__KEY=cle_hex_64        # AES-256 : pas de __IV
# AES__TROUSSEAU__aes128_2024__KEY=ancienne_cle / __IV=ancien_iv   # AES-128 historique
```

Inventaire complet des variables : [docs/configuration.md](docs/configuration.md) · procédure de
déploiement : [docs/deploiement.md](docs/deploiement.md) · gabarit d'exemple :
[`deploy/providers/example/`](deploy/providers/example/).

**Rotation des clés AES** : éditer le `secrets.env` chiffré (`sops providers/<slug>/secrets.env`),
ajouter la nouvelle clé sous un nouveau label, garder les anciennes, commit → pull → restart — chaque
fichier déchiffre par essai avec la clé qui marche ; un flux qui a des fichiers mais **0 déchiffrement
réussi** fait passer le job à `failed` et alerte le bot
([ADR-0037](docs/adr/0037-trousseau-cles-aes-n-cles-selection-par-essai.md)/[0040](docs/adr/0040-schema-dechiffrement-aes-iv-prefixe.md)).

---

## 🧪 Développement & tests

```bash
# Setup dev recommandé
uv sync --extra ingestion --extra dbt --group test --group typecheck

uv run --group test pytest              # suite complète (~30 s)
uv run --group test pytest -n auto      # exécution parallèle (pytest-xdist)
uv run --group test pytest -m unit      # markers : unit/integration/slow/smoke/duckdb/odoo/hypothesis
uv run --group test pytest --cov=electricore   # couverture (plancher CI : 45 %)

uvx ruff check --fix ; uvx ruff format  # lint + format
uv run --group typecheck mypy           # typage (surface publique core/)
uvx pre-commit install                  # hooks ruff/gitleaks (+ --hook-type pre-push pour pytest)
```

Près de **800 fonctions de test** réparties sur 119 fichiers, sans secret requis (les tests
dépendant d'Odoo s'auto-skippent). Couverture : fixtures + snapshots Syrupy (gros du filet),
tests d'expressions Polars, contrats Pandera, et property-based Hypothesis ; **golden d'ingestion**
générés depuis les XSD Enedis (parité dbt garantie en CI) ; **tests d'architecture** verrouillant
la pureté ERP du cœur ([ADR-0016](docs/adr/0016-core-erp-agnostique.md)) et les imports par rôle
([ADR-0019](docs/adr/0019-roles-loaders-pipelines-builds-integrations.md)).

**CI** (GitHub Actions) : lint + typecheck + tests (matrice Python 3.12/3.13), plus un job
`test-client` qui prouve en venv isolé que `electricore-client` ne tire ni polars ni duckdb ni
fastapi. Les **releases** publient l'image `ghcr.io/energie-de-nantes/electricore` (sur tag `v*`,
build → scan secrets → smoke → push) et le client sur PyPI via OIDC (sur tag `client-v*`, versionné
indépendamment).

**Contribution** : `main` est protégé. Brancher → commit (Conventional Commits en français) →
pousser → ouvrir une PR → la CI tourne → **le merge est une étape humaine** sur le site. Détails :
[CONTRIBUTING.md](CONTRIBUTING.md). Nouvel arrivant (profil Rust/Java) :
[docs/transmission.md](docs/transmission.md).

---

## 🗺️ Roadmap

Le corps de ce README décrit la **réalité livrée** sur `main`. Les directions en cours :

- **Protection des secrets au repos** — follow-ups de la bascule secrets-as-code
  ([ADR-0044](docs/adr/0044-secrets-as-code-sops-age.md), déjà mergée pour le mécanisme) : chiffrement
  disque (LUKS + Tang/NBDE), isolation `raw.db`/`serve.db`, durcissement DuckDB de l'API, et OpenBao
  quand la flotte le justifiera.
- **`souscriptions_odoo`** — addon qui consommera l'API via `electricore-client` et **remplacera**
  le lanceur de notebooks opérateur transitoire.
- **Régularisation des contrats lissés** — recalcul a posteriori des contrats facturés au lissé
  ([#191](https://github.com/Energie-De-Nantes/electricore/issues/191)).
- **Estimation des périodes non-communicantes via R15**
  ([#322](https://github.com/Energie-De-Nantes/electricore/issues/322)).
- **Nouvelles sources** — API SOAP Enedis (alternative SFTP), courbes Axpo, autres fournisseurs.

Le suivi se fait en [issues GitHub](https://github.com/Energie-De-Nantes/electricore/issues) et en
[décisions d'architecture](docs/adr/).

---

## 📚 Documentation

- **Décisions d'architecture** — [docs/adr/](docs/adr/) (45 ADR : monorepo, Polars, DuckDB, dlt+dbt,
  API-centrique, core ERP-agnostique, spine/chronologie, client séparé, trousseau AES…)
- **Carte des contextes** — [CONTEXT-MAP.md](CONTEXT-MAP.md) + `CONTEXT.md` par module
- **Ingestion** — [README module](electricore/ingestion/README.md) · [docs/ingestion.md](docs/ingestion.md)
- **API & client** — [README API](electricore/api/README.md) · [README client](packages/electricore-client/README.md)
- **Bot** — [README bot](electricore/bot/README.md)
- **Modèle de données** — [contrat méta-périodes](docs/contrat-meta-periodes.md) · [conventions de dates](docs/conventions-dates-enedis.md) · [qualité R151](docs/qualite-donnees-r151.md)
- **Odoo** — [Query Builder Odoo](docs/odoo-query-builder.md)
- **Déploiement & config** — [docs/deploiement.md](docs/deploiement.md) · [docs/configuration.md](docs/configuration.md)
- **Onboarding** — [docs/transmission.md](docs/transmission.md)

---

## 📄 Licence

[AGPL-3.0](LICENSE).

## 🙏 Remerciements

[Polars](https://pola.rs) · [DuckDB](https://duckdb.org) · [dlt](https://dlthub.com) ·
[dbt](https://www.getdbt.com) · [FastAPI](https://fastapi.tiangolo.com) ·
[Pandera](https://pandera.readthedocs.io) · [Marimo](https://marimo.io) ·
[httpx](https://www.python-httpx.org) · [pydantic](https://docs.pydantic.dev) · [uv](https://docs.astral.sh/uv/).
