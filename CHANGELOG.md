# Changelog

Toutes les modifications notables de ce projet seront documentées dans ce fichier.

Le format est basé sur [Keep a Changelog](https://keepachangelog.com/fr/1.0.0/),
et ce projet adhère au [Semantic Versioning](https://semver.org/lang/fr/).

---

## [Unreleased]

### ♻️ Renommage — le module `etl` devient `ingestion`

Le nom « ETL » décrivait une technique abandonnée (le procédé est ELT depuis la
bascule dbt, ADR-0020/0021) ; « ingestion » nomme la fonction et était déjà le
terme canonique de la doc. Changements cassants et transitions :

- **Package** : `electricore/etl/` → `electricore/ingestion/` ; CLI :
  `uv run python -m electricore.ingestion <all|test|rebuild|resync|flux…>` ;
- **Extra** : `uv sync --extra etl` → `uv sync --extra ingestion` (**cassant**) ;
- **Routes API** : `/etl/run`, `/etl/jobs` → `/ingestion/run`, `/ingestion/jobs`
  (**cassant**) ; les alias de transition `/etl/*` sont **retirés** (#193) et
  répondent désormais `404` — la crontab doit appeler `/ingestion/run`
  (`deploy/docker/crontab` l'est déjà) ;
- **Bot** : `/ingestion` remplace `/etl`, **sans alias** (#193) ; les callbacks
  `etl:*` des claviers postés avant le renommage ne routent plus ;
- **Compose** : service `etl-scheduler` → `ingestion-scheduler`, conteneur
  `electricore-etl` → `electricore-ingestion` (recréation au prochain pull).

## [2.1.1] - 2026-06-12

### 🐛 Corrections

- **Check Odoo figé sur ⏳** (incident prod 2026-06-12) : `KeyError` dans le
  rendu des blocs « factures draft » et « lissés » — colonnes réelles du check
  (`name`, `name_account_move`, `categ_names`) alignées. Bug hérité du bot v1,
  latent tant qu'aucune facture draft n'existait.
- **Filet anti-⏳** : toute erreur de formatage/édition du check s'affiche
  désormais en `❌ …` dans Telegram au lieu de figer le message.
- **Garde 4096 caractères** : un résumé de check trop long pour Telegram est
  tronqué aux frontières de lignes et bascule automatiquement sur le XLSX de
  détail.

---

## [2.1.0] - 2026-06-11

### 🤖 Bot Telegram : surface par domaines hybrides (ADR-0022)

Refonte complète de la surface du bot (issues #150–#160) : 11 commandes plates →
**5 domaines métier** (`/etl`, `/flux`, `/perimetre`, `/taxes`, `/facturation`).
Sans argument, un domaine ouvre un **clavier inline** ; avec arguments, raccourci
power-user. Guide : [electricore/bot/README.md](electricore/bot/README.md).

#### 💥 Breaking

- **Rupture du contrat de commandes, sans alias** (big bang, ADR-0022) :
  `/status`, `/stats`, `/export`, `/entrees`, `/sorties`, `/check` disparaissent —
  absorbées respectivement par `/etl statut`, `/flux stats`, `/flux export`,
  `/perimetre entrees|sorties`, `/facturation check`. L'ancien `/flux` plat et
  `/facturation [date]` changent de forme (`/flux`, `/facturation documents [date]`).
- **`/etl reset` retiré** de la surface du bot (déprécié côté runner — `resync`
  le remplace, derrière une **confirmation à deux taps**).

#### ✨ Nouveau

- **Menu natif Telegram** (`setMyCommands`) publié au démarrage, **adapté à
  l'instance** : sans ERP configuré, `/taxes` et `/facturation` sont masqués et
  répondent un message explicite (P2.5, traduction bot d'ADR-0016).
- **Modes ETL réels exposés** : `rebuild`, `resync`, sélection de flux arbitraire
  (`/etl r151 c15`) — l'API `POST /etl/run` accepte désormais les listes de flux.
- **Suivi de job par édition** : le message de lancement s'édite
  (`⏳ running` → `✅`/`❌` + sortie) au lieu d'un second message.
- **Alertes proactives** : `TELEGRAM_NOTIFY_CHAT_ID` — alerte 🚨 sur tout job ETL
  `failed`, y compris ceux du scheduler nocturne.
- **Fraîcheur des données** : `/flux` stats affiche la dernière date *métier* par
  table (`GET /flux/{table}/info` expose `derniere_date`).
- Rendu **HTML partout** (fin du mélange Markdown V1/V2), descriptions des tables
  dans le menu `/flux`, `/start` annonce l'instance servie
  (convention `@<slug>_electricore_bot`).
- API : `GET /facturation/check/odoo.xlsx` (détail du check pré-facturation) —
  le bot redevient strictement client HTTP (garde-fou d'architecture en CI).

#### 🔧 Interne

- `bot.py` (monolithe) supprimé : package par domaine (`handlers/`), allowlist
  factorisée en décorateur, fakes Telegram partagés côté tests (+39 tests).

---

## [2.0.0] - 2026-06-11

### 🏗️ Ingestion ELT : la linéarisation des flux vit en dbt (ADR-0020 → ADR-0021)

Release **majeure** : le chemin d'ingestion maison (parseur Python piloté par le DSL
`flux.yaml`) est remplacé par une architecture ELT — dlt dépose les documents Enedis
**intégraux** en colonne JSON (`flux_raw`), dbt les linéarise en SQL (`flux_enedis`).
Parité record-par-record prouvée 3× (golden, cache local 4 400 XML, corpus SFTP
complet ~700 k lignes, 7/7 tables). Guide : [docs/ingestion.md](docs/ingestion.md).

#### 💥 Breaking

- **Historique re-matérialisé ≠ legacy là où le legacy avait tort** : la validation a
  corrigé 5 défauts latents — relevés agrégés par PDL (chimères inter-événements),
  ~75 % des index R15 mélangeant index/consommation, relevés multiples perdus (jusqu'à
  20/PRM sur R151), re-livraisons F15 double-comptées (261 lignes), gagnant R64
  arbitraire. Le grain des tables R15/R151 devient **le relevé**.
- **`pipeline_production.py` supprimé** → `pipeline_dbt.py` (modes `test`/`all`/
  sélection/`rebuild`/`resync` ; `reset` déprécié, alias de `resync`).
- **`flux.yaml` réduit au mouvement** (`file_pattern`/`format`/`file_regex`) — le DSL
  de sélection (`row_level`/`data_fields`/`nested_fields`) disparaît avec le moteur.
- L'image Docker embarque l'extra `dbt` ; premier run : nouvel état incrémental
  (re-téléchargement complet, ~15 min), tables `flux_enedis` remplacées par les
  versions typées.

#### ✨ Nouveau

- **Mode `rebuild`** : re-matérialiser toutes les tables depuis le brut, zéro réseau
  (~13 s pour 700 k lignes) — le geste standard après un changement de modèle.
- **Types à la source** (XSD Enedis) : TIMESTAMPTZ/DATE/BIGINT/DOUBLE portés par les
  tables ; instants ancrés `Europe/Paris` indépendamment du fuseau de session ;
  domaine de cadrans fermé (contrat de colonnes stable).
- **Dédup par construction** : re-livraisons Enedis (merge `file_name`), R64
  multi-fenêtres (gagnant déterministe = livraison la plus récente).
- **Filet d'ingestion en CI** : golden générés par le chemin de production, fixtures
  XSD maximales, data tests dbt + contrat de types.
- `docs/ingestion.md` (schéma Excalidraw + recettes), `docs/configuration.md`
  (inventaire complet), ADR-0020/0021.

#### 🧱 Architecture (revue du 11/06, depuis la rc1 — issues #142–#146)

- **Le livrable facturation descend en core** : assemblage dans
  `core/builds/rapport_facturation.py` + `contexte_mensuel.py`, I/O Odoo dans
  `integrations/odoo/sources.py`, wire-up dans `api/services/facturation_service.py` —
  `integrations/odoo/facturation.py` supprimé. Garde-fou CI en whitelist (ADR-0019
  règle 4) : `integrations/` n'importe de `core` que `models` et `loaders`.
- **Vraie passe-plat dans `rapprocher()`** : sortie = colonnes d'entrée + colonnes
  calculées, plus aucune colonne ERP nommée en core. ⚠️ le détail rapprochement gagne
  `est_brouillon` et change d'ordre de colonnes (l'ordre facturiste est porté par
  `feuilles_rapport_*`) ; collision de noms → erreur claire au seam.
- **`chemin_base_duckdb()`** : résolution unique du chemin de la base
  (`electricore/config/`) — `DUCKDB_PATH` (`.env` compris) honoré par les loaders,
  l'API, le runner dbt **et** les tools ; défaut absolu indépendant du CWD.
- **`contexte_du_mois(mois)`** : entrée I/O du contexte mensuel ; `charger(frames)`
  reste la composition pure (scindage prévu par ADR-0019).

#### 🔧 Corrections

- `DUCKDB_PATH` honoré par le runner (volume Docker) ; smoke-test release réaligné.

---

## [1.7.0] - 2026-06-07

### 🏗️ core/ ERP-agnostique + déploiement script-first + API épaisse

Release majeure structurée autour de deux ADR architecturaux et de la finalisation de l'« API épaisse » (notebooks de prod migrés en HTTP).

#### Architecture — ADR-0016 (`core/` ERP-agnostique)

- **ADR-0016** ([`docs/adr/0016-core-erp-agnostique.md`](docs/adr/0016-core-erp-agnostique.md)) — `core/` ne dépend plus d'aucun ERP. Tout l'Odoo migre vers `electricore/integrations/odoo/` (lecteurs, query builder, modèles Pandera, helpers, orchestrations).
- **Refactor `core/` → `integrations/odoo/`** — `OdooReader`, `OdooQuery`, `OdooWriter`, helpers (`commandes`, `factures`, `lignes_factures_du_mois`…) et modèles (`FactureOdoo`, `CommandeVenteOdoo`…) sortis de `core/`. Imports `from electricore.integrations.odoo import …` (breaking).
- **Orchestrations Odoo** — `rapprocher_facturation_mensuelle`, `calculer_cta_detail`, `accise_par_contrat` déplacées dans `integrations/odoo/`. `core/pipelines/` reste strictement Polars/DuckDB.
- **Test architectural** ([`tests/architecture/test_core_erp_agnostique.py`](tests/architecture/test_core_erp_agnostique.py)) — verrouille le contrat : aucun import Odoo détectable dans `core/`.
- **`integrations/odoo/decorators.py`** — décorateur `@with_odoo` qui encapsule l'ouverture/fermeture d'`OdooReader` au call-site et l'injecte en 1er argument. Services `taxes_service` / `facturation_service` deviennent des pass-through purs de sérialisation.
- **`api/serializers/`** — extraction des sérialiseurs XLSX, Arrow, ZIP hors des services pour les rendre réutilisables.
- **Glossaire / ADR / README / CLAUDE alignés** sur la nouvelle frontière `core/` ↔ `integrations/`.

#### Déploiement — ADR-0017 (script-first)

- **ADR-0017** ([`docs/adr/0017-deploiement-script-first.md`](docs/adr/0017-deploiement-script-first.md)) — layout `/srv/<INSTANCE_SLUG>/`, user dédié, bind-mount backups, `INSTANCE_SLUG` au cœur du provisioning.
- **`deploy/install.sh`** — installateur idempotent (mode fresh + mode `reconfigure`) avec helpers Bash modulaires dans [`deploy/lib/`](deploy/lib/). Tests unitaires + e2e (`deploy/tests/`).
- **Auto-refresh `lib/` en mode reconfigure** (#62) — élimine le piège « stale lib » lors des migrations rcN → rcN+1. Refactor : `fetch_lib_files`, `main()` guard, `_source_lib` pour permettre le sourcing dans les tests.
- **`--version` pilote le ref Git** — `latest` → `main`, sinon le tag exact ; `ELECTRICORE_VERSION` et `nano` par défaut dans l'env généré.
- **Doc déploiement** ([`docs/deploiement.md`](docs/deploiement.md)) réécrite script-first ([#50](https://github.com/Energie-De-Nantes/electricore/issues/50)).

#### API épaisse — splits rapport / détail (convention `.extension`)

- **#56 — Accise** : split en `rapport_accise` (agrégé, métier) + `accise_par_contrat` (détail brut, audit). Endpoint `/taxes/accise/{rapport,detail}.{xlsx,arrow}`.
- **#63 — CTA** : split en `rapport_cta` (par contrat-mois) + `cta_par_contrat` (détail brut Odoo). Même convention d'endpoints.
- **#64 — Facturation** : split en `rapport_facturation` (proposition mensuelle) + `facturation_du_mois` (lignes brutes Odoo avec flags `a_facturer`/`a_supprimer`).
- **#65 — Convention `.extension` étendue à `/flux/*`** : `/flux/c15/entrees.xlsx`, `/flux/c15/sorties.xlsx`, `/flux/{table}.xlsx`, `/flux/{table}.arrow`. Anciens paths segmentés supprimés (404). Documentation inline sur l'ordre des routes FastAPI (les paths à extension doivent précéder le catch-all).
- **#67 — `@with_odoo`** : collapse des 7 services pass-through (4 taxes + 3 facturation), qui n'avaient plus qu'un rôle d'ouverture/fermeture d'`OdooReader`.

#### DuckDB query builder

- **#52 — Retry-on-lock baked in** — `DuckDBQuery` gère le retry transparent sur `IO Error: Could not set lock`, plus besoin de wrappers ad hoc.
- **#53 — `c15().entrees()` / `c15().sorties()`** — méthodes builder dédiées (typage + auto-complétion) qui factorisent les filtres `evenement_declencheur`.
- **#54 — SQL injection killée** — `query_table` valide les filtres via `FluxSchema` (whitelist colonnes + opérateurs).

#### Client HTTP & Notebooks

- **Notebooks de prod migrés vers `ElectricoreClient`** — le notebook `facturation.py` consomme exclusivement l'API HTTP (`client.facturation(mois)`, `client.lignes_factures_du_mois(...)`) au lieu d'instancier `OdooReader` localement. Sépare proprement le déploiement notebook du déploiement core/ETL.

#### Fixes

- **`api_version` dynamique** ([#73](https://github.com/Energie-De-Nantes/electricore/pull/73)) — `/health` retournait `0.1.0` quel que soit le tag. Lue maintenant via `importlib.metadata.version("electricore")`, override `API_VERSION` toujours possible mais retiré de `.env.example`.
- **`deploy/install.sh`** — `latest` → ref `main`, ownership préservé par `substitute_env`/`caddyfile`/prepend, `prepend_errors_to_env` ne réécrase plus le `.env`, auto-bootstrap de `lib/` si absent.

#### Sécurité / CI

- **Scan secrets en pre-commit + avant push GHCR** — TruffleHog ajouté localement et dans le job release.
- **Pre-commit pytest hook** sur stage `pre-push` — évite de pousser un main rouge.
- Bumps `actions/checkout@6`, `docker/setup-buildx-action@4`, `docker/build-push-action@7`, `docker/login-action@4`.

#### Breaking changes

- **Imports Odoo** : `from electricore.core.loaders.odoo import …` → `from electricore.integrations.odoo import …` (ADR-0016).
- **`/flux/*` endpoints** : `/flux/c15/entrees/xlsx` → `/flux/c15/entrees.xlsx` (et équivalents). Anciens paths renvoient 404 (#65).

#### Tests

- 455 passed, 35 skipped.

---

## [1.6.1] - 2026-06-05

### 🔒 Sécurité + multi-instance + ContexteFacturation

Release patch combinant la résolution de 28 alertes Dependabot, la mise en place du déploiement multi-instance (ADR-0015) et la refonte interne de `facturation_service` autour de `ContexteFacturation`. Aucune API publique cassée.

#### Sécurité

- **28 alertes Dependabot résolues** ([`.github/dependabot.yml`](.github/dependabot.yml)) — bump `marimo>=0.23.0` (CVE-2026-39987, pre-auth RCE WebSocket, **critical**) et `lxml>=6.1.0` (CVE-2026-41066, XXE, **high**) ; `uv lock --upgrade` propage les correctifs aux transitives (aiohttp 3.14.0, urllib3 2.7.0, gitpython 3.1.50, cryptography 48.0.0, idna 3.18, pymdown-extensions 10.21.3, pytest 9.0.3, pygments 2.20.0, requests 2.34.2). Ajout de `.github/dependabot.yml` (pip + github-actions, hebdo, PRs minor/patch groupées, `versioning-strategy: lockfile-only` côté pip pour ne pas élargir les bornes upper de `pyproject.toml`) pour prévenir la dérive. Alerte #34 paramiko ≤ 4.0.0 (SHA-1, low) laissée ouverte — pas de correctif amont.

#### Ajouts

- **ADR-0015** ([`docs/adr/0015-deploiement-multi-instance.md`](docs/adr/0015-deploiement-multi-instance.md)) — modèle multi-instance : une instance Electricore par fournisseur/client final, identifiée par `INSTANCE_SLUG`. Isolation données, secrets et URL ; même codebase.
- **`INSTANCE_SLUG` exposé** ([`electricore/api/main.py`](electricore/api/main.py)) — visible dans `/health`, dans le titre OpenAPI et dans le log de boot pour identifier sans ambiguïté l'instance courante.
- **Préfixage des backups DuckDB** ([`deploy/docker/`](deploy/docker/)) — backups nommés `{INSTANCE_SLUG}-flux_enedis-YYYYMMDD.duckdb` pour éviter les collisions multi-instances dans un même bucket S3.
- **Guide de provisioning** ([`docs/deploiement.md`](docs/deploiement.md)) — procédure pas-à-pas pour démarrer une nouvelle instance sur le VPS Docker (DNS, secrets, .env, premier déploiement).
- **`ContexteFacturation`** ([`electricore/core/pipelines/facturation.py`](electricore/core/pipelines/facturation.py)) — objet immuable portant `(historique, abonnements, energies, accise)` partagés entre `rapprocher_facturation_mensuelle` et `calculer_cta_detail`. Évite le rechargement double dans `facturation_service`.
- **Décorateurs `@xlsx_endpoint` / `@arrow_endpoint` / `@zip_endpoint`** ([`electricore/api/decorators.py`](electricore/api/decorators.py)) — factorise headers, content-type et streaming des réponses binaires de l'API. Suite de tests d'intégration ([`tests/integration/test_decorators.py`](tests/integration/test_decorators.py)).

#### Modifications

- **`facturation_service`** ([`electricore/api/services/facturation_service.py`](electricore/api/services/facturation_service.py)) — consomme `ContexteFacturation` au lieu de recharger Odoo+Enedis dans chaque sous-fonction. Migration de `calculer_cta_detail` en conséquence.
- **Suppression des helpers d'orchestration morts** ([`electricore/core/pipelines/orchestration.py`](electricore/core/pipelines/orchestration.py)) — code mort depuis la migration Polars de v1.4, nettoyé.

#### Tests

- 285 passed, 35 skipped. Nouveaux tests d'intégration `test_decorators.py` et `test_health_endpoint.py`.

---

## [1.6.0] - 2026-06-04

### 🧮 Facturation — flags d'état + glossaire Historique

Deuxième brique de l'API épaisse : refonte du chargement Odoo pour exposer toutes les lignes de factures du mois (avec flags), résolvant la douleur de test hors période de facturation. Renommage terminologique acté par ADR-0013 pour aligner code et glossaire.

#### Ajouts

- **ADR-0013** ([`docs/adr/0013-renommage-perimetre-historique.md`](docs/adr/0013-renommage-perimetre-historique.md)) — Renommage *Périmètre* → *Historique* dans le code et le glossaire. Deux concepts distincts : `Historique` (séquence temporelle enrichie produite par `pipeline_historique`) ; `Périmètre` (snapshot à une date, conservé au glossaire sans implémentation).
- **ADR-0014** ([`docs/adr/0014-lignes-factures-du-mois-avec-flags.md`](docs/adr/0014-lignes-factures-du-mois-avec-flags.md)) — `lignes_factures_du_mois` qui ne filtre Odoo que par `sale.order.state == 'sale'` et `invoice_date` du mois ; flags `a_facturer` / `a_supprimer` matérialisent les sous-ensembles métier. Résout : tester le notebook hors période, audit a posteriori, debug.
- **`lignes_factures_du_mois(odoo, mois)`** ([`electricore/core/loaders/odoo/helpers.py`](electricore/core/loaders/odoo/helpers.py)) — LazyFrame de toutes les lignes de factures du mois cible avec les colonnes `a_facturer` (drafts + qty > 0) et `a_supprimer` (drafts + qty == 0).
- **`flags_etat_facturation(lf)`** — helper pur testable séparément (`x_invoicing_state`, `state_account_move`, `quantity` → flags).
- **Modèle Pandera `Historique`** ([`electricore/core/models/historique.py`](electricore/core/models/historique.py)) — valide la sortie enrichie de `pipeline_historique` (impacte_*, resume_modification, événements FACTURATION).

#### Modifications

- **`LignesFactureRapprochees`** ([`electricore/core/models/lignes_facture_rapprochees.py`](electricore/core/models/lignes_facture_rapprochees.py)) — 9 → 20 colonnes nullable : méta-période Enedis (`debut`, `fin`, `data_complete`, `turpe_fixe_eur`, `turpe_variable_eur`, `ref_situation_contractuelle`, `pdl`), identifiants compteur (`num_compteur`, `type_compteur`) joints depuis l'Historique, flags d'état (`a_facturer`, `a_supprimer`). `x_lisse`, `x_pdl`, `name_product_category`, `name_product_product` rendus nullable.
- **`rapprocher_facturation_mensuelle`** ([`electricore/core/pipelines/facturation.py`](electricore/core/pipelines/facturation.py)) — nouveau paramètre `historique` (LazyFrame), join `.unique(keep='last')` sur `ref_situation_contractuelle` pour récupérer les identifiants compteur ; propage les flags ; ne projette plus aucune colonne méta.
- **`pipeline_perimetre`** → **`pipeline_historique`** ([`electricore/core/pipelines/historique.py`](electricore/core/pipelines/historique.py)) — décoré `@pa.check_types` validant le modèle `Historique`.
- **`facturation_service`** ([`electricore/api/services/facturation_service.py`](electricore/api/services/facturation_service.py)) — utilise `lignes_factures_du_mois`, résout le mois avant l'appel Odoo.
- **`notebooks/facturation.py`** — un seul call `client.facturation(mois)` côté Enedis + `lignes_factures_du_mois(odoo, mois)` côté Odoo. `lignes_a_facturer_df` et `lignes_a_supprimer` dérivés par `.filter()` sur les flags côté client. UI `mois_input` par défaut au 1er du mois courant. `sim_mode=True` + `run_button` gate préservés.

#### Suppressions (breaking)

- **`lignes_a_facturer`** et **`lignes_quantite_zero`** ([`electricore/core/loaders/odoo/helpers.py`](electricore/core/loaders/odoo/helpers.py)) — remplacés par `lignes_factures_du_mois(odoo, mois)` + filtre client `.filter(pl.col("a_facturer"))` ou `.filter(pl.col("a_supprimer"))`.
- **`pipeline_perimetre`** → renommée `pipeline_historique` (cf. ADR-0013).
- **`HistoriquePérimètre`** (modèle Pandera) supprimé — le brut C15 n'a plus de nom métier autonome, validation déplacée à la sortie de `pipeline_historique`.
- **`transform_historique_perimetre`** → renommée `transform_historique`.
- **`load_historique_perimetre`** → renommée `load_historique`.
- **`registry.py`** : `validator=None` pour C15 (validation maintenant à la sortie du pipeline).

#### Tests

- TDD complet : 4 cycles RED→GREEN sur `flags_etat_facturation` (table tests), 1 cycle sur la propagation des flags par `rapprocher_facturation_mensuelle`, fixtures unit+smoke adaptées.
- Adaptation `test_facturation_service_smoke.py` pour la nouvelle signature de `lignes_factures_du_mois` et les nouvelles colonnes.

---

## [1.5.0] - 2026-06-04

### 🧮 Facturation — API épaisse

Première brique de l'API épaisse v1.5 : extraction du rapprochement Odoo↔Enedis vers le domaine `core`, prérequis pour exposer les résultats structurés en Arrow IPC aux notebooks distants.

#### Ajouts

- **`rapprocher_facturation_mensuelle()`** ([`electricore/core/pipelines/facturation.py`](electricore/core/pipelines/facturation.py)) — fonction métier pure qui joint les lignes de facture Odoo (taggées `x_ref_situation_contractuelle`) avec la facturation Enedis mensuelle et calcule `quantite_enedis` selon la catégorie produit (HP/HC/Base/Abonnements).
- **Schéma Pandera `LignesFactureRapprochees`** ([`electricore/core/models/lignes_facture_rapprochees.py`](electricore/core/models/lignes_facture_rapprochees.py)) — validation stricte en sortie via `@pa.check_types(lazy=True)`.
- **ADR-0012** ([`docs/adr/0012-api-read-only-odoo.md`](docs/adr/0012-api-read-only-odoo.md)) — politique « API read-only sur Odoo ; écritures via notebook humain-dans-la-boucle » + règle nuancée pour OdooReader en notebook (autorisé pour enrichissement, interdit en amont d'une pipeline).
- **Glossaire** ([`electricore/core/CONTEXT.md`](electricore/core/CONTEXT.md)) — entrées « Rapprochement PDL ↔ RSC » (étape amont, notebook `injection_rsc.py`) et « Rapprochement facturation mensuelle » (étape aval, exposée par l'API) pour clarifier la distinction.

- **Endpoint `GET /facturation/arrow`** ([`electricore/api/main.py`](electricore/api/main.py)) — sérialise `lignes_facture_rapprochees` en flux Arrow IPC, lisible par `pl.read_ipc_stream`. Query param `mois=YYYY-MM-DD` ; sans paramètre, dernier mois disponible. Authentification API key, comme les autres endpoints data.
- **Endpoint `GET /taxes/accise/arrow`** ([`electricore/api/main.py`](electricore/api/main.py)) — détail Accise TICFE (table par PDL × mois × trimestre) sérialisé en Arrow IPC. Query param `trimestre=YYYY-TX` (sans : tout). Les agrégations « Par taux » et « Résumé » de l'XLSX restent à charge du notebook (group_by trivial).
- **Endpoint `GET /taxes/cta/arrow`** ([`electricore/api/main.py`](electricore/api/main.py)) — détail CTA mensuel (table par PDL × mois avec `cta_eur`, `taux_cta_pct`, `turpe_fixe_eur`, `trimestre`) sérialisé en Arrow IPC. Query param `trimestre=YYYY-TX`.
- **Extra optionnel `[viz]`** ([`pyproject.toml`](pyproject.toml)) — `altair`, `vegafusion`, `vl-convert-python`, `marimo`, `plotly[express]` quittent les dépendances obligatoires et passent en optional. L'image Docker production (`uv sync --extra etl --no-dev`) ne les inclut donc plus. Installer côté notebook avec `uv sync --extra viz`.
- **Module `electricore.client`** ([`electricore/client/__init__.py`](electricore/client/__init__.py)) — classe `ElectricoreClient(url, api_key)` avec méthodes `.facturation(mois)`, `.accise(trimestre)`, `.cta(trimestre)` retournant un `pl.DataFrame`. Extension point pour le HTTP transport DuckDBQuery prévu en v1.6.

#### Modifications

- **`api/services/facturation_service.generer_facturation_xlsx`** consomme désormais `rapprocher_facturation_mensuelle()` — comportement XLSX inchangé, logique métier simplifiée. Le chargement Odoo+Enedis est factorisé dans `calculer_lignes_facture_rapprochees()`, partagé entre les services XLSX et Arrow.

#### Correctifs

- **`generer_documents_facturation` régression #5** ([`electricore/api/services/facturation_service.py`](electricore/api/services/facturation_service.py)) — `MAPPING_CATEGORIE` avait été supprimé du module lors de l'extraction de `rapprocher_facturation_mensuelle`, laissant un `NameError` runtime dans `/facturation/documents` (déclenché par le bot Telegram). Refactor : la fonction consomme désormais `rapprocher_facturation_mensuelle` (single source of truth) + 3 tests smoke ([`tests/integration/test_facturation_service_smoke.py`](tests/integration/test_facturation_service_smoke.py)) qui catchent ce type de bug post-refactor.
- **Lignes draft incluses dans le calcul Accise** ([`electricore/core/pipelines/accise.py`](electricore/core/pipelines/accise.py)) — `agreger_consommations_mensuelles` agrégeait toutes les lignes énergie, y compris celles dont l'`account.move` est en draft (sans `invoice_date`). Conséquence : `mois_consommation = null` → ValueError "ligne(s) sans taux en vigueur" en sortie de `ajouter_taux_en_vigueur`. Fix : filtrer `invoice_date.is_not_null()` au début de l'agrégation — sémantique « assiette accise = factures validées ». Test unitaire dédié.

#### Tests

- Isolation des tests crypto ETL ([`tests/etl/test_crypto.py`](tests/etl/test_crypto.py)) — autouse fixture qui efface `AES__*` d'`os.environ` avant chaque test `load_key_chain`. Bug pré-existant exposé en important `electricore.api.main` au niveau test (lecture `.env` au chargement de l'API).

---

## [1.4.0] - 2026-06-02

### 🚀 Déployabilité — fondations VPS

Mise en place d'une stack Docker reproductible (`docker compose up -d`) avec ETL planifié et reverse-proxy TLS. La cible : un VPS unique qui exécute l'ETL à intervalle fixe, expose l'API + bot Telegram 24/7, et héberge optionnellement les fichiers Enedis collocés (pas de SFTP redondant). L'accès distant des notebooks via API HTTP est reporté à v1.5.

#### Ajouts

- **Image Docker** ([`deploy/docker/Dockerfile`](deploy/docker/Dockerfile)) — multi-stage avec `uv` au build, runtime minimal (`python:3.13-slim` + libxml2 + supercronic + duckdb CLI), utilisateur non-root, `tini` comme PID 1. Publiée sur `ghcr.io/energie-de-nantes/electricore` à chaque tag `vX.Y.Z`.
- **Stack docker-compose** ([`deploy/docker/docker-compose.yml`](deploy/docker/docker-compose.yml)) — services `api` (FastAPI + bot), `etl-scheduler` (supercronic), `caddy` (TLS automatique). Volumes nommés pour la base DuckDB et les sauvegardes.
- **ETL planifié** via supercronic — déclenche `POST /etl/run` toutes les nuits à 02:00. Les jobs scheduled apparaissent dans `/etl/jobs` aux côtés des runs manuels (bot, API), donc l'historique reste unifié.
- **Mode "fichiers collocés"** — pour le scénario où l'application tourne sur le même VPS qu'un dépôt Enedis. Aucune modification de code : `SFTP__URL=file:///var/enedis/` est nativement supporté par `dlt.sources.filesystem`. Les fichiers restent chiffrés AES sur disque, donc les clés AES restent obligatoires. Voir [`docs/deploiement.md`](docs/deploiement.md).
- **Sauvegardes DuckDB** ([`deploy/docker/backup_duckdb.sh`](deploy/docker/backup_duckdb.sh)) — `EXPORT DATABASE` + tar.gz quotidien, rétention 14 snapshots. Restauration documentée.
- **Endpoint `/health` enrichi** ([`electricore/api/main.py`](electricore/api/main.py)) — retourne désormais un payload structuré `{database: {accessible, last_write, tables}, bot: {running}, ...}` au lieu de juste `{status: "ok"}`. Toujours `200` (même base inaccessible), pour qu'ops puisse différencier un verrou ETL transitoire d'une panne réelle.
- **Retry-on-lock DuckDB** ([`electricore/api/services/duckdb_service.py`](electricore/api/services/duckdb_service.py)) — 3 tentatives × 1s pour absorber les fenêtres de checkpoint pendant que l'ETL écrit.
- **Documentation déploiement** ([`docs/deploiement.md`](docs/deploiement.md)) — guide complet : prérequis, installation initiale, modes SFTP vs collocé, rotation clés AES, sauvegarde/restauration, dépannage.

#### Modifications

- **Import package-style** ([`electricore/etl/pipeline_production.py:29`](electricore/etl/pipeline_production.py#L29)) — `from electricore.etl.sources.sftp_enedis import flux_enedis`. Permet d'invoquer le pipeline via `python -m electricore.etl.pipeline_production` depuis n'importe quel `cwd`. Service ETL côté API ajusté en conséquence.
- **Masquage URL généralisé** ([`electricore/etl/sources/sftp_enedis.py`](electricore/etl/sources/sftp_enedis.py)) — `mask_password_in_url()` gère désormais `file://` (no-op) en plus de `sftp://`.

#### Suppressions (breaking)

- **Unit systemd `electricore.service`** supprimée (ainsi que les fichiers `.example` annexes). Les utilisateurs sur déploiement bare-metal doivent rester sur v1.3.x ou migrer vers Docker. La doc Docker couvre tous les cas d'usage de l'ancienne unit.

#### Reportés à v1.5

- Transport HTTP pour `DuckDBQuery` builders — permettra aux notebooks marimo locaux de lire les données depuis l'API distante (sans nécessiter le fichier DuckDB local).
- Extra `[viz]` optionnel — marimo, altair, vegafusion, vl-convert-python, plotly retirés des dépendances par défaut. L'image production passerait de ~1.2 GB à ~400 MB.
- Image multi-arch (arm64) — actuellement amd64 uniquement.
- Observabilité : logging JSON structuré, intégration Sentry.

---

## [1.3.5] - 2026-06-01

### 🛠️ Hygiène d'ingénierie

Première itération de mise à niveau des conventions OSS standards (CI, lint, type-checking, workflow release). Pas de changement métier — l'ensemble du comportement runtime reste identique à v1.3.4.

- **CI GitHub Actions** ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) : exécute pytest sur chaque PR (Python 3.12 + 3.13), ruff lint + format, mypy. `uv sync --locked` détecte les dérives entre `pyproject.toml` et `uv.lock`.
- **Ruff lint + format** activés sur `electricore/` et `tests/` (ruleset E/F/I/UP/B, line-length 120, notebooks/scripts exclus). 536 violations auto-corrigées. 1 vrai bug surfacé et corrigé : `__all__` exportait `expr_data_complete` (symbole inexistant) dans `electricore/core/pipelines/facturation.py`.
- **Mypy strict** (scope étroit sur la surface d'API publique : `electricore.core.loaders.*`, `electricore.core.writers.*`, `electricore.core.pipelines.orchestration`). 85 erreurs → 0. Implicit-Optional éliminé, `QueryConfig.validator` correctement typé `type[pa.DataFrameModel] | None`, paramètre `how` du join typé `Literal`.
- **Pre-commit hooks** ([`.pre-commit-config.yaml`](.pre-commit-config.yaml)) : ruff lint + format à chaque commit local.
- **Workflow release** ([`.github/workflows/release.yml`](.github/workflows/release.yml)) : déclenché sur push de tag `vX.Y.Z`, publie sur PyPI via trusted publishing (OIDC, sans token API) et crée la release GitHub avec artefacts attachés + notes auto-générées.

### 🐛 Détecté latent — non corrigé dans cette release

- `json_to_dict_from_bytes` appelé mais non défini dans `electricore/etl/transformers/parsers.py` (chemin générique JSON jamais exercé en production ; tous les flux JSON utilisent `_json_r64_transformer_base`). Annoté `# noqa: F821` + TODO ; à implémenter avant d'ajouter un nouveau type de flux JSON.

---

## [1.0.0] - 2025-10-06

### 🎉 Version majeure - Architecture moderne Polars/DuckDB

Refonte complète du projet avec migration de pandas vers Polars, nouvelle architecture modulaire, et ajout de fonctionnalités majeures. **BREAKING CHANGES** : non rétrocompatible avec v0.x.

### ✨ Ajouts majeurs

#### Architecture & Performance
- **Migration Polars 100%** : Remplacement complet de pandas par Polars pour performances optimales
- **DuckDB integration** : Query builder fluide pour interroger la base de données DLT
- **LazyFrame pipelines** : Évaluation différée pour optimisation mémoire et calcul
- **Architecture modulaire** : Séparation claire ETL / Core / API

#### Module ETL (nouveau)
- **Pipeline DLT** : Extraction automatisée depuis SFTP Enedis
- **Transformers modulaires** : Crypto (AES-256-CBC), Archive (ZIP/TAR), Parsers (XML/CSV/JSON)
- **Configuration flux** : `flux.yaml` centralisé pour tous les flux Enedis
- **Modes exécution** : Test (2 fichiers, ~3s), Flux unique (~6s), Production complète
- **Support flux** : C15, F12, F15, R15, R151, R64, R401
- **Documentation** : Guide complet ETL + outils diagnostic

#### Module Core (refonte)
- **Query Builders**
  - `DuckDBQuery` : API fluide pour C15, R151, R15, F15, R64, relevés harmonisés
  - `OdooQuery` : Navigation relations avec `.follow()`, enrichissement `.enrich()`
  - `ParquetLoader` : Chargement direct fichiers Parquet
- **Pipelines Polars**
  - `pipeline_perimetre` : Détection périmètre actif avec 8 expressions composables
  - `pipeline_abonnements` : Calcul périodes abonnement avec bornes temporelles
  - `pipeline_energie` : Consommations par cadran (HPH/HCH/HPB/HCB/HP/HC/Base)
  - `pipeline_turpe` : TURPE fixe + variable + CMDPS
- **Modèles Pandera** : Validation stricte avec décorateurs `@pa.check_types`
- **Writers** : OdooWriter pour synchronisation ERP

#### Module API (nouveau)
- **FastAPI** : API REST sécurisée avec authentification API key
- **Endpoints** : `/flux/c15`, `/flux/r151`, `/flux/r15`, `/flux/f15`, `/health`
- **Services DuckDB** : Requêtage optimisé avec filtres et pagination
- **Documentation** : OpenAPI interactive sur `/docs`

#### TURPE - Implémentation complète
- **TURPE Fixe C5** (BT ≤ 36 kVA)
  - Formule : `(b × P) + cg + cc`
  - Calcul annuel, journalier, période avec prorata
- **TURPE Fixe C4** (BT > 36 kVA) - **NOUVEAU**
  - 4 puissances souscrites : P₁ (HPH), P₂ (HCH), P₃ (HPB), P₄ (HCB)
  - Formule progressive : `b₁×P₁ + b₂×(P₂-P₁) + b₃×(P₃-P₂) + b₄×(P₄-P₃) + cg + cc`
  - Validation contrainte réglementaire : P₁ ≤ P₂ ≤ P₃ ≤ P₄
  - Économies jusqu'à 20% via modulation saisonnière
  - Détection automatique C4/C5, rétrocompatibilité totale
- **TURPE Variable** : Calcul par cadran horaire (HPH/HCH/HPB/HCB ou HP/HC ou Base)
- **CMDPS** : Pénalités dépassement puissance
- **Configuration** : `turpe_rules.csv` avec tarifs officiels CRE
- **Nomenclature CRE** : `b_*` (puissance €/kVA/an), `c_*` (énergie c€/kWh)

#### Documentation
- **README** : Guide complet architecture + quickstart + exemples
- **CLAUDE.md** : Instructions projet pour IA avec patterns établis
- **Guides modules** : ETL, API, Query Builders, Conventions dates
- **Tutoriels TURPE** : Documentation technique C4 + usage standalone
- **Tests** : 167 tests unitaires + intégration avec fixtures métier

### 🔧 Améliorations

#### Code Quality
- **Type hints** : Annotations complètes pour meilleure maintenabilité
- **Functional programming** : Expressions pures `Fn(Series) -> Series`
- **Immutabilité** : Query builders fluides sans mutation d'état
- **Tests exhaustifs** : 167 passing avec Hypothesis property-based testing
- **Fixtures métier** : Cas réels (déménagement, changements contrat, etc.)

#### Performance
- **LazyFrame optimization** : Évaluation différée par Polars
- **DuckDB analytics** : Requêtes SQL optimisées sur données DLT
- **Vectorisation** : Élimination boucles Python au profit opérations Polars
- **Mémoire** : Réduction empreinte grâce lazy evaluation

#### Developer Experience
- **Notebooks Marimo** : Exploration interactive des pipelines
- **Outils diagnostic** : Scripts analyse flux, états incrémentaux
- **Configuration centralisée** : `database.yaml`, `flux.yaml`
- **Logs structurés** : Meilleure traçabilité ETL

### 🗑️ Suppressions (Breaking Changes)

#### Modules supprimés
- `electricore.core.énergies` → remplacé par `electricore.core.pipelines.energie`
- `electricore.core.périmètre` → remplacé par `electricore.core.pipelines.perimetre`
- `electricore.core.relevés` → intégré dans `electricore.core.pipelines.energie`
- `electricore.core.pipeline_*` (ancien format) → nouveaux pipelines dans `pipelines/`
- `electricore.core.orchestration` → remplacé par `pipelines/orchestration`
- `electricore.core.taxes` → remplacé par `pipelines/turpe`
- `electricore.core.services` → remplacé par `loaders/` et `writers/`
- `electricore.inputs.flux` → parsers intégrés dans `etl/transformers/parsers.py`

#### Dépendances supprimées
- `pandas` → migration complète vers Polars
- `toolz` → remplacement par opérations Polars natives
- Autres dépendances obsolètes nettoyées

### 📦 Changements techniques

#### Dependencies
- **Ajoutées**
  - `polars >=1.0.0` (remplacement pandas)
  - `pandera[polars] >=0.24.0` (validation)
  - `duckdb >=1.3.2` (analytics)
  - `dlt[workspace] >=1.16.0` (ETL)
  - `fastapi >=0.116.1` (API)
  - `uvicorn >=0.35.0` (serveur ASGI)
  - `pycryptodome >=3.23.0` (décryptage flux)
- **Supprimées**
  - `pandas` (migration Polars)
  - `toolz` (refactor architecture)

#### Configuration
- `pyproject.toml` : Configuration Poetry moderne avec groupes optionnels
- Python : `>=3.12,<3.15` (support Python 3.12+)
- Build : `poetry-core>=2.0.0`

#### Structure projet
```
electricore/
├── etl/              # Nouveau module ETL (DLT)
├── core/
│   ├── loaders/      # Query builders (nouveau)
│   ├── pipelines/    # Pipelines Polars (refonte)
│   ├── models/       # Modèles Pandera (refonte)
│   └── writers/      # Writers Odoo (nouveau)
├── api/              # Nouveau module API (FastAPI)
└── config/           # Configuration centralisée (nouveau)
```

### 🔄 Migration depuis v0.x

⚠️ **ATTENTION** : Cette version introduit des breaking changes majeurs.

#### Actions requises

1. **Mise à jour imports**
   ```python
   # Avant (v0.x)
   from electricore.core.énergies import calculer_energies
   from electricore.core.périmètre import detecter_perimetre

   # Après (v1.0.0)
   from electricore.core.pipelines.energie import pipeline_energie
   from electricore.core.pipelines.perimetre import pipeline_perimetre
   from electricore.core.loaders import c15, r151, charger_releves
   ```

2. **Migration pandas → Polars**
   ```python
   # Les DataFrames sont maintenant des Polars DataFrames/LazyFrames
   import polars as pl

   # Avant : pandas df
   df = pd.read_csv("data.csv")

   # Après : Polars LazyFrame (recommandé)
   lf = pl.scan_csv("data.csv")
   ```

3. **Configuration**
   - Créer `config/database.yaml` pour DuckDB
   - Créer `etl/config/flux.yaml` pour ETL
   - Migrer anciens paramètres vers nouvelle structure

4. **Tests**
   - Adapter fixtures pour Polars
   - Utiliser nouveaux modèles Pandera
   - Tester avec nouveaux query builders

#### Guide complet
Consultez [CLAUDE.md](CLAUDE.md) pour architecture détaillée et patterns établis.

### 🐛 Corrections

- Fix validation Pandera pour colonnes optionnelles C4
- Fix gestion dates timezone Europe/Paris
- Fix convention dates R151 (+1 jour pour harmonisation)
- Fix calcul TURPE avec règles temporelles multiples
- Fix tests paramétrés avec colonnes C4 NULL

### 🔒 Sécurité

- API authentification par clé API
- Décryptage sécurisé flux cryptés (AES-256-CBC)
- Validation inputs stricte avec Pandera
- Configuration sensible via variables environnement

### 📊 Métriques

- **Tests** : 167 passing, 29 skipped
- **Couverture** : ~49%
- **Fichiers modifiés** : 141 fichiers
- **Lignes** : +24,734 / -9,297
- **Commits** : 287 commits depuis v0.2.7

### 🙏 Contributeurs

- Virgile - Architecture, développement, migration Polars
- Claude Code (Anthropic) - Assistance développement et refactoring

---

## [0.2.7] - 2024-08-13

Dernière version avant migration majeure v1.0.0.

### Ajouts
- Amélioration visualisation pipeline
- Mise à jour toolz vers 1.0.0

### Corrections
- Correction FutureWarning pandas pour fillna

---

## [0.1.0] - 2024-01-01

Version initiale du projet ElectriCore.

### Ajouts
- Pipeline de base pour traitement flux Enedis
- Calculs énergétiques avec pandas
- Intégration Odoo basique

---

[1.0.0]: https://github.com/Energie-De-Nantes/electricore/compare/v0.2.7...v1.0.0
[0.2.7]: https://github.com/Energie-De-Nantes/electricore/compare/v0.1.0...v0.2.7
[0.1.0]: https://github.com/Energie-De-Nantes/electricore/releases/tag/v0.1.0
