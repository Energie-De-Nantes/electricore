# Registre runtime en pydantic-settings : domaines indépendants, validation par point d'entrée

## Contexte

[ADR-0024](0024-trois-registres-de-savoir.md) nomme le registre runtime et lui assigne un
module canonique (`electricore/config/runtime.py`, issue #141). L'état de départ : trois
loaders de `.env` (parseur artisanal de `config/env.py`, préambule de `pipeline_dbt.py`,
convention dlt), des îlots de lecture par module (`APISettings` recopiant 11 `os.getenv`
à la main dans un `BaseModel` pydantic, `charger_config_odoo`, lectures directes dans
`crypto.py` et la source SFTP), aucun fail-fast — une var manquante explose au premier
usage, parfois en plein run. Le bug « `pipeline_dbt` ignorait `DUCKDB_PATH` en Docker »
venait de cette duplication.

[ADR-0018](0018-classes-justifiees-par-l-etat.md) fait des `@dataclass(frozen=True)` la
forme par défaut, mais exempte explicitement les `BaseModel` pydantic (« leur propre
machinerie de déclaration de schéma »). La revue d'architecture du 12/06/2026 a comparé
les deux voies (pydantic-settings vs dataclasses artisanales) sur l'état de l'art
(Prefect 3, écosystème FastAPI).

## Décision

### Domaines indépendants, pas de racine

`runtime.py` définit **un `BaseSettings` indépendant par domaine** — `sftp`, `aes`,
`duckdb`, `api`, `bot`, `odoo` — chacun mappé sur les **noms de vars existants** (aucun
renommage : `SFTP__URL`, `AES__CURRENT__KEY`, `DUCKDB_PATH`, `TELEGRAM_*`, `ODOO_*`),
avec `.env` ancré sur la racine du dépôt et la précédence env-système > `.env` (native
pydantic-settings). Pas d'objet racine : un domaine non demandé n'est jamais validé.

Accès par **accessors de module mis en cache** : `runtime.sftp()`, `runtime.bot()`…
Cas particulier : `runtime.odoo(env=None)` résout le sélecteur `ODOO_ENV` (`test`/`prod`)
et injecte le préfixe `ODOO_{ENV}_` à l'instanciation — seul l'environnement demandé est
validé.

### Fail-fast par point d'entrée

Un helper `valider(*accessors)` appelle chaque domaine, collecte les `ValidationError`,
et lève une unique `ConfigurationManquante` listant **toutes** les vars manquantes,
groupées par domaine. Chaque point d'entrée déclare ses domaines au boot :

| point d'entrée | domaines validés |
|---|---|
| API (lifespan) | `duckdb`, `api` |
| bot | `bot` (+ `odoo` si l'instance a un ERP — no-ERP servi, ADR-0022) |
| runner ETL | `sftp`, `aes`, `duckdb` |
| notebooks | rien d'imposé — chaque accessor valide à son premier appel |

### Conséquences structurantes sur l'existant

- **`API_BASE_URL` est supprimée.** Le bot tourne toujours dans le processus de l'API
  (aucun service bot dans le compose) ; son client httpx reçoit
  `httpx.ASGITransport(app=app)` via le paramètre `transport` existant (#174) — mêmes
  endpoints, même auth `X-API-Key`, zéro socket. Le domaine bot se réduit à `TELEGRAM_*`.
- **Façades conservées** : `charger_env()` (réimplémentée en python-dotenv, peuple
  `os.environ` — nécessaire à dlt et aux notebooks) ; `charger_config_odoo()` (2 lignes
  déléguant à `runtime.odoo()`, préserve 8 notebooks et l'API documentée).
- **Migration + suppression** : `chemin_base_duckdb()` → `runtime.duckdb().chemin`
  (tous les callers sont in-repo).
- **L'ETL consomme runtime** : `crypto.py` construit sa chaîne de clés depuis
  `runtime.aes()` (modèles imbriqués, `AES__CURRENT__KEY` se parse nativement avec le
  délimiteur `__`), la source SFTP lit `runtime.sftp()` ; le fallback `dlt.secrets`
  reste dans ces modules — c'est la façade dlt documentée par ADR-0024.

## Raison

1. **Le contrat par point d'entrée tombe naturellement des domaines indépendants.**
   Une racine unique aurait imposé `Optional` sur tous les domaines (pour ne pas casser
   notebooks et instance no-ERP), transformant chaque accès en None-check — le typage
   qu'on venait chercher s'évaporait.
2. **La convention `__` est déjà celle du dépôt.** `SFTP__URL`, `AES__CURRENT__KEY`
   suivent la convention dlt `SECTION__CLÉ`, qui est exactement le
   `env_nested_delimiter="__"` de pydantic-settings : aucun renommage, parsing natif.
3. **pydantic est déjà dans l'arbre** (FastAPI, dlt) ; seule `pydantic-settings`
   s'ajoute. La validation déclarative et l'agrégation d'erreurs sont gratuites — la
   version dataclasses re-codait ~150 lignes de coercition, parsing `.env` et collecte
   de manquantes, en gardant le parseur artisanal.
4. **Le fail-fast tue un footgun réel** : aujourd'hui, oublier `ODOO_ENV=prod` en prod
   fait défaut silencieusement sur la base *test*. Avec les domaines, ce cas cherche
   `ODOO_TEST_*` (non définies en prod) et lève `ConfigurationManquante` au boot.

### Alternatives écartées

- **Dataclasses frozen artisanales** (`depuis_env()`) — zéro dépendance et forme par
  défaut d'ADR-0018, mais ~150 lignes sur mesure pour un confort moindre, et ADR-0018
  exempte précisément les `BaseModel` de sa convention. Restera la voie de repli si
  pydantic-settings devait sortir de l'arbre.
- **Racine unique nested** (`RuntimeSettings` + sous-modèles) — un seul objet mais
  validation monolithique : incompatible avec no-ERP et notebooks sans `Optional`
  généralisé.
- **Conserver `API_BASE_URL`** — une var pour un réseau qui n'existe pas ; le seam
  transport (#174) permet de la faire renaître le jour où le bot devient un conteneur
  séparé (deux formes de déploiement = la var serait alors justifiée).

## Conséquences

- `api/config.py` (`APISettings`) est absorbé : champs API → domaine `api` (titre,
  version, description, `INSTANCE_SLUG`, clés), `TELEGRAM_*` → domaine `bot`,
  `ODOO_ENV` → accessor `odoo` ; la validation de clé (`secrets.compare_digest`) suit
  le domaine `api` ; `public_endpoints` (constante, pas de la config env) part en
  `api/security.py`.
- `.env.example`, `deploy/docker/.env.example` (suppression d'`API_BASE_URL`) et
  [docs/configuration.md](../configuration.md) sont mis à jour — ce dernier restructuré
  par registres d'ADR-0024 (sa section 1 devient la doc de `runtime.py`).
- Tests : chaque domaine s'instancie avec des valeurs d'init explicites, sans
  monkeypatch d'`os.environ` ; `ConfigurationManquante` se teste par domaine.
- L'entrée « Config partagée » de [core/CONTEXT.md](../../electricore/core/CONTEXT.md)
  est révisée : « stdlib-only » devient « aucune dépendance ERP ; pydantic-settings est
  la seule lib externe admise ici ».

## Limites à connaître

- **Deux chemins de lecture `.env` coexistent** : pydantic-settings (domaines) et
  python-dotenv (`charger_env`, pour dlt qui lit `os.environ`). Assumé — la façade dlt
  d'ADR-0024 l'exige ; les deux honorent la même précédence env-système > `.env`.
- **La mécanique `ODOO_ENV`/`ODOO_TEST_*` est conservée à l'identique** dans l'attente
  de #190 (stratégie de test des écritures Odoo) — le sélecteur est un outillage
  d'opérateur (répéter une injection avant de la jouer sur prod), pas une dimension du
  déploiement. Ne pas retoucher avant l'arbitrage de #190.
- **Si le bot devient un conteneur séparé**, le transport ASGI ne suffit plus : injecter
  un transport réseau et réintroduire une URL de base — le paramètre `transport` du
  client est le seam prévu pour ça.
