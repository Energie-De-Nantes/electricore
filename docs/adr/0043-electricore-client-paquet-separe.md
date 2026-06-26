# electricore-client : paquet séparé, JSONL streamé, modèles single-source, extra `[arrow]`

## Contexte

`souscriptions_odoo` (Odoo 19, Python 3.12, AGPL-3) doit consommer les endpoints
**facturiste** d'electricore (méta-périodes, chronologie, calcul TURPE variable) sans tirer
le moteur entier. Or les dépendances de base du moteur sont **toutes lourdes** (`polars`,
`duckdb`, `fastapi`, `uvicorn`, `pyarrow`, `numpy`, `python-telegram-bot`…) : un *extra* ne
donne jamais un install léger (un extra **s'ajoute** aux deps de base), et on ne peut pas
« vider » les deps de base — le moteur en a légitimement besoin. Il faut donc une
**distribution séparée**.

Le client Arrow historique (`electricore/client/`, méthodes `flux/releves/facturation/accise/
cta` → `pl.DataFrame`) importait polars **en tête de module** et vivait sous le top-level
`electricore` : tant qu'il restait là, toute distribution livrant `electricore/__init__.py`
revendiquait le package entier — deux wheels ne peuvent pas tous deux livrer ce fichier sans
se marcher dessus.

Un document de conception antérieur (`docs/electricore-client-design.md`) proposait une
réponse en **JSON enveloppé + pagination** (`{mois|grain, contract_version, filters,
pagination, data}`) et des modèles redéfinis côté client. Cette conception est **supersédée**
ici : aucun consommateur de production ne dépendait de l'enveloppe, et la pagination +
duplication de modèles ajoutait de la surface sans bénéfice.

## Décision

### 1. Un paquet distribué séparément, top-level `electricore_client`

Sous-projet à `packages/electricore-client/` (src-layout), `pyproject.toml` distinct,
**deps de base httpx + pydantic uniquement**, `requires-python = ">=3.10"`, `py.typed`.
Top-level **distinct** (`electricore_client`) : pas de conflit de `__init__.py`, pas de
conversion namespace.

- **Workspace uv** à la racine (`[tool.uv.workspace] members = ["packages/*"]` +
  `[tool.uv.sources] electricore-client = { workspace = true }`) : le moteur résout le client
  localement et **en dépend** comme **source unique** des modèles de contrat.
- Le wheel du moteur est épinglé (`[tool.hatch.build.targets.wheel] packages = ["electricore"]`)
  pour qu'il **cesse** d'auto-embarquer `packages/`. Deux wheels **disjoints** : le wheel
  moteur n'a aucun `electricore_client/`, le wheel client aucun `electricore/`.

### 2. Substrat de transport partagé (`_BaseClient`)

URL de base, en-tête `X-API-Key`, construction/timeout `httpx.Client`, conversion d'erreur
HTTP **503 (verrou d'ingestion) → `IngestionEnCours`**, et une **garde de version de contrat
asymétrique** sur l'en-tête `X-Contract-Version` : **warn** si le serveur est en avance
(le client tolère l'additif via `model_config = ConfigDict(extra="ignore")`), **raise** s'il
est en retard (champs attendus absents).

### 3. Les deux lectures en **JSONL streamé** (pas d'enveloppe, pas de pagination)

`meta-periodes` et `chronologie` répondent en `application/x-ndjson` (une ligne = un objet),
construit **par modèle** côté serveur (chaque ligne est validée à l'émission). Pas de
pagination : le serveur streame **toutes** les lignes. Les métadonnées (`contract_version`,
`mois` résolu / `grain`) remontent dans les **en-têtes** de réponse. Côté client, un
**context-manager** (`JsonlStream`) itère des lignes typées paresseusement, applique la garde
de version à l'ouverture, libère la connexion à la sortie du `with` (même mi-consommé), et
offre `.collect()`.

- `meta-periodes` → `PeriodeMeta` (contrat **v3**), relevés imbriqués `releves_utilises:
  list[ObjetReleve]` (ADR-0038), `source_hash` requis.
- `chronologie` → `LigneChronologie`, **union discriminée** pydantic v2 sur `type_ligne`
  (`LigneEvenement | LigneReleve | LignePeriodeEnergie`, contrat **v1**). `pdl` **XOR** `rsc`
  validé côté client (`ValueError`) avant toute requête.

### 4. `turpe_variable` en **POST RPC** (pas un stream)

Petit calcul stateless, batch POST (request/response JSON) : un round-trip pour tout le lot.
Résultats typés (`ResultatTurpeVariable`) **appariés à l'entrée par l'`id` opaque** ré-émis
par le calculateur (jamais positionnel). Modèles (`LigneTurpeVariable`, request, résultat)
**single-sourcés** dans le client ; le router les importe (plus de définition inline).

### 5. Typage des nombres (ADR-0034)

`index_*_kwh` sont des **entiers** (kWh entier au boundary dbt) ; `energie_*_kwh` et `*_eur`
sont des **flottants**. Les registres/énergies ne sont émis que **non-nuls** (jamais de cadran
synthétisé) → tous `Optional`.

### 6. Le client Arrow derrière un extra `[arrow]`

`ElectricoreArrowClient` (méthodes Arrow → `pl.DataFrame`) vit dans le sous-module
`electricore_client.arrow`, **jamais importé au top-level** ; polars y est importé
**paresseusement** (message d'install clair si l'extra manque). L'extra `[arrow]` tire polars.
La base reste **polars-free** — invariant prouvé par un test de pureté (sous-processus :
`import electricore_client` ne charge ni polars, ni duckdb, ni fastapi), côté paquet **et**
côté moteur.

### 7. CI/CD

- `release-client.yml` : déclenché par les tags `client-v*` (+ rc/dev PEP 440, **distincts**
  des `v*` du moteur), build `uv build packages/electricore-client`, vérif que le nom
  d'artefact matche le tag, publication PyPI via **Trusted Publishing / OIDC**
  (`pypa/gh-action-pypi-publish`, `skip-existing`). Pas de Docker.
- Job `test-client` dans `ci.yml` : installe **uniquement** le client (httpx + pydantic, **ni
  moteur ni polars**) et lance ses tests — la garantie polars-free devient un **gate de CI**.

## Conséquences

- Un intégrateur léger (`souscriptions_odoo`) installe `electricore-client` (httpx + pydantic)
  sans jamais tirer polars/duckdb/fastapi.
- Modèles **single-source** : router moteur et client consomment la même définition → pas de
  dérive de contrat. Le moteur dépend du client (couplage assumé, sens unique).
- **Rupture** : les deux lectures n'ont plus d'enveloppe paginée. Aucun consommateur de prod
  ne s'y appuyait (seuls nos tests d'intégration, réécrits). La pagination disparaît au profit
  du streaming.
- **Étape manuelle unique** : configurer côté PyPI un *pending Trusted Publisher* pour le
  projet `electricore-client` → repo `Energie-De-Nantes/electricore`, workflow
  `release-client.yml` (avant le premier tag `client-v*`).

## Statut

Accepté — implémenté par #406 (squelette + transport + workspace + pureté), #407
(meta-periodes JSONL), #408 (chronologie JSONL union discriminée), #409 (turpe_variable POST
RPC + déménagement modèles), #410 (extra `[arrow]`), #411 (CI/CD + ce document).

Supersède la conception enveloppée de `docs/electricore-client-design.md` (réécrit en
conséquence). S'appuie sur ADR-0034 (kWh entiers), ADR-0038 (relevés imbriqués), ADR-0027
(« Odoo tire »), ADR-0030 (calculateur TURPE variable), ADR-0039/0041 (chronologie),
ADR-0016 (core ERP-agnostique).
