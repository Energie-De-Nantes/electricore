# Conception — paquet `electricore-client` distribué séparément

> Client léger (httpx + pydantic) que `souscriptions_odoo` (Odoo 19, Python 3.12, AGPL-3)
> consomme pour les endpoints facturiste d'electricore, **sans** tirer polars/duckdb/fastapi.
> Lecture seule (ADR-0012). **La décision est consignée dans [ADR-0043](adr/0043-electricore-client-paquet-separe.md)** ; ce document en est la note de conception.

> ⚠️ Ce document a été **réécrit** (#411). La version antérieure décrivait une réponse en
> **JSON enveloppé + pagination** + des modèles redéfinis côté client : cette conception est
> **supersédée** par le **streaming JSONL** + métadonnées en en-têtes + modèles single-source
> décrits ci-dessous (cf. ADR-0043).

## 1. Layout : deux distributions disjointes

- Sous-projet `packages/electricore-client/` (src-layout), top-level **distinct**
  `electricore_client`, deps de base **httpx + pydantic uniquement**,
  `requires-python = ">=3.10"`, `py.typed`.
- **Workspace uv** à la racine (`[tool.uv.workspace] members = ["packages/*"]` +
  `[tool.uv.sources] electricore-client = { workspace = true }`) : le moteur résout le client
  localement et **en dépend** (source unique des modèles de contrat).
- Wheel du moteur épinglé (`[tool.hatch.build.targets.wheel] packages = ["electricore"]`) →
  il cesse d'auto-embarquer `packages/`. **Deux wheels disjoints** : le wheel moteur n'a
  aucun `electricore_client/`, le wheel client aucun `electricore/`.

## 2. Substrat de transport (`_BaseClient`)

URL de base + `X-API-Key` + timeout `httpx.Client`, **503 → `IngestionEnCours`**, et une
**garde de version asymétrique** sur `X-Contract-Version` : *warn* si serveur en avance
(`extra="ignore"` tolère l'additif), *raise* si serveur en retard.

## 3. Les deux lectures — JSONL streamé

`GET /facturation/meta-periodes` et `GET /facturation/chronologie` répondent en
`application/jsonl` (une ligne = un objet), **sans enveloppe ni pagination**. Métadonnées
(`contract_version`, `mois` / `grain`) dans les **en-têtes** (`X-Contract-Version`,
`X-Mois`, `X-Grain`). Chaque ligne est **validée par construction d'un modèle** côté serveur.

Côté client, un context-manager `JsonlStream` :

```python
from electricore_client import ElectricoreClient

client = ElectricoreClient(url="https://electricore.example", api_key="…")

with client.meta_periodes(mois="2026-05-01") as stream:
    stream.contract_version          # depuis l'en-tête, garde de version en __enter__
    for periode in stream:           # PeriodeMeta typés (relevés imbriqués, source_hash)
        ...

with client.chronologie(pdl="12345678901234") as stream:  # pdl XOR rsc (ValueError sinon)
    for ligne in stream:             # union discriminée sur type_ligne
        ...                          # LigneEvenement | LigneReleve | LignePeriodeEnergie
```

- `PeriodeMeta` (contrat **v3**) — `releves_utilises: list[ObjetReleve]` (ADR-0038),
  `source_hash` requis.
- `LigneChronologie` (contrat **v1**) — union discriminée pydantic v2 sur `type_ligne`.
- Le flux est paresseux et libère la connexion à la sortie du `with`, même mi-consommé ;
  `.collect()` matérialise tout.

## 4. `turpe_variable` — POST RPC

Calcul stateless, batch POST (request/response JSON, **pas un stream**). Résultats typés
appariés par l'`id` opaque ré-émis (jamais positionnel) :

```python
from electricore_client.models import LigneTurpeVariable

resultats = client.turpe_variable([
    LigneTurpeVariable(id="L1", formule_tarifaire_acheminement="BTINFCUST",
                       debut="2025-08-15T00:00:00+02:00", energie_base_kwh=1000.0),
])
par_id = {r.id: r.turpe_variable_eur for r in resultats}   # ou r.error (xor)
```

Modèles (`LigneTurpeVariable`, `TurpeVariableRequest`, `ResultatTurpeVariable`)
**single-sourcés** dans `electricore_client.models` ; le router les importe.

## 5. Typage (ADR-0034)

`index_*_kwh` **entiers**, `energie_*_kwh` / `*_eur` **flottants**. Registres et énergies
émis **non-nuls** uniquement → tous `Optional`. `model_config = ConfigDict(extra="ignore")`
(additive-tolérant).

## 6. Client Arrow — extra `[arrow]`

`ElectricoreArrowClient` (méthodes `flux/releves/facturation/accise/cta` → `pl.DataFrame`)
vit dans `electricore_client.arrow`, **jamais importé au top-level** ; polars y est importé
**paresseusement** (message d'install clair si l'extra manque) :

```python
from electricore_client.arrow import ElectricoreArrowClient   # nécessite [arrow]
```

La base reste **polars-free** — prouvé par un test de pureté (sous-processus :
`import electricore_client` ne charge ni polars, ni duckdb, ni fastapi).

## 7. CI/CD

- `release-client.yml` — tags `client-v*` (distincts des `v*` du moteur), `uv build
  packages/electricore-client`, vérif du nom d'artefact, publication PyPI via **Trusted
  Publishing / OIDC**. Pas de Docker.
- Job `test-client` dans `ci.yml` — installe **uniquement** le client (httpx + pydantic, ni
  moteur ni polars), lance ses tests : la garantie polars-free est un **gate de CI**.
- **Étape manuelle unique** : configurer un *pending Trusted Publisher* PyPI pour
  `electricore-client` → repo `Energie-De-Nantes/electricore`, workflow `release-client.yml`.
