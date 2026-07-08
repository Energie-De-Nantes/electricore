# electricore-client

Client Python **léger** vers l'API facturiste [electricore](https://github.com/Energie-De-Nantes/electricore).

Dépendances de base : **httpx + pydantic uniquement** — pas de polars, duckdb ni
fastapi. Pensé pour être consommé par `souscriptions_odoo` (Odoo 19) et tout
intégrateur qui n'a pas besoin de tirer le moteur entier.

Lecture seule sur electricore (« Odoo tire d'electricore », ADR-0027/0012).

## Installation

```bash
pip install electricore-client            # base : httpx + pydantic
pip install "electricore-client[arrow]"   # + client Arrow (DataFrames polars)
```

## Usage

```python
from electricore_client import ElectricoreClient

client = ElectricoreClient(url="https://electricore.example", api_key="…")

# Méta-périodes mensuelles (flux JSONL typé, sans pagination)
with client.meta_periodes(mois="2026-05-01") as stream:
    print(stream.contract_version)        # version de contrat (en-tête)
    for periode in stream:                # itère des PeriodeMeta typés
        ...

# Chronologie d'un point ou d'un contrat (union discriminée)
with client.chronologie(pdl="12345678901234") as stream:
    lignes = stream.collect()

# Calculateur TURPE variable (POST RPC, pas un stream)
resultats = client.turpe_variable([...])  # résultats indexés par id opaque

# Prestations F15 à refacturer (pull-tout, dédup par `reference` côté consommateur)
with client.prestations() as stream:
    for prestation in stream:             # itère des PrestationF15 typés
        ...

# Résolution RSC : lot d'id_Affaire (X12) → ref_situation_contractuelle (POST RPC)
resultats = client.resoudre_rsc(["id_affaire_1", "id_affaire_2"])
par_id = {r.id_affaire: r for r in resultats}   # chaque résultat porte ref ou error
```

Le client Arrow historique (`flux/releves/facturation/accise/cta` → `pl.DataFrame`)
vit dans `electricore_client.arrow`, derrière l'extra `[arrow]`.

## Conception

Voir [ADR-0043](https://github.com/Energie-De-Nantes/electricore/blob/main/docs/adr/0043-electricore-client-paquet-separe.md).
