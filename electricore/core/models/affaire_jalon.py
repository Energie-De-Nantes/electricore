"""Modèle Pandera du seam `affaires_ouvertes` ← `flux_affaires` (#295, ADR-0035).

Garde-fou de la **frontière dbt → pipeline-cœur** où `affaires_ouvertes`
(`core/pipelines/affaires.py`, suivi opérationnel des affaires SGE, #276) consomme la
forme de `flux_affaires` (grain = un *jalon* par affaire). Avant #295 ce seam était la
dernière *lacune de registre* identifiée par la discovery #147 (ADR-0035, Conséquences) :
le loader `affaires()` portait `validator=None`.

Propriété par fait (ADR-0035) :
- **Type physique + forme** (1 ligne/jalon, dédup `(affaire_id, jalon_num)`, casts) → dbt
  (`models/flux/flux_affaires.sql`).
- **Dtype Polars + nullabilité-règle** → ce contrat, posé au seam.
- Le **loader** ne connaît aucun type → `SELECT *` ; la parité dbt↔Pandera est *prouvée*
  (jamais copiée) par `test_dbt_affaires_respecte_le_contrat_pandera` via la table
  SQL↔Polars de `parite_typage.py`.

Nullabilité (axe par couche, hors test de parité — ADR-0035 §5) : seules les colonnes
que `flux_affaires` garantit (`not_null` dbt) ET dont le rollup dépend sont non-null —
`affaire_id` (clé de regroupement), `jalon_num` (clé de tri / max), `jalon_date_heure`
(ancienneté), `origine`, `affaire_etat`. `statut` est **nullable** (#296 : une affaire
fraîchement initiée arrive sans statut → `affaires_ouvertes` la traite comme « en attente
Enedis »). Les libellés / `pdl` / `segment` sont nullable (le rollup `.last()` les tolère).
"""

import pandera.polars as pa
import polars as pl
from pandera.engines.polars_engine import DateTime


class AffaireJalon(pa.DataFrameModel):
    """📌 Un jalon d'affaire SGE consommé par `affaires_ouvertes` (grain = un jalon)."""

    # 🔹 Identité & regroupement (non-null : clés du rollup)
    affaire_id: pl.Utf8 = pa.Field(nullable=False)
    origine: pl.Utf8 = pa.Field(nullable=False, isin=["initiee", "recue"])
    jalon_num: pl.Int32 = pa.Field(nullable=False)
    jalon_date_heure: DateTime = pa.Field(nullable=False, dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"})
    affaire_etat: pl.Utf8 = pa.Field(nullable=False)

    # 🔸 Cycle de vie : NULLABLE (#296) — `<statut>` vide pour une affaire fraîchement
    # initiée qu'Enedis n'a pas encore rangée ; `affaires_ouvertes` la garde (en attente).
    statut: pl.Utf8 | None = pa.Field(nullable=True)

    # 🏷️ Attributs descriptifs roulés par `.last()` (nullable : le rollup les tolère)
    prestation: pl.Utf8 | None = pa.Field(nullable=True)
    prestation_libelle: pl.Utf8 | None = pa.Field(nullable=True)
    pdl: pl.Utf8 | None = pa.Field(nullable=True)
    segment: pl.Utf8 | None = pa.Field(nullable=True)
    affaire_etat_libelle: pl.Utf8 | None = pa.Field(nullable=True)

    class Config:
        """Tolère les colonnes supplémentaires servies par le loader `SELECT *`
        (`affaire_jalon_id`, `affaire_date_effet`, `source`)."""

        strict = False
