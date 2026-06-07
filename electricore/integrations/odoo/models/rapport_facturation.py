"""Schémas Pandera pour le livrable `rapport_facturation` (issue #64).

Spécificité vs `rapport_accise` / `rapport_cta` :
- Les onglets `lignes` et `changements_puissance` partagent la **même** shape
  (le second est un filtre du premier). On réutilise donc le schéma existant
  `electricore.core.models.lignes_facture_rapprochees.LignesFactureRapprochees`
  pour ces deux frames — pas de duplication ici.
- Seul l'onglet `resume` introduit une shape nouvelle : une ligne de totaux
  pour le mois (nb PDL, lignes à facturer, lignes à supprimer).
"""

import pandera.polars as pa
import polars as pl


class RapportFacturationResume(pa.DataFrameModel):
    """Totaux mensuels (= une ligne par mois cible)."""

    mois: pl.Utf8 = pa.Field(nullable=False)  # "YYYY-MM-DD"
    nb_pdl: pl.Int64 = pa.Field(nullable=False, ge=0)
    total_a_facturer: pl.Int64 = pa.Field(nullable=False, ge=0)
    total_a_supprimer: pl.Int64 = pa.Field(nullable=False, ge=0)
