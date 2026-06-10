"""Schéma Pandera Polars pour l'Accise TICFE au grain mensuel (PDL × mois).

Sortie de `pipeline_accise` (cf. ADR-0019, issues #110, #116) : une ligne par
(PDL, mois de consommation), avec énergie consommée, taux en vigueur,
et montant de l'accise. La colonne `trimestre` permet de filtrer/agréger
au grain trimestriel pour les déclarations.

Le grain est garanti par `Config.unique` — vérifié à la matérialisation
(`validate()` sur DataFrame), le décorateur du pipeline ne validant que
colonnes et dtypes sur LazyFrame.
"""

import pandera.polars as pa
import polars as pl


class AcciseMensuel(pa.DataFrameModel):
    """Accise TICFE au grain mensuel : une ligne par (PDL, mois de consommation)."""

    pdl: pl.Utf8 = pa.Field(nullable=False)
    mois_annee: pl.Utf8 = pa.Field(nullable=False, str_matches=r"^\d{4}-\d{2}$")  # ex: "2025-03"
    trimestre: pl.Utf8 = pa.Field(nullable=False)  # ex: "2025-T1"
    order_name: pl.Utf8 = pa.Field(nullable=False)
    energie_kwh: pl.Float64 = pa.Field(nullable=False, ge=0.0)
    energie_mwh: pl.Float64 = pa.Field(nullable=False, ge=0.0)
    taux_accise_eur_mwh: pl.Float64 = pa.Field(nullable=False, ge=0.0)
    accise_eur: pl.Float64 = pa.Field(nullable=False, ge=0.0)

    class Config:
        strict = False
        coerce = True
        unique = ["pdl", "mois_annee"]
