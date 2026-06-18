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
    # Pas de `ge=0` sur énergie/montant (#341) : `pipeline_accise` somme les lignes de
    # factures Odoo par (PDL, mois), et un mois peut être net-négatif quand un avoir /
    # une régularisation y tombe sans nominal en face. La déclaration accise est
    # *trimestrielle* et nette positif ; le grain mensuel peut légitimement être < 0.
    # Band-aid assumé imparfait : le netting brut sous-compte les avoirs « conso réelle
    # retournée » et ne distingue pas régul/nominal — la correctness relève du modèle de
    # facturé maîtrisé / périodes typées (cf #225, #282). `taux_accise_eur_mwh` garde
    # `ge=0` : un taux reste positif.
    energie_kwh: pl.Float64 = pa.Field(nullable=False)
    energie_mwh: pl.Float64 = pa.Field(nullable=False)
    taux_accise_eur_mwh: pl.Float64 = pa.Field(nullable=False, ge=0.0)
    accise_eur: pl.Float64 = pa.Field(nullable=False)

    class Config:
        strict = False
        coerce = True
        unique = ["pdl", "mois_annee"]
