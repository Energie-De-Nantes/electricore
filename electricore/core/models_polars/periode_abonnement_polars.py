"""
Modèle Pandera Polars pour les périodes d'abonnement.

Ce modèle définit la structure des données pour les périodes homogènes
de facturation de la part fixe (TURPE) en utilisant Polars pour des
performances optimisées.
"""

import polars as pl
import pandera.polars as pa
from pandera.typing.polars import DataFrame
from pandera.engines.polars_engine import DateTime
from typing import Optional


class PeriodeAbonnementPolars(pa.DataFrameModel):
    """
    📌 Modèle Pandera pour les périodes d'abonnement - Version Polars.

    Représente une période homogène de facturation de la part fixe (TURPE)
    pour une situation contractuelle donnée, optimisée pour Polars.
    """

    # Identifiants principaux
    ref_situation_contractuelle: pl.Utf8 = pa.Field(nullable=False)
    pdl: pl.Utf8 = pa.Field(nullable=False)

    # Métadonnées de période
    mois_annee: pl.Utf8 = pa.Field(nullable=False)  # ex: "mars 2025"
    debut_lisible: pl.Utf8 = pa.Field(nullable=False)  # ex: "1 mars 2025"
    fin_lisible: pl.Utf8 = pa.Field(nullable=False)    # ex: "31 mars 2025" ou "en cours"

    # Paramètres tarifaires
    formule_tarifaire_acheminement: pl.Utf8 = pa.Field(nullable=False)
    puissance_souscrite: pl.Float64 = pa.Field(nullable=False)

    # Durée de la période
    nb_jours: pl.Int32 = pa.Field(nullable=False)

    # Bornes temporelles précises (timezone Europe/Paris)
    debut: DateTime = pa.Field(
        nullable=False,
        dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"}
    )
    fin: Optional[DateTime] = pa.Field(
        nullable=True,
        dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"}
    )

    # Champs TURPE (ajoutés après calcul)
    turpe_fixe_journalier: Optional[pl.Float64] = pa.Field(nullable=True)
    turpe_fixe: Optional[pl.Float64] = pa.Field(nullable=True)

    class Config:
        """Configuration du modèle Pandera."""
        strict = True  # Validation stricte des types
        coerce = True  # Conversion automatique des types compatibles