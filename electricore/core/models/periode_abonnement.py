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


class PeriodeAbonnement(pa.DataFrameModel):
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

    # Puissance souscrite C5 (BT ≤ 36 kVA) - une seule puissance
    puissance_souscrite: pl.Float64 = pa.Field(nullable=False)

    # Puissances souscrites C4 (BT > 36 kVA) - 4 puissances par cadran temporel
    # Contrainte réglementaire CRE : P₁ ≤ P₂ ≤ P₃ ≤ P₄
    puissance_souscrite_hph: Optional[pl.Float64] = pa.Field(nullable=True, ge=0.0)  # P₁ - HPH
    puissance_souscrite_hch: Optional[pl.Float64] = pa.Field(nullable=True, ge=0.0)  # P₂ - HCH
    puissance_souscrite_hpb: Optional[pl.Float64] = pa.Field(nullable=True, ge=0.0)  # P₃ - HPB
    puissance_souscrite_hcb: Optional[pl.Float64] = pa.Field(nullable=True, ge=0.0)  # P₄ - HCB

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

    # Métadonnées de qualité et complétude
    data_complete: Optional[pl.Boolean] = pa.Field(nullable=True)
    nb_sous_periodes: Optional[pl.Int32] = pa.Field(nullable=True, ge=1)
    coverage_abo: Optional[pl.Float64] = pa.Field(nullable=True, ge=0.0, le=1.0)
    has_changement: Optional[pl.Boolean] = pa.Field(nullable=True)

    class Config:
        """Configuration du modèle Pandera."""
        strict = False  # Permet colonnes supplémentaires pour flexibilité
        coerce = True   # Conversion automatique des types compatibles