"""
Modèles Pandera Polars pour les agrégats intermédiaires.

Ce module définit les structures de données intermédiaires utilisées
dans les pipelines de transformation, notamment pour les agrégations
mensuelles avant la jointure finale.
"""

import polars as pl
import pandera.polars as pa
from pandera.engines.polars_engine import DateTime
from typing import Optional


class AbonnementMensuelPolars(pa.DataFrameModel):
    """
    Modèle pour les abonnements agrégés par mois.

    Résultat de l'agrégation des périodes d'abonnement avec puissance
    moyenne pondérée par le nombre de jours.
    """

    # Clés d'agrégation
    ref_situation_contractuelle: pl.Utf8 = pa.Field(nullable=False)
    pdl: pl.Utf8 = pa.Field(nullable=False)
    mois_annee: pl.Utf8 = pa.Field(nullable=False)  # ex: "mars 2025"

    # Bornes temporelles du mois
    debut: DateTime = pa.Field(
        nullable=False,
        dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"}
    )
    fin: DateTime = pa.Field(
        nullable=False,
        dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"}
    )

    # Paramètres tarifaires agrégés
    puissance_moyenne: pl.Float64 = pa.Field(nullable=False, ge=0.0)
    formule_tarifaire_acheminement: pl.Utf8 = pa.Field(nullable=False)
    nb_jours: pl.Int32 = pa.Field(nullable=False, ge=1)

    # Montants TURPE fixe
    turpe_fixe: Optional[pl.Float64] = pa.Field(nullable=True, ge=0.0)

    # Métadonnées d'agrégation
    nb_sous_periodes_abo: pl.Int32 = pa.Field(nullable=False, ge=1)
    has_changement_abo: pl.Boolean = pa.Field(nullable=False)
    coverage_abo: pl.Float64 = pa.Field(nullable=False, ge=0.0, le=1.0)

    # Mémo optionnel pour lisibilité
    memo_puissance: Optional[pl.Utf8] = pa.Field(nullable=True)

    class Config:
        strict = False
        coerce = True


class EnergieMensuelPolars(pa.DataFrameModel):
    """
    Modèle pour les énergies agrégées par mois.

    Résultat de l'agrégation des périodes d'énergie avec sommes simples
    des cadrans horaires.
    """

    # Clés d'agrégation
    ref_situation_contractuelle: pl.Utf8 = pa.Field(nullable=False)
    pdl: pl.Utf8 = pa.Field(nullable=False)
    mois_annee: pl.Utf8 = pa.Field(nullable=False)  # ex: "mars 2025"

    # Bornes temporelles du mois
    debut: DateTime = pa.Field(
        nullable=False,
        dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"}
    )
    fin: DateTime = pa.Field(
        nullable=False,
        dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"}
    )

    # Énergies par cadran (sommes mensuelles)
    base_energie: Optional[pl.Float64] = pa.Field(nullable=True, ge=0.0)
    hp_energie: Optional[pl.Float64] = pa.Field(nullable=True, ge=0.0)
    hc_energie: Optional[pl.Float64] = pa.Field(nullable=True, ge=0.0)

    # Montants TURPE variable
    turpe_variable: Optional[pl.Float64] = pa.Field(nullable=True, ge=0.0)

    # Métadonnées d'agrégation
    nb_sous_periodes_energie: pl.Int32 = pa.Field(nullable=False, ge=0)
    has_changement_energie: pl.Boolean = pa.Field(nullable=False)
    coverage_energie: pl.Float64 = pa.Field(nullable=False, ge=0.0, le=1.0)

    # Qualité des données
    data_complete: pl.Boolean = pa.Field(nullable=False)

    class Config:
        strict = False
        coerce = True