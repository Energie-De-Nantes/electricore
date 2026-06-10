"""
Modèle Pandera Polars pour les méta-périodes de facturation.

Ce modèle définit la structure des données pour les méta-périodes mensuelles
agrégées de facturation, combinant abonnements et énergies en utilisant Polars
pour des performances optimisées.
"""

import pandera.polars as pa
import polars as pl
from pandera.engines.polars_engine import DateTime


class PeriodeMeta(pa.DataFrameModel):
    """
    📌 Modèle Pandera pour les méta-périodes de facturation - Version Polars.

    Représente une méta-période mensuelle agrégée combinant les données
    d'abonnement et d'énergie pour une facturation simplifiée, optimisée pour Polars.

    L'agrégation utilise :
    - Puissance moyenne pondérée par nb_jours (mathématiquement équivalente)
    - Somme simple pour les énergies et montants TURPE
    - Métadonnées pour traçabilité des changements et complétude
    """

    # Identifiants principaux
    ref_situation_contractuelle: pl.Utf8 = pa.Field(nullable=False)
    pdl: pl.Utf8 = pa.Field(nullable=False)
    mois_annee: pl.Utf8 = pa.Field(nullable=False, str_matches=r"^\d{4}-\d{2}$")  # ex: "2025-03"

    # Bornes temporelles de la méta-période (timezone Europe/Paris)
    debut: DateTime = pa.Field(nullable=False, dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"})
    fin: DateTime = pa.Field(nullable=False, dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"})

    # Paramètres tarifaires agrégés
    puissance_moyenne_kva: pl.Float64 = pa.Field(nullable=False, ge=0.0)
    formule_tarifaire_acheminement: pl.Utf8 = pa.Field(nullable=False)
    nb_jours: pl.Int32 = pa.Field(nullable=False, ge=1)

    # Énergies consommées par cadran en kWh (optionnelles selon le type de compteur)
    energie_base_kwh: pl.Float64 | None = pa.Field(nullable=True, ge=0.0)
    energie_hp_kwh: pl.Float64 | None = pa.Field(nullable=True, ge=0.0)
    energie_hc_kwh: pl.Float64 | None = pa.Field(nullable=True, ge=0.0)

    # Montants TURPE en euros
    turpe_fixe_eur: pl.Float64 | None = pa.Field(nullable=True, ge=0.0)
    turpe_variable_eur: pl.Float64 | None = pa.Field(nullable=True, ge=0.0)

    # Métadonnées de traçabilité des sous-périodes
    nb_sous_periodes_abo: pl.Int32 = pa.Field(nullable=False, ge=1)
    nb_sous_periodes_energie: pl.Int32 = pa.Field(nullable=False, ge=0)
    has_changement: pl.Boolean = pa.Field(nullable=False)

    # 🆕 Métadonnées de couverture temporelle pour tracer l'incomplétude
    coverage_abo: pl.Float64 = pa.Field(nullable=False, ge=0.0, le=1.0)  # 0.0 à 1.0
    coverage_energie: pl.Float64 = pa.Field(nullable=False, ge=0.0, le=1.0)  # 0.0 à 1.0
    data_complete: pl.Boolean = pa.Field(nullable=False)  # True si coverage_abo=1.0 ET coverage_energie=1.0

    # Métadonnées optionnelles pour lisibilité
    debut_lisible: pl.Utf8 | None = pa.Field(nullable=True)
    fin_lisible: pl.Utf8 | None = pa.Field(nullable=True)

    # Mémo des changements de puissance (optionnel, pratique pour facturation)
    memo_puissance: pl.Utf8 | None = pa.Field(nullable=True)

    @pa.dataframe_check
    def verifier_coherence_periode(cls, data) -> pl.LazyFrame:
        """
        Vérifie que les méta-périodes sont cohérentes (début < fin).
        """
        df_lazy = data.lazyframe

        condition = pl.col("debut") < pl.col("fin")
        return df_lazy.select(condition.alias("periode_coherente"))

    @pa.dataframe_check
    def verifier_nb_jours_coherent(cls, data) -> pl.LazyFrame:
        """
        Vérifie que nb_jours correspond à la différence debut-fin.
        """
        df_lazy = data.lazyframe

        # Calculer nb_jours attendu
        nb_jours_calcule = (pl.col("fin").dt.date() - pl.col("debut").dt.date()).dt.total_days().cast(pl.Int32)

        condition = pl.col("nb_jours") == nb_jours_calcule
        return df_lazy.select(condition.alias("nb_jours_coherent"))

    @pa.dataframe_check
    def verifier_data_complete_coherent(cls, data) -> pl.LazyFrame:
        """
        Vérifie que data_complete est cohérent avec les coverage.
        data_complete doit être True ssi coverage_abo=1.0 ET coverage_energie=1.0
        """
        df_lazy = data.lazyframe

        condition = pl.col("data_complete") == ((pl.col("coverage_abo") == 1.0) & (pl.col("coverage_energie") == 1.0))
        return df_lazy.select(condition.alias("data_complete_coherent"))

    @pa.dataframe_check
    def verifier_has_changement_coherent(cls, data) -> pl.LazyFrame:
        """
        Vérifie que has_changement est cohérent avec le nombre de sous-périodes.
        has_changement doit être True ssi nb_sous_periodes_abo > 1 OU nb_sous_periodes_energie > 1
        """
        df_lazy = data.lazyframe

        condition = pl.col("has_changement") == (
            (pl.col("nb_sous_periodes_abo") > 1) | (pl.col("nb_sous_periodes_energie") > 1)
        )
        return df_lazy.select(condition.alias("has_changement_coherent"))

    class Config:
        """Configuration du modèle Pandera."""

        strict = False  # Permet colonnes supplémentaires pour compatibilité
        coerce = True  # Conversion automatique des types compatibles
