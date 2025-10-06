import polars as pl
import pandera.polars as pa
from pandera.typing.polars import DataFrame
from pandera.engines.polars_engine import DateTime
from typing import Optional


class PeriodeEnergie(pa.DataFrameModel):
    """
    Représente une période homogène de calcul d'énergie entre deux relevés successifs - Version Polars.

    Cette classe modélise les périodes de consommation/production d'énergie électrique
    avec les références d'index, les sources de données et les indicateurs de qualité,
    optimisée pour les performances Polars.
    """
    # Identifiants
    pdl: pl.Utf8 = pa.Field(nullable=False)
    ref_situation_contractuelle: Optional[pl.Utf8] = pa.Field(nullable=True)

    # Période
    debut: DateTime = pa.Field(nullable=False, dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"})
    fin: DateTime = pa.Field(nullable=False, dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"})
    nb_jours: Optional[pl.Int32] = pa.Field(nullable=True, ge=0)

    # Dates lisibles (optionnelles)
    debut_lisible: Optional[pl.Utf8] = pa.Field(nullable=True)
    fin_lisible: Optional[pl.Utf8] = pa.Field(nullable=True)
    mois_annee: Optional[pl.Utf8] = pa.Field(nullable=True)

    # Sources des relevés
    source_avant: pl.Utf8 = pa.Field(nullable=False)
    source_apres: pl.Utf8 = pa.Field(nullable=False)

    # Flags de qualité des données
    data_complete: pl.Boolean = pa.Field(nullable=False)

    # Énergies par cadran (optionnelles selon le type de compteur)
    base_energie: Optional[pl.Float64] = pa.Field(nullable=True)
    hp_energie: Optional[pl.Float64] = pa.Field(nullable=True)
    hc_energie: Optional[pl.Float64] = pa.Field(nullable=True)
    hph_energie: Optional[pl.Float64] = pa.Field(nullable=True)
    hpb_energie: Optional[pl.Float64] = pa.Field(nullable=True)
    hch_energie: Optional[pl.Float64] = pa.Field(nullable=True)
    hcb_energie: Optional[pl.Float64] = pa.Field(nullable=True)

    # Informations contractuelles pour calcul TURPE (colonnes optionnelles)
    formule_tarifaire_acheminement: Optional[pl.Utf8] = pa.Field(nullable=True)

    # Calculs TURPE (colonnes optionnelles)
    turpe_variable: Optional[pl.Float64] = pa.Field(nullable=True)

    # Composante dépassement C4 (colonne optionnelle en entrée, fournie par l'appelant)
    # Durée totale de dépassement de puissance souscrite sur tous les cadrans (heures)
    depassement_puissance_h: Optional[pl.Float64] = pa.Field(nullable=True, ge=0.0)

    # Flags pour tracer les relevés manquants (colonnes optionnelles)
    releve_manquant_debut: Optional[pl.Boolean] = pa.Field(nullable=True)
    releve_manquant_fin: Optional[pl.Boolean] = pa.Field(nullable=True)

    # Métadonnées de qualité et complétude
    nb_sous_periodes: Optional[pl.Int32] = pa.Field(nullable=True, ge=1)
    coverage_energie: Optional[pl.Float64] = pa.Field(nullable=True, ge=0.0, le=1.0)
    has_changement: Optional[pl.Boolean] = pa.Field(nullable=True)

    @pa.dataframe_check
    def verifier_coherence_periode(cls, data) -> pl.LazyFrame:
        """
        Vérifie que les périodes sont cohérentes (début < fin).
        """
        df_lazy = data.lazyframe

        # Condition : début doit être antérieur à fin (ou fin null pour période en cours)
        condition = (
            pl.col("fin").is_null() |
            (pl.col("debut") < pl.col("fin"))
        )

        return df_lazy.select(condition.alias("periode_coherente"))

    @pa.dataframe_check
    def verifier_nb_jours_coherent(cls, data) -> pl.LazyFrame:
        """
        Vérifie que nb_jours correspond à la différence debut-fin.
        """
        df_lazy = data.lazyframe

        # Calculer nb_jours attendu
        nb_jours_calcule = (
            pl.col("fin").dt.date() - pl.col("debut").dt.date()
        ).dt.total_days().cast(pl.Int32)

        # Condition : nb_jours doit correspondre au calcul (ou être null)
        condition = (
            pl.col("nb_jours").is_null() |
            pl.col("fin").is_null() |
            (pl.col("nb_jours") == nb_jours_calcule)
        )

        return df_lazy.select(condition.alias("nb_jours_coherent"))

    class Config:
        """Configuration pour permettre des colonnes supplémentaires."""
        strict = False