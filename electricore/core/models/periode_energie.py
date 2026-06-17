import pandera.polars as pa
import polars as pl
from pandera.engines.polars_engine import DateTime


class PeriodeEnergie(pa.DataFrameModel):
    """
    Représente une période homogène de calcul d'énergie entre deux relevés successifs - Version Polars.

    Cette classe modélise les périodes de consommation/production d'énergie électrique
    avec les références d'index, les sources de données et les indicateurs de qualité,
    optimisée pour les performances Polars.
    """

    # Identifiants
    pdl: pl.Utf8 = pa.Field(nullable=False)
    ref_situation_contractuelle: pl.Utf8 | None = pa.Field(nullable=True)

    # Période
    debut: DateTime = pa.Field(nullable=False, dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"})
    fin: DateTime = pa.Field(nullable=False, dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"})
    nb_jours: pl.Int32 | None = pa.Field(nullable=True, ge=0)

    # Dates lisibles (optionnelles)
    debut_lisible: pl.Utf8 | None = pa.Field(nullable=True)
    fin_lisible: pl.Utf8 | None = pa.Field(nullable=True)
    mois_annee: pl.Utf8 | None = pa.Field(nullable=True, str_matches=r"^\d{4}-\d{2}$")

    # Sources des relevés
    source_avant: pl.Utf8 = pa.Field(nullable=False)
    source_apres: pl.Utf8 = pa.Field(nullable=False)

    # Verdicts de période jumeaux (axes orthogonaux, ADR-0036). Remplacent l'ancien
    # flag `data_complete` (retiré, ADR-0033 — un booléen ne distinguait pas réel/estimé).
    # Qualité (ADR-0033) : rollup pire-gagne de la nature d'index des deux bornes.
    qualite: pl.Utf8 | None = pa.Field(nullable=True, isin=["réelle", "estimée", "incalculable"])
    # Communication (ADR-0036) : communicante ssi les deux bornes sont à niveau ≥ 1.
    statut_communication: pl.Utf8 | None = pa.Field(nullable=True, isin=["communicante", "non_communicante"])

    # Énergies consommées par cadran en kWh (optionnelles selon le type de compteur)
    energie_base_kwh: pl.Float64 | None = pa.Field(nullable=True)
    energie_hp_kwh: pl.Float64 | None = pa.Field(nullable=True)
    energie_hc_kwh: pl.Float64 | None = pa.Field(nullable=True)
    energie_hph_kwh: pl.Float64 | None = pa.Field(nullable=True)
    energie_hpb_kwh: pl.Float64 | None = pa.Field(nullable=True)
    energie_hch_kwh: pl.Float64 | None = pa.Field(nullable=True)
    energie_hcb_kwh: pl.Float64 | None = pa.Field(nullable=True)

    # Informations contractuelles pour calcul TURPE (colonnes optionnelles)
    formule_tarifaire_acheminement: pl.Utf8 | None = pa.Field(nullable=True)

    # Calculs TURPE en euros (colonnes optionnelles)
    turpe_variable_eur: pl.Float64 | None = pa.Field(nullable=True)

    # Composante dépassement C4 (colonne optionnelle en entrée, fournie par l'appelant)
    # Durée totale de dépassement de puissance souscrite sur tous les cadrans (heures)
    duree_depassement_h: pl.Float64 | None = pa.Field(nullable=True, ge=0.0)

    # Flags pour tracer les relevés manquants (colonnes optionnelles)
    releve_manquant_debut: pl.Boolean | None = pa.Field(nullable=True)
    releve_manquant_fin: pl.Boolean | None = pa.Field(nullable=True)

    # Métadonnées d'agrégation
    nb_sous_periodes: pl.Int32 | None = pa.Field(nullable=True, ge=1)
    has_changement: pl.Boolean | None = pa.Field(nullable=True)

    @pa.dataframe_check
    def verifier_coherence_periode(cls, data) -> pl.LazyFrame:
        """
        Vérifie que les périodes sont cohérentes (début < fin).
        """
        df_lazy = data.lazyframe

        # Condition : début doit être antérieur à fin (ou fin null pour période en cours)
        condition = pl.col("fin").is_null() | (pl.col("debut") < pl.col("fin"))

        return df_lazy.select(condition.alias("periode_coherente"))

    @pa.dataframe_check
    def verifier_nb_jours_coherent(cls, data) -> pl.LazyFrame:
        """
        Vérifie que nb_jours correspond à la différence debut-fin.
        """
        df_lazy = data.lazyframe

        # Calculer nb_jours attendu
        nb_jours_calcule = (pl.col("fin").dt.date() - pl.col("debut").dt.date()).dt.total_days().cast(pl.Int32)

        # Condition : nb_jours doit correspondre au calcul (ou être null)
        condition = pl.col("nb_jours").is_null() | pl.col("fin").is_null() | (pl.col("nb_jours") == nb_jours_calcule)

        return df_lazy.select(condition.alias("nb_jours_coherent"))

    class Config:
        """Configuration pour permettre des colonnes supplémentaires."""

        strict = False
