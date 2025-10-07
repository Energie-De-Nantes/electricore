import polars as pl
import pandera.polars as pa
from pandera.typing.polars import DataFrame
from pandera.engines.polars_engine import DateTime
from typing import Optional, Annotated

class HistoriquePérimètre(pa.DataFrameModel):
    """
    📌 Modèle Pandera pour l'historique des événements contractuels - Version Polars.
    
    Contient toutes les modifications de périmètre au fil du temps.
    Adapté pour fonctionner avec Polars pour des performances optimales.
    """

    # Timestamp principal
    date_evenement: DateTime = pa.Field(nullable=False, dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"})

    # Couple d'identifiants principaux
    pdl: pl.Utf8 = pa.Field(nullable=False)
    ref_situation_contractuelle: pl.Utf8 = pa.Field(nullable=False)

    # Informations Contractuelles
    segment_clientele: pl.Utf8 = pa.Field(nullable=False)
    etat_contractuel: pl.Utf8 = pa.Field(nullable=False)  # "EN SERVICE", "RESILIE", etc.
    evenement_declencheur: pl.Utf8 = pa.Field(nullable=False)  # Ex: "MCT", "MES", "RES"
    type_evenement: pl.Utf8 = pa.Field(nullable=False)
    categorie: Optional[pl.Utf8] = pa.Field(nullable=True)

    # Informations pour calculs tarifs
    puissance_souscrite_kva: pl.Float64 = pa.Field(nullable=False)
    formule_tarifaire_acheminement: pl.Utf8 = pa.Field(nullable=False)

    # Informations Compteur
    type_compteur: pl.Utf8 = pa.Field(nullable=False)
    num_compteur: pl.Utf8 = pa.Field(nullable=False)

    # Informations Demande (Optionnelles)
    ref_demandeur: Optional[pl.Utf8] = pa.Field(nullable=True)
    id_affaire: Optional[pl.Utf8] = pa.Field(nullable=True)

    # Colonnes supplémentaires fréquemment présentes dans les exports
    source: Optional[pl.Utf8] = pa.Field(nullable=True)
    marque: Optional[pl.Utf8] = pa.Field(nullable=True)
    unite: Optional[pl.Utf8] = pa.Field(nullable=True)
    precision: Optional[pl.Utf8] = pa.Field(nullable=True)
    num_depannage: Optional[pl.Utf8] = pa.Field(nullable=True)
    date_derniere_modification_fta: Optional[pl.Utf8] = pa.Field(nullable=True)
    
    # Colonnes de relevés "Avant" (index de compteurs)
    avant_date_releve: Optional[DateTime] = pa.Field(nullable=True, dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"})
    avant_nature_index: Optional[pl.Utf8] = pa.Field(nullable=True)
    avant_id_calendrier_fournisseur: Optional[pl.Utf8] = pa.Field(nullable=True)
    avant_id_calendrier_distributeur: Optional[pl.Utf8] = pa.Field(nullable=True)
    avant_index_hp_kwh: Optional[pl.Float64] = pa.Field(nullable=True)
    avant_index_hc_kwh: Optional[pl.Float64] = pa.Field(nullable=True)
    avant_index_hch_kwh: Optional[pl.Float64] = pa.Field(nullable=True)
    avant_index_hph_kwh: Optional[pl.Float64] = pa.Field(nullable=True)
    avant_index_hpb_kwh: Optional[pl.Float64] = pa.Field(nullable=True)
    avant_index_hcb_kwh: Optional[pl.Float64] = pa.Field(nullable=True)
    avant_index_base_kwh: Optional[pl.Float64] = pa.Field(nullable=True)

    # Colonnes de relevés "Après" (index de compteurs)
    apres_date_releve: Optional[DateTime] = pa.Field(nullable=True, dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"})
    apres_nature_index: Optional[pl.Utf8] = pa.Field(nullable=True)
    apres_id_calendrier_fournisseur: Optional[pl.Utf8] = pa.Field(nullable=True)
    apres_id_calendrier_distributeur: Optional[pl.Utf8] = pa.Field(nullable=True)
    apres_index_hp_kwh: Optional[pl.Float64] = pa.Field(nullable=True)
    apres_index_hc_kwh: Optional[pl.Float64] = pa.Field(nullable=True)
    apres_index_hch_kwh: Optional[pl.Float64] = pa.Field(nullable=True)
    apres_index_hph_kwh: Optional[pl.Float64] = pa.Field(nullable=True)
    apres_index_hpb_kwh: Optional[pl.Float64] = pa.Field(nullable=True)
    apres_index_hcb_kwh: Optional[pl.Float64] = pa.Field(nullable=True)
    apres_index_base_kwh: Optional[pl.Float64] = pa.Field(nullable=True)

    @pa.dataframe_check
    def verifier_coherence_dates(cls, data) -> pl.LazyFrame:
        """
        Vérifie la cohérence des dates dans l'historique.
        Les dates de relevés avant/après doivent être cohérentes avec la date d'événement.
        """
        df_lazy = data.lazyframe
        
        # Vérifier que les dates de relevés "Avant" sont <= date_evenement (quand définies)
        condition_avant = (
            pl.when(pl.col("avant_date_releve").is_not_null())
            .then(pl.col("avant_date_releve") <= pl.col("date_evenement"))
            .otherwise(pl.lit(True))
        )

        # Vérifier que les dates de relevés "Après" sont >= date_evenement (quand définies)
        condition_apres = (
            pl.when(pl.col("apres_date_releve").is_not_null())
            .then(pl.col("apres_date_releve") >= pl.col("date_evenement"))
            .otherwise(pl.lit(True))
        )
        
        # Combiner les conditions
        coherence_dates = condition_avant & condition_apres
        
        return df_lazy.select(coherence_dates.alias("dates_coherentes"))

    @pa.dataframe_check
    def verifier_presence_mesures_releves(cls, data) -> pl.LazyFrame:
        """
        Vérifie que si un calendrier distributeur est défini, 
        les mesures correspondantes sont présentes.
        """
        df_lazy = data.lazyframe
        
        # Pour les relevés "Avant"
        cond_avant_d1 = (
            pl.when(pl.col("avant_id_calendrier_distributeur") == "DI000001")
            .then(pl.col("avant_index_base_kwh").is_not_null())
            .otherwise(pl.lit(True))
        )

        cond_avant_d2 = (
            pl.when(pl.col("avant_id_calendrier_distributeur") == "DI000002")
            .then(pl.col("avant_index_hp_kwh").is_not_null() & pl.col("avant_index_hc_kwh").is_not_null())
            .otherwise(pl.lit(True))
        )

        cond_avant_d3 = (
            pl.when(pl.col("avant_id_calendrier_distributeur") == "DI000003")
            .then(
                pl.col("avant_index_hph_kwh").is_not_null() &
                pl.col("avant_index_hch_kwh").is_not_null() &
                pl.col("avant_index_hpb_kwh").is_not_null() &
                pl.col("avant_index_hcb_kwh").is_not_null()
            )
            .otherwise(pl.lit(True))
        )

        # Pour les relevés "Après" (même logique)
        cond_apres_d1 = (
            pl.when(pl.col("apres_id_calendrier_distributeur") == "DI000001")
            .then(pl.col("apres_index_base_kwh").is_not_null())
            .otherwise(pl.lit(True))
        )

        cond_apres_d2 = (
            pl.when(pl.col("apres_id_calendrier_distributeur") == "DI000002")
            .then(pl.col("apres_index_hp_kwh").is_not_null() & pl.col("apres_index_hc_kwh").is_not_null())
            .otherwise(pl.lit(True))
        )

        cond_apres_d3 = (
            pl.when(pl.col("apres_id_calendrier_distributeur") == "DI000003")
            .then(
                pl.col("apres_index_hph_kwh").is_not_null() &
                pl.col("apres_index_hch_kwh").is_not_null() &
                pl.col("apres_index_hpb_kwh").is_not_null() &
                pl.col("apres_index_hcb_kwh").is_not_null()
            )
            .otherwise(pl.lit(True))
        )
        
        # Combiner toutes les conditions
        mesures_valides = (
            cond_avant_d1 & cond_avant_d2 & cond_avant_d3 &
            cond_apres_d1 & cond_apres_d2 & cond_apres_d3
        )
        
        return df_lazy.select(mesures_valides.alias("mesures_releves_valides"))

    class Config:
        """Configuration du modèle."""
        strict = False  # Permet les colonnes supplémentaires durant la migration