import polars as pl
import pandera.polars as pa
from pandera.typing.polars import DataFrame
from pandera.engines.polars_engine import DateTime
from typing import Optional, Annotated

class HistoriquePérimètrePolars(pa.DataFrameModel):
    """
    📌 Modèle Pandera pour l'historique des événements contractuels - Version Polars.
    
    Contient toutes les modifications de périmètre au fil du temps.
    Adapté pour fonctionner avec Polars pour des performances optimales.
    """

    # Timestamp principal
    Date_Evenement: DateTime = pa.Field(nullable=False, dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"})

    # Couple d'identifiants principaux
    pdl: pl.Utf8 = pa.Field(nullable=False)
    Ref_Situation_Contractuelle: pl.Utf8 = pa.Field(nullable=False)
    
    # Informations Contractuelles
    Segment_Clientele: pl.Utf8 = pa.Field(nullable=False)
    Etat_Contractuel: pl.Utf8 = pa.Field(nullable=False)  # "EN SERVICE", "RESILIE", etc.
    Evenement_Declencheur: pl.Utf8 = pa.Field(nullable=False)  # Ex: "MCT", "MES", "RES"
    Type_Evenement: pl.Utf8 = pa.Field(nullable=False)
    Categorie: Optional[pl.Utf8] = pa.Field(nullable=True)

    # Informations pour calculs tarifs
    Puissance_Souscrite: pl.Float64 = pa.Field(nullable=False)
    Formule_Tarifaire_Acheminement: pl.Utf8 = pa.Field(nullable=False)

    # Informations Compteur
    Type_Compteur: pl.Utf8 = pa.Field(nullable=False)
    Num_Compteur: pl.Utf8 = pa.Field(nullable=False)

    # Informations Demande (Optionnelles)
    Ref_Demandeur: Optional[pl.Utf8] = pa.Field(nullable=True)
    Id_Affaire: Optional[pl.Utf8] = pa.Field(nullable=True)
    
    # Colonnes supplémentaires fréquemment présentes dans les exports
    Source: Optional[pl.Utf8] = pa.Field(nullable=True)
    Marque: Optional[pl.Utf8] = pa.Field(nullable=True)
    Unité: Optional[pl.Utf8] = pa.Field(nullable=True)
    Précision: Optional[pl.Utf8] = pa.Field(nullable=True)
    Num_Depannage: Optional[pl.Utf8] = pa.Field(nullable=True)
    Date_Derniere_Modification_FTA: Optional[pl.Utf8] = pa.Field(nullable=True)
    
    # Colonnes de relevés "Avant"
    Avant_Date_Releve: Optional[DateTime] = pa.Field(nullable=True, dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"})
    Avant_Nature_Index: Optional[pl.Utf8] = pa.Field(nullable=True)
    Avant_Id_Calendrier_Fournisseur: Optional[pl.Utf8] = pa.Field(nullable=True)
    Avant_Id_Calendrier_Distributeur: Optional[pl.Utf8] = pa.Field(nullable=True)
    Avant_HP: Optional[pl.Float64] = pa.Field(nullable=True)
    Avant_HC: Optional[pl.Float64] = pa.Field(nullable=True)
    Avant_HCH: Optional[pl.Float64] = pa.Field(nullable=True)
    Avant_HPH: Optional[pl.Float64] = pa.Field(nullable=True)
    Avant_HPB: Optional[pl.Float64] = pa.Field(nullable=True)
    Avant_HCB: Optional[pl.Float64] = pa.Field(nullable=True)
    Avant_BASE: Optional[pl.Float64] = pa.Field(nullable=True)
    
    # Colonnes de relevés "Après"
    Après_Date_Releve: Optional[DateTime] = pa.Field(nullable=True, dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"})
    Après_Nature_Index: Optional[pl.Utf8] = pa.Field(nullable=True)
    Après_Id_Calendrier_Fournisseur: Optional[pl.Utf8] = pa.Field(nullable=True)
    Après_Id_Calendrier_Distributeur: Optional[pl.Utf8] = pa.Field(nullable=True)
    Après_HP: Optional[pl.Float64] = pa.Field(nullable=True)
    Après_HC: Optional[pl.Float64] = pa.Field(nullable=True)
    Après_HCH: Optional[pl.Float64] = pa.Field(nullable=True)
    Après_HPH: Optional[pl.Float64] = pa.Field(nullable=True)
    Après_HPB: Optional[pl.Float64] = pa.Field(nullable=True)
    Après_HCB: Optional[pl.Float64] = pa.Field(nullable=True)
    Après_BASE: Optional[pl.Float64] = pa.Field(nullable=True)

    @pa.dataframe_check
    def verifier_coherence_dates(cls, data) -> pl.LazyFrame:
        """
        Vérifie la cohérence des dates dans l'historique.
        Les dates de relevés avant/après doivent être cohérentes avec la date d'événement.
        """
        df_lazy = data.lazyframe
        
        # Vérifier que les dates de relevés "Avant" sont <= Date_Evenement (quand définies)
        condition_avant = (
            pl.when(pl.col("Avant_Date_Releve").is_not_null())
            .then(pl.col("Avant_Date_Releve") <= pl.col("Date_Evenement"))
            .otherwise(pl.lit(True))
        )
        
        # Vérifier que les dates de relevés "Après" sont >= Date_Evenement (quand définies)
        condition_apres = (
            pl.when(pl.col("Après_Date_Releve").is_not_null())
            .then(pl.col("Après_Date_Releve") >= pl.col("Date_Evenement"))
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
            pl.when(pl.col("Avant_Id_Calendrier_Distributeur") == "DI000001")
            .then(pl.col("Avant_BASE").is_not_null())
            .otherwise(pl.lit(True))
        )
        
        cond_avant_d2 = (
            pl.when(pl.col("Avant_Id_Calendrier_Distributeur") == "DI000002")
            .then(pl.col("Avant_HP").is_not_null() & pl.col("Avant_HC").is_not_null())
            .otherwise(pl.lit(True))
        )
        
        cond_avant_d3 = (
            pl.when(pl.col("Avant_Id_Calendrier_Distributeur") == "DI000003")
            .then(
                pl.col("Avant_HPH").is_not_null() & 
                pl.col("Avant_HCH").is_not_null() & 
                pl.col("Avant_HPB").is_not_null() & 
                pl.col("Avant_HCB").is_not_null()
            )
            .otherwise(pl.lit(True))
        )
        
        # Pour les relevés "Après" (même logique)
        cond_apres_d1 = (
            pl.when(pl.col("Après_Id_Calendrier_Distributeur") == "DI000001")
            .then(pl.col("Après_BASE").is_not_null())
            .otherwise(pl.lit(True))
        )
        
        cond_apres_d2 = (
            pl.when(pl.col("Après_Id_Calendrier_Distributeur") == "DI000002")
            .then(pl.col("Après_HP").is_not_null() & pl.col("Après_HC").is_not_null())
            .otherwise(pl.lit(True))
        )
        
        cond_apres_d3 = (
            pl.when(pl.col("Après_Id_Calendrier_Distributeur") == "DI000003")
            .then(
                pl.col("Après_HPH").is_not_null() & 
                pl.col("Après_HCH").is_not_null() & 
                pl.col("Après_HPB").is_not_null() & 
                pl.col("Après_HCB").is_not_null()
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