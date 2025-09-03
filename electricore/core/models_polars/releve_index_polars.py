import polars as pl
import pandera.polars as pa
from pandera.typing.polars import DataFrame
from pandera.engines.polars_engine import DateTime
from typing import Optional, Annotated

class RelevéIndexPolars(pa.DataFrameModel):
    """
    📌 Modèle Pandera pour les relevés d'index issus de différentes sources - Version Polars.

    Ce modèle permet de valider les relevés de compteurs avec leurs métadonnées
    en utilisant Polars pour des performances optimales.
    """
    
    # 📆 Date du relevé - Utilisation du type DateTime Polars avec timezone
    Date_Releve: DateTime = pa.Field(nullable=False, dtype_kwargs={"time_unit": "ns", "time_zone": "Europe/Paris"})
    ordre_index: pl.Boolean = pa.Field(default=False)

    # 🔹 Identifiant du Point de Livraison (PDL)
    pdl: pl.Utf8 = pa.Field(nullable=False)
    Ref_Situation_Contractuelle: Optional[pl.Utf8] = pa.Field(nullable=True)
    Formule_Tarifaire_Acheminement: Optional[pl.Utf8] = pa.Field(nullable=True)

    # 🏢 Références Fournisseur & Distributeur
    Id_Calendrier_Fournisseur: Optional[pl.Utf8] = pa.Field(nullable=True)
    Id_Calendrier_Distributeur: pl.Utf8 = pa.Field(nullable=True, isin=["DI000001", "DI000002", "DI000003"])
    Id_Affaire: Optional[pl.Utf8] = pa.Field(nullable=True)

    # Source des données
    Source: pl.Utf8 = pa.Field(nullable=False, isin=["flux_R151", "flux_R15", "flux_C15", "FACTURATION"])

    # 📏 Unité de mesure
    Unité: pl.Utf8 = pa.Field(nullable=False, eq="kWh")
    Précision: pl.Utf8 = pa.Field(nullable=False, isin=["kWh", "Wh", "MWh"])

    # ⚡ Mesures
    HP: Optional[pl.Float64] = pa.Field(nullable=True)
    HC: Optional[pl.Float64] = pa.Field(nullable=True)
    HCH: Optional[pl.Float64] = pa.Field(nullable=True)
    HPH: Optional[pl.Float64] = pa.Field(nullable=True)
    HPB: Optional[pl.Float64] = pa.Field(nullable=True)
    HCB: Optional[pl.Float64] = pa.Field(nullable=True)
    BASE: Optional[pl.Float64] = pa.Field(nullable=True)

    @pa.dataframe_check
    def verifier_presence_mesures(cls, data) -> pl.LazyFrame:
        """
        Vérifie que les mesures attendues sont présentes selon l'Id_Calendrier_Distributeur.
        Utilise les expressions Polars natives pour la validation.
        """
        df_lazy = data.lazyframe
        
        # Créer des conditions pour chaque type de calendrier
        conditions = []
        
        # DI000001: BASE doit être non-null
        cond_d1 = (
            pl.when(pl.col("Id_Calendrier_Distributeur") == "DI000001")
            .then(pl.col("BASE").is_not_null())
            .otherwise(pl.lit(True))
        )
        conditions.append(cond_d1)
        
        # DI000002: HP et HC doivent être non-null
        cond_d2 = (
            pl.when(pl.col("Id_Calendrier_Distributeur") == "DI000002")
            .then(pl.col("HP").is_not_null() & pl.col("HC").is_not_null())
            .otherwise(pl.lit(True))
        )
        conditions.append(cond_d2)
        
        # DI000003: HPH, HCH, HPB, HCB doivent être non-null
        cond_d3 = (
            pl.when(pl.col("Id_Calendrier_Distributeur") == "DI000003")
            .then(
                pl.col("HPH").is_not_null() & 
                pl.col("HCH").is_not_null() & 
                pl.col("HPB").is_not_null() & 
                pl.col("HCB").is_not_null()
            )
            .otherwise(pl.lit(True))
        )
        conditions.append(cond_d3)
        
        # Combiner toutes les conditions
        combined_condition = conditions[0]
        for cond in conditions[1:]:
            combined_condition = combined_condition & cond
        
        return df_lazy.select(combined_condition.alias("mesures_valides"))

    class Config:
        """Configuration du modèle."""
        strict = False  # Permet les colonnes supplémentaires durant la migration


class RequêteRelevéPolars(pa.DataFrameModel):
    """
    📌 Modèle Pandera pour les requêtes d'interrogation des relevés d'index - Version Polars.

    Assure que les requêtes sont bien formatées avant d'interroger le DataFrame `RelevéIndexPolars`.
    """
    # 📆 Date du relevé demandée
    Date_Releve: DateTime = pa.Field(nullable=False, dtype_kwargs={"time_unit": "ns", "time_zone": "Europe/Paris"})

    # 🔹 Identifiant du Point de Livraison (PDL)
    pdl: pl.Utf8 = pa.Field(nullable=False)