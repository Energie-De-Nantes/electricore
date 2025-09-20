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
    date_releve: DateTime = pa.Field(nullable=False, dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"})
    ordre_index: pl.Boolean = pa.Field(default=False)

    # 🔹 Identifiant du Point de Livraison (PDL)
    pdl: pl.Utf8 = pa.Field(nullable=False)
    ref_situation_contractuelle: Optional[pl.Utf8] = pa.Field(nullable=True)
    formule_tarifaire_acheminement: Optional[pl.Utf8] = pa.Field(nullable=True)

    # 🏢 Références Fournisseur & Distributeur
    id_calendrier_fournisseur: Optional[pl.Utf8] = pa.Field(nullable=True)
    id_calendrier_distributeur: pl.Utf8 = pa.Field(nullable=True, isin=["DI000001", "DI000002", "DI000003"])
    id_affaire: Optional[pl.Utf8] = pa.Field(nullable=True)

    # Source des données
    source: pl.Utf8 = pa.Field(nullable=False, isin=["flux_R151", "flux_R15", "flux_C15", "FACTURATION"])

    # 📏 Unité de mesure
    unite: pl.Utf8 = pa.Field(nullable=False, isin=["kWh", "Wh", "MWh"])
    precision: pl.Utf8 = pa.Field(nullable=False, isin=["kWh", "Wh", "MWh"])

    # ⚡ Mesures
    hp: Optional[pl.Float64] = pa.Field(nullable=True)
    hc: Optional[pl.Float64] = pa.Field(nullable=True)
    hch: Optional[pl.Float64] = pa.Field(nullable=True)
    hph: Optional[pl.Float64] = pa.Field(nullable=True)
    hpb: Optional[pl.Float64] = pa.Field(nullable=True)
    hcb: Optional[pl.Float64] = pa.Field(nullable=True)
    base: Optional[pl.Float64] = pa.Field(nullable=True)

    @pa.dataframe_check
    def verifier_presence_mesures(cls, data) -> pl.LazyFrame:
        """
        Vérifie que les mesures attendues sont présentes selon l'Id_Calendrier_Distributeur.
        Utilise les expressions Polars natives pour la validation.
        """
        df_lazy = data.lazyframe
        
        # Créer des conditions pour chaque type de calendrier
        conditions = []
        
        # DI000001: base doit être non-null
        cond_d1 = (
            pl.when(pl.col("id_calendrier_distributeur") == "DI000001")
            .then(pl.col("base").is_not_null())
            .otherwise(pl.lit(True))
        )
        conditions.append(cond_d1)

        # DI000002: hp et hc doivent être non-null
        cond_d2 = (
            pl.when(pl.col("id_calendrier_distributeur") == "DI000002")
            .then(pl.col("hp").is_not_null() & pl.col("hc").is_not_null())
            .otherwise(pl.lit(True))
        )
        conditions.append(cond_d2)

        # DI000003: hph, hch, hpb, hcb doivent être non-null
        cond_d3 = (
            pl.when(pl.col("id_calendrier_distributeur") == "DI000003")
            .then(
                pl.col("hph").is_not_null() &
                pl.col("hch").is_not_null() &
                pl.col("hpb").is_not_null() &
                pl.col("hcb").is_not_null()
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
    date_releve: DateTime = pa.Field(nullable=False, dtype_kwargs={"time_unit": "ns", "time_zone": "Europe/Paris"})

    # 🔹 Identifiant du Point de Livraison (PDL)
    pdl: pl.Utf8 = pa.Field(nullable=False)