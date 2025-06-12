import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series, DataFrame
from typing import Annotated, Optional

from icecream import ic
class RelevéIndex(pa.DataFrameModel):
    """
    📌 Modèle Pandera pour les relevés d’index issus de différentes sources.

    Ce modèle permet de valider les relevés de compteurs avec leurs métadonnées.
    """
    # 
    id: Series[str] = pa.Field(nullable=False)

    # 📆 Date du relevé
    Date_Releve: Series[Annotated[pd.DatetimeTZDtype, "ns", "Europe/Paris"]] = pa.Field(nullable=False, coerce=True)
    ordre_index: Series[bool] = pa.Field(default=0)

    # 🔹 Identifiant du Point de Livraison (PDL)
    pdl: Series[str] = pa.Field(nullable=False)
    Ref_Situation_Contractuelle: Optional[Series[str]]

    # 🏢 Références Fournisseur & Distributeur
    Id_Calendrier_Fournisseur: Optional[Series[str]] = pa.Field(nullable=True)  # Peut être absent selon la source
    Id_Calendrier_Distributeur: Series[str] = pa.Field(nullable=True, isin=["DI000001", "DI000002", "DI000003"])
    Id_Affaire: Optional[Series[str]] = pa.Field(nullable=True)  # Référence de la demande associée

    # 
    Source: Series[str] = pa.Field(nullable=False, isin=["flux_R151", "flux_R15", "flux_C15"])

    # 📏 Unité de mesure
    Unité: Series[str] = pa.Field(nullable=False, eq="kWh")
    Précision: Series[str] = pa.Field(nullable=False, isin=["kWh", "Wh", "MWh"])

    # ⚡ Mesures
    HP: Series[float] = pa.Field(nullable=True, coerce=True)
    HC: Series[float] = pa.Field(nullable=True, coerce=True)
    HCH: Series[float] = pa.Field(nullable=True, coerce=True)
    HPH: Series[float] = pa.Field(nullable=True, coerce=True)
    HPB: Series[float] = pa.Field(nullable=True, coerce=True)
    HCB: Series[float] = pa.Field(nullable=True, coerce=True)
    BASE: Series[float] = pa.Field(nullable=True, coerce=True)

    @pa.dataframe_parser
    def parser_ordre_index(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        if "ordre_index" not in df.columns:
            df["ordre_index"] = False
        else:
            df["ordre_index"] = df["ordre_index"].fillna(False).astype(bool)
        return df
    
    @pa.dataframe_parser
    def parser_id(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        # Génère un id lisible à partir des colonnes clés
        df["id"] = df[["pdl", "Date_Releve", "Source", "ordre_index"]].astype(str).agg("-".join, axis=1)
        return df
    
    @pa.dataframe_parser
    def parser_colonnes(cls, df: pd.DataFrame) -> pd.DataFrame:
        ordre = list(cls.to_schema().columns.keys())
        df = df.copy()
        df = df[[col for col in ordre if col in df.columns]]
        return df
    
    @pa.dataframe_check
    def verifier_présence_mesures(cls, df: DataFrame) -> bool:
        """Vérifie que les mesures attendues sont présentes selon l'Id_Calendrier_Distributeur."""
        
        # DI000001: BASE non nul
        mask_d1 = df["Id_Calendrier_Distributeur"] == "DI000001"
        base_valide = df.loc[mask_d1, "BASE"].notnull().all()

        # DI000002: HP et HC non nul
        mask_d2 = df["Id_Calendrier_Distributeur"] == "DI000002"
        hp_hc_valide = df.loc[mask_d2, ["HP", "HC"]].notnull().all(axis=1).all()

        # DI000003: HPH, HCH, HPB, HCB non nul
        mask_d3 = df["Id_Calendrier_Distributeur"] == "DI000003"
        hph_hch_hpb_hcb_valide = df.loc[mask_d3, ["HPH", "HCH", "HPB", "HCB"]].notnull().all(axis=1).all()

        # Retourne True si toutes les conditions sont valides
        return base_valide and hp_hc_valide and hph_hch_hpb_hcb_valide
    
class RequêteRelevé(pa.DataFrameModel):
    """
    📌 Modèle Pandera pour les requêtes d'interrogation des relevés d'index.

    Assure que les requêtes sont bien formatées avant d'interroger le DataFrame `RelevéIndex`.
    """
    # 📆 Date du relevé demandée
    Date_Releve: Series[Annotated[pd.DatetimeTZDtype, "ns", "Europe/Paris"]] = pa.Field(nullable=False, coerce=True)

    # 🔹 Identifiant du Point de Livraison (PDL)
    pdl: Series[str] = pa.Field(nullable=False)

    # 