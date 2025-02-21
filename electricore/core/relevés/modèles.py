import pandas as pd
import pandera as pa
from pandera.typing import Series, DataFrame
from typing import Annotated

class RelevéIndex(pa.DataFrameModel):
    """
    📌 Modèle Pandera pour les relevés d’index issus de différentes sources.

    Ce modèle permet de valider les relevés de compteurs avec leurs métadonnées.
    """
    # 📆 Date du relevé
    Date_Releve: Series[Annotated[pd.DatetimeTZDtype, "ns", "Europe/Paris"]] = pa.Field(nullable=False, coerce=True)

    # 🔹 Identifiant du Point de Livraison (PDL)
    pdl: Series[str] = pa.Field(nullable=False)

    # 🏢 Références Fournisseur & Distributeur
    Id_Calendrier_Fournisseur: Series[str] = pa.Field(nullable=True)  # Peut être absent selon la source
    Id_Calendrier_Distributeur: Series[str] = pa.Field(nullable=True, isin=["DI000001", "DI000002", "DI000003"])
    Id_Affaire: Series[str] = pa.Field(nullable=True)  # Référence de la demande associée

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