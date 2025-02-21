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
    Id_Calendrier_Distributeur: Series[str] = pa.Field(nullable=True)  # Peut être absent selon la source
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