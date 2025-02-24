import numpy as np
import pandas as pd
import pandera as pa
from pandera.typing import DataFrame, Series
from typing import Annotated

class FluxR151(pa.DataFrameModel):
    # 📆 Date du relevé
    Date_Releve: Series[Annotated[pd.DatetimeTZDtype, "ns", "Europe/Paris"]] = pa.Field(nullable=False)
    
    # 🔹 Identifiant du Point de Livraison (PDL), aussi appelé Point Réf. Mesures (PRM)
    pdl: Series[str] = pa.Field(nullable=False)

    # 🏢 Références Fournisseur & Distributeur
    Id_Calendrier_Fournisseur: Series[str] = pa.Field(nullable=True)
    Id_Calendrier_Distributeur: Series[str] = pa.Field(nullable=True) 
    Id_Affaire: Series[str] = pa.Field(nullable=True)

    # 📏 Unité de mesure
    Unité: Series[str] = pa.Field(nullable=False, isin=["kWh", "Wh", "MWh"])
    Précision: Series[str] = pa.Field(nullable=False)

    # ⚡ Mesures
    HP: Series[float] = pa.Field(nullable=True, coerce=True)
    HC: Series[float] = pa.Field(nullable=True, coerce=True)
    HCH: Series[float] = pa.Field(nullable=True, coerce=True)
    HPH: Series[float] = pa.Field(nullable=True, coerce=True)
    HPB: Series[float] = pa.Field(nullable=True, coerce=True)
    HCB: Series[float] = pa.Field(nullable=True, coerce=True)
    BASE: Series[float] = pa.Field(nullable=True, coerce=True)

    # 📆 Parser qui converti les Dates en CET "Europe/Paris"
    @pa.dataframe_parser
    def parser_dates(cls, df: DataFrame) -> DataFrame:
        df["Date_Releve"] = (
            pd.to_datetime(df["Date_Releve"], errors="coerce")
            .dt.tz_localize("Europe/Paris")
        )
        return df

    # ⚡ Parser qui converti unités en kWh tout en gardant la précision
    @pa.dataframe_parser
    def parser_unites(cls, df: DataFrame) -> DataFrame:
        cols_index = ["HP", "HC", "BASE", "HCH", "HPH", "HPB", "HCB"]
        
        # Sauvegarde unité originale
        df["Précision"] = df["Unité"]
        
        # Conversion des unités
        mask_wh = df["Unité"] == "Wh"
        df.loc[mask_wh, cols_index] /= 1000

        mask_mwh = df["Unité"] == "MWh"
        df.loc[mask_mwh, cols_index] *= 1000

        df["Unité"] = "kWh"
        return df
