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


# Définition du Modèle pour le DataFrame c15
class FluxC15(pa.DataFrameModel):
    # Timestamp
    Date_Evenement: Series[Annotated[pd.DatetimeTZDtype, "ns", "Europe/Paris"]] = pa.Field(nullable=False)

    # Couple d'identifiants
    pdl: Series[str] = pa.Field(nullable=False)
    Ref_Situation_Contractuelle: Series[str] = pa.Field(nullable=False)
    
    # Infos Contractuelles
    Segment_Clientele: Series[str] = pa.Field(nullable=False)
    Etat_Contractuel: Series[str] = pa.Field(nullable=False) # "EN SERVICE", "RESILIE", etc.
    Evenement_Declencheur: Series[str] = pa.Field(nullable=False)  # Ex: "MCT", "MES", "RES"
    Type_Evenement: Series[str] = pa.Field(nullable=False)
    Categorie: Series[str] = pa.Field(nullable=True)

    # Infos calculs tarifs
    Puissance_Souscrite: Series[float] = pa.Field(nullable=False, coerce=True)
    Formule_Tarifaire_Acheminement: Series[str] = pa.Field(nullable=False,)

    # Infos Compteur
    Type_Compteur: Series[str] = pa.Field(nullable=False)
    Num_Compteur: Series[str] = pa.Field(nullable=False)

    # Infos Demande (Optionnel)
    Ref_Demandeur: Series[str] = pa.Field(nullable=True)
    Id_Affaire: Series[str] = pa.Field(nullable=True)
    
    # 📏 Unité de mesure
    Unité: Series[str] = pa.Field(nullable=False, default='kWh')

    Avant_Date_Releve: Series[Annotated[pd.DatetimeTZDtype, "ns", "Europe/Paris"]] = pa.Field(nullable=True)
    Avant_Nature_Index: Series[str] = pa.Field(nullable=True)
    
    # On a parfois deux relevés, dans le cas notamment de changement de calendriers
    Avant_HP: Series[float] = pa.Field(nullable=True, coerce=True)
    Avant_HC: Series[float] = pa.Field(nullable=True, coerce=True)
    Avant_HCH: Series[float] = pa.Field(nullable=True, coerce=True)
    Avant_HPH: Series[float] = pa.Field(nullable=True, coerce=True)
    Avant_HPB: Series[float] = pa.Field(nullable=True, coerce=True)
    Avant_HCB: Series[float] = pa.Field(nullable=True, coerce=True)
    Avant_BASE: Series[float] = pa.Field(nullable=True, coerce=True)

    Après_Date_Releve: Series[Annotated[pd.DatetimeTZDtype, "ns", "Europe/Paris"]] = pa.Field(nullable=True)
    Après_Nature_Index: Series[str] = pa.Field(nullable=True)
    

    Après_HP: Series[float] = pa.Field(nullable=True, coerce=True)
    Après_HC: Series[float] = pa.Field(nullable=True, coerce=True)
    Après_HCH: Series[float] = pa.Field(nullable=True, coerce=True)
    Après_HPH: Series[float] = pa.Field(nullable=True, coerce=True)
    Après_HPB: Series[float] = pa.Field(nullable=True, coerce=True)
    Après_HCB: Series[float] = pa.Field(nullable=True, coerce=True)
    Après_BASE: Series[float] = pa.Field(nullable=True, coerce=True)
    
    @pa.dataframe_parser
    def add_unite(cls, df: DataFrame) -> DataFrame:
        if "Unité" not in df.columns:
            df["Unité"] = "kWh"
        return df

    # 📆 Parser qui converti les Dates en CET "Europe/Paris"
    @pa.dataframe_parser
    def parser_dates(cls, df: DataFrame) -> DataFrame:
        df["Avant_Date_Releve"] = (
            pd.to_datetime(df["Avant_Date_Releve"], utc=True, format="ISO8601")
            .dt.tz_convert("Europe/Paris")
        )
        df["Après_Date_Releve"] = (
            pd.to_datetime(df["Après_Date_Releve"], utc=True, format="ISO8601")
            .dt.tz_convert("Europe/Paris")
        )
        df["Date_Evenement"] = (
            pd.to_datetime(df["Date_Evenement"], utc=True, format="ISO8601")
            .dt.tz_convert("Europe/Paris")
        )
        return df
    
    # ⚡ Parser qui converti unités en kWh tout en gardant la précision
    @pa.dataframe_parser
    def parser_unites(cls, df: DataFrame) -> DataFrame:
        classes_temporelles = ["HP", "HC", "BASE", "HCH", "HPH", "HPB", "HCB"]
        cols_index = (
            ['Avant_'+c for c in classes_temporelles]
            + ['Après_'+c for c in classes_temporelles]
        )
        
        # Sauvegarde unité originale
        df["Précision"] = df["Unité"]
        
        # Conversion des unités
        mask_wh = df["Unité"] == "Wh"
        df.loc[mask_wh, cols_index] /= 1000

        mask_mwh = df["Unité"] == "MWh"
        df.loc[mask_mwh, cols_index] *= 1000

        df["Unité"] = "kWh"
        return df