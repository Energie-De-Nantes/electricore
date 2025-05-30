import numpy as np
import pandas as pd
import pandera.pandas as pa
from pandera.typing import DataFrame, Series
from typing import Annotated, Optional


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
        df[cols_index] = df[cols_index].apply(pd.to_numeric, errors="coerce")
        
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
        df[cols_index] = df[cols_index].apply(pd.to_numeric, errors="coerce")
        # Sauvegarde unité originale
        df["Précision"] = df["Unité"]
        
        # Conversion des unités
        mask_wh = df["Unité"] == "Wh"
        df.loc[mask_wh, cols_index] /= 1000

        mask_mwh = df["Unité"] == "MWh"
        df.loc[mask_mwh, cols_index] *= 1000

        df["Unité"] = "kWh"
        return df
    


class FluxF1X(pa.DataFrameModel):
    
    
    # Qu'est-ce qui est facturé ?
    Id_EV: Series[str] = pa.Field(nullable=False)
    Libelle_EV: Series[str] = pa.Field(nullable=False)
    Nature_EV: Optional[Series[str]] = pa.Field(nullable=True) # pas dans le F12
    Date_Debut: Series[Annotated[pd.DatetimeTZDtype, "ns", "Europe/Paris"]] = pa.Field(nullable=False)
    Date_Fin: Series[Annotated[pd.DatetimeTZDtype, "ns", "Europe/Paris"]] = pa.Field(nullable=False)
    
    Unite: Series[str] = pa.Field(nullable=False)
    Quantite: Series[float] = pa.Field(nullable=False, coerce=True)
    

    # 💰 Taxes et Prix
    Prix_Unitaire: Series[float] = pa.Field(nullable=False, coerce=True)
    Montant_HT: Series[float] = pa.Field(nullable=False, coerce=True)
    Taux_TVA_Applicable: Series[str] = pa.Field(nullable=False)

    Formule_Tarifaire_Acheminement: Series[str] = pa.Field(nullable=True)
    

    # 🔹 Identifiant du Point de Livraison (PDL), aussi appelé Point Réf. Mesures (PRM)
    pdl: Series[str] = pa.Field(nullable=False)
    
    # 📆 Infos sur la facture qui contient cette ligne
    Num_Facture: Series[str] = pa.Field(nullable=False)
    Date_Facture: Series[Annotated[pd.DatetimeTZDtype, "ns", "Europe/Paris"]] = pa.Field(nullable=False)
    
    Type_Facturation: Series[str] = pa.Field(nullable=False)

    Source: Series[str] = pa.Field(nullable=False)
    @pa.dataframe_parser
    def parser_source(cls, df: DataFrame) -> DataFrame:
        df["Source"] = "Flux_"+df["Flux"]
        return df
    
    @pa.dataframe_parser
    def parser_colonnes_exclusives(cls, df: DataFrame) -> DataFrame:
        # 🛠️ Transformation spécifique F12
        df_f12 = df[df["Flux"] == "F12"].copy()
        if not df_f12.empty:
            df_f12["Formule_Tarifaire_Acheminement"] = df_f12["Tarif_Souscrit"]
            df_f12["Segment_Clientele"] = df_f12["Code_Segmentation_ERDF"]
            df_f12["Nature_EV"] = None  # Ce champ n'existe pas en F12

        # 🛠️ Transformation spécifique F15
        df_f15 = df[df["Flux"] == "F15"].copy()
        if not df_f15.empty:
            df_f15["Segment_Clientele"] = "C2"  # les F15 sont toujours en C2
            df_f15["CSPE_Applicable"] = True

        # Fusionner les deux sous-DataFrames après transformation
        df_transformed = pd.concat([df_f12, df_f15], ignore_index=True)

        return df_transformed
    
    @pa.parser("Taux_TVA_Applicable")
    def parse_TVA(cls, s: Series[str]) -> Series[str]:
        return pd.to_numeric(s, errors="coerce").fillna(s).astype(str)

    @pa.parser("Nature_EV")
    def parse_nature_EV(cls, s: Series[str]) -> Series[str]:
        
        mapping_catégories = {
            "01": "Acheminement",
            "02": "Prestations et frais",
            "03": "Dispositions contractuelles",
            "04": "Contributions",
        }

        return s.map(mapping_catégories)

    # 📆 Parser qui converti les Dates en CET "Europe/Paris"
    @pa.dataframe_parser
    def parser_dates(cls, df: DataFrame) -> DataFrame:
        for c in ["Date_Debut", "Date_Fin", "Date_Facture"]:
            df[c] = (
                pd.to_datetime(df[c])
                .dt.tz_localize("Europe/Paris")
            )
        return df