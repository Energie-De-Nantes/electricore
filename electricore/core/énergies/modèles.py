import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series, DataFrame
from typing import Annotated

# EN QUESTION : En GROS c'est un schéma hybride, composé d'un HistoriquePérimètre + deux RelevésIndex 
# Est-ce qu'on fait un truc d'héritage des modèles ? ou balek ? 
class BaseCalculEnergies(pa.DataFrameModel):
    """
    📌 Modèle Pandera pour la base de calcul des énergies.

    Ce modèle garantit que la base de calcul des énergies est conforme
    et prête à être utilisée pour des calculs plus avancés.
    """
    
    # Sous modèle Pèrimètre : Données contractuelles / Métadonnées
    # Timestamp
    # Date_Evenement: Series[Annotated[pd.DatetimeTZDtype, "ns", "Europe/Paris"]] = pa.Field(nullable=False, coerce=True)

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
    
    # Sous modèle RelevéIndex : Relevés début de période
    Date_Releve_deb: Series[Annotated[pd.DatetimeTZDtype, "ns", "Europe/Paris"]] = pa.Field(nullable=True)
    Source_deb: Series[str] = pa.Field(nullable=False, isin=["flux_R151", "flux_R15", "flux_C15"])

    # 📏 Unité de mesure
    Unité_deb: Series[str] = pa.Field(nullable=False, eq="kWh")
    Précision_deb: Series[str] = pa.Field(nullable=False, isin=["kWh", "Wh", "MWh"])

    # ⚡ Mesures
    HP_deb: Series[float] = pa.Field(nullable=True, coerce=True)
    HC_deb: Series[float] = pa.Field(nullable=True, coerce=True)
    HCH_deb: Series[float] = pa.Field(nullable=True, coerce=True)
    HPH_deb: Series[float] = pa.Field(nullable=True, coerce=True)
    HPB_deb: Series[float] = pa.Field(nullable=True, coerce=True)
    HCB_deb: Series[float] = pa.Field(nullable=True, coerce=True)
    BASE_deb: Series[float] = pa.Field(nullable=True, coerce=True)

    # Sous modèle RelevéIndex : Relevés fin de période
    Date_Releve_fin: Series[Annotated[pd.DatetimeTZDtype, "ns", "Europe/Paris"]] = pa.Field(nullable=True)
    Source_fin: Series[str] = pa.Field(nullable=False, isin=["flux_R151", "flux_R15", "flux_C15"])

    # 📏 Unité de mesure
    Unité_fin: Series[str] = pa.Field(nullable=False, eq="kWh")
    Précision_fin: Series[str] = pa.Field(nullable=False, isin=["kWh", "Wh", "MWh"])

    # ⚡ Mesures
    HP_fin: Series[float] = pa.Field(nullable=True, coerce=True)
    HC_fin: Series[float] = pa.Field(nullable=True, coerce=True)
    HCH_fin: Series[float] = pa.Field(nullable=True, coerce=True)
    HPH_fin: Series[float] = pa.Field(nullable=True, coerce=True)
    HPB_fin: Series[float] = pa.Field(nullable=True, coerce=True)
    HCB_fin: Series[float] = pa.Field(nullable=True, coerce=True)
    BASE_fin: Series[float] = pa.Field(nullable=True, coerce=True)


class PeriodeEnergie(pa.DataFrameModel):
    """
    Représente une période homogène de calcul d'énergie entre deux relevés successifs.
    
    Cette classe modélise les périodes de consommation/production d'énergie électrique
    avec les références d'index, les sources de données et les indicateurs de qualité.
    """
    # Identifiants
    pdl: Series[str] = pa.Field(nullable=False)
    id_index_avant: Series[object] = pa.Field(nullable=False)  # int ou str
    id_index_apres: Series[object] = pa.Field(nullable=False)  # int ou str
    
    # Période
    Date_Debut: Series[Annotated[pd.DatetimeTZDtype, "ns", "Europe/Paris"]] = pa.Field(nullable=False, coerce=True)
    Date_Fin: Series[Annotated[pd.DatetimeTZDtype, "ns", "Europe/Paris"]] = pa.Field(nullable=False, coerce=True)
    duree_jours: Series[int] = pa.Field(nullable=True, ge=0)
    
    # Sources des relevés
    source_avant: Series[str] = pa.Field(nullable=False)
    source_apres: Series[str] = pa.Field(nullable=False)
    
    # Flags de qualité des données
    data_complete: Series[bool] = pa.Field(nullable=False)
    periode_irreguliere: Series[bool] = pa.Field(nullable=False)
    
    # Énergies par cadran (optionnelles selon le type de compteur)
    BASE_energie: Series[float] = pa.Field(nullable=True, coerce=True)
    HP_energie: Series[float] = pa.Field(nullable=True, coerce=True) 
    HC_energie: Series[float] = pa.Field(nullable=True, coerce=True)
    HPH_energie: Series[float] = pa.Field(nullable=True, coerce=True)
    HPB_energie: Series[float] = pa.Field(nullable=True, coerce=True)
    HCH_energie: Series[float] = pa.Field(nullable=True, coerce=True)
    HCB_energie: Series[float] = pa.Field(nullable=True, coerce=True)