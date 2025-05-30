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