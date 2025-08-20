import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series
from typing import Annotated


class PeriodeEnergie(pa.DataFrameModel):
    """
    Représente une période homogène de calcul d'énergie entre deux relevés successifs.
    
    Cette classe modélise les périodes de consommation/production d'énergie électrique
    avec les références d'index, les sources de données et les indicateurs de qualité.
    """
    # Identifiants
    pdl: Series[str] = pa.Field(nullable=False)
    
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