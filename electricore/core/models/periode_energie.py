import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series
from typing import Annotated, Optional


class PeriodeEnergie(pa.DataFrameModel):
    """
    Représente une période homogène de calcul d'énergie entre deux relevés successifs.
    
    Cette classe modélise les périodes de consommation/production d'énergie électrique
    avec les références d'index, les sources de données et les indicateurs de qualité.
    """
    # Identifiants
    pdl: Series[str] = pa.Field(nullable=False)
    Ref_Situation_Contractuelle: Optional[Series[str]] = pa.Field(nullable=True)
    
    # Période
    debut: Series[Annotated[pd.DatetimeTZDtype, "ns", "Europe/Paris"]] = pa.Field(nullable=False, coerce=True)
    fin: Series[Annotated[pd.DatetimeTZDtype, "ns", "Europe/Paris"]] = pa.Field(nullable=False, coerce=True)
    nb_jours: Series[int] = pa.Field(nullable=True, ge=0)
    
    # Dates lisibles (optionnelles)
    debut_lisible: Optional[Series[str]] = pa.Field(nullable=True)
    fin_lisible: Optional[Series[str]] = pa.Field(nullable=True)
    mois_annee: Optional[Series[str]] = pa.Field(nullable=True)
    
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
    
    # Informations contractuelles pour calcul TURPE (colonnes optionnelles)
    Formule_Tarifaire_Acheminement: Optional[Series[str]] = pa.Field(nullable=True)
    
    # Calculs TURPE (colonnes optionnelles)
    turpe_variable: Optional[Series[float]] = pa.Field(nullable=True, coerce=True)
    turpe_total: Optional[Series[float]] = pa.Field(nullable=True, coerce=True)
    
    class Config:
        """Configuration pour permettre des colonnes supplémentaires."""
        strict = False