import numpy as np
import pandas as pd
import pandera as pa
from pandera.typing import DataFrame, Series
from typing import Annotated
from icecream import ic
from electricore.core.energies import diviser_lignes_mct

from electricore.core.models import BaseFacturationModèle
  
# Définition du Modèle pour le DataFrame c15
class C15Schema(pa.DataFrameModel):
    pdl: Series[str] = pa.Field(nullable=False,)
    Date_Evenement: Series[Annotated[pd.DatetimeTZDtype, "ns", "Europe/Paris"]] = pa.Field(nullable=True, coerce=True)
    Date_Releve: Series[Annotated[pd.DatetimeTZDtype, "ns", "Europe/Paris"]] = pa.Field(nullable=True, coerce=True)
    Ref_Situation_Contractuelle: Series[str] = pa.Field(nullable=False,)
    Etat_Contractuel: Series[str] = pa.Field(nullable=False,)
    Evenement_Declencheur: Series[str] = pa.Field(nullable=True)
    Num_Depannage: Series[str] = pa.Field(nullable=False,)
    Type_Compteur: Series[str] = pa.Field(nullable=False,)
    Num_Compteur: Series[str] = pa.Field(nullable=False,)
    Nature_Index: Series[str] = pa.Field(nullable=True)
    HP: Series[float] = pa.Field(nullable=True)
    HC: Series[float] = pa.Field(nullable=True)
    HCH: Series[float] = pa.Field(nullable=True)
    HPH: Series[float] = pa.Field(nullable=True)
    HPB: Series[float] = pa.Field(nullable=True)
    HCB: Series[float] = pa.Field(nullable=True)
    BASE: Series[float] = pa.Field(nullable=True)

class R151Schema(pa.DataFrameModel):
    pdl: Series[str]
    Date_Releve: Series[Annotated[pd.DatetimeTZDtype, "ns", "Europe/Paris"]] = pa.Field(nullable=True, coerce=True)
    HP: Series[float] = pa.Field(nullable=True)
    HC: Series[float] = pa.Field(nullable=True)
    HCH: Series[float] = pa.Field(nullable=True)
    HPH: Series[float] = pa.Field(nullable=True)
    HPB: Series[float] = pa.Field(nullable=True)
    HCB: Series[float] = pa.Field(nullable=True)
    BASE: Series[float] = pa.Field(nullable=True)

@pa.check_types
def situation_périmetre(date: pd.Timestamp, c15: DataFrame[C15Schema]) -> DataFrame[C15Schema]:
    return (
        c15[c15['Date_Evenement'] <= date]
        .copy()
        .sort_values(by='Date_Evenement', ascending=False)
        .drop_duplicates(subset=['pdl', 'Ref_Situation_Contractuelle'], keep='first')
    )

@pa.check_types
def filtrer_evenements_c15(
    c15: DataFrame[C15Schema], deb: pd.Timestamp, fin: pd.Timestamp, evenements: list
) -> DataFrame[C15Schema]:
    return c15[
        (c15['Date_Evenement'] >= deb) & 
        (c15['Date_Evenement'] <= fin) & 
        (c15['Evenement_Declencheur'].isin(evenements))
    ].copy()

@pa.check_types
def extraire_releves_c15(
    c15: DataFrame[C15Schema], deb: pd.Timestamp, fin: pd.Timestamp, evenements: list, colonnes_releve: list, suffixe: str
) -> DataFrame:
    
    releves = filtrer_evenements_c15(c15, deb, fin, evenements)
    
    if releves.empty:
        return pd.DataFrame(columns=["Ref_Situation_Contractuelle"] + [f"{col}{suffixe}" for col in colonnes_releve] + [f"source_releve{suffixe}"])

    releves = (
        releves
        .set_index("Ref_Situation_Contractuelle")[colonnes_releve]
        .add_suffix(suffixe)
    )
    releves[f"source_releve{suffixe}"] = f"releve_C15_{suffixe.strip('_')}"
    
    return releves

@pa.check_types
def base_facturation_perimetre(deb: pd.Timestamp, fin: pd.Timestamp, c15: DataFrame[C15Schema]) -> DataFrame:
    
    c15_fin = c15[c15['Date_Evenement'] <= fin]
    c15_periode = c15_fin[c15_fin['Date_Evenement'] >= deb]
    situation_fin = situation_périmetre(fin, c15)

    _masque = (situation_fin['Etat_Contractuel'] == 'EN SERVICE') | (
        (situation_fin['Etat_Contractuel'] == 'RESILIE') & (situation_fin['Date_Evenement'] >= deb)
    )

    colonnes_releve = ['Date_Releve', 'Nature_Index', 'HP', 'HC', 'HCH', 'HPH', 'HPB', 'HCB', 'BASE']
    colonnes_evenement = ['Date_Derniere_Modification_FTA', 'Evenement_Declencheur', 'Type_Evenement', 'Date_Evenement', 'Ref_Demandeur', 'Id_Affaire']
    
    base_de_travail = (
        situation_fin[_masque]
        .drop(columns=[col for col in colonnes_releve + colonnes_evenement if col in situation_fin])
        .copy()
    )

    entrees = extraire_releves_c15(c15, deb, fin, ['MES', 'PMES', 'CFNE'], colonnes_releve, '_deb')
    sorties = extraire_releves_c15(c15, deb, fin, ['RES', 'CFNS'], colonnes_releve, '_fin')

    base_de_travail = (
        base_de_travail
        .merge(entrees, how='left', left_on='Ref_Situation_Contractuelle', right_index=True)
        .merge(sorties, how='left', left_on='Ref_Situation_Contractuelle', right_index=True)
    )

    mct = c15_periode[c15_periode['Evenement_Declencheur'].isin(['MCT'])]

    base_de_travail = diviser_lignes_mct(base_de_travail, mct, colonnes_releve)
    ic(base_de_travail)
    return base_de_travail

@pa.check_types
def ajout_relevés_R151(
    deb: pd.Timestamp, fin: pd.Timestamp, df: DataFrame, r151: DataFrame[R151Schema]
) -> DataFrame:
    
    colonnes_releve_r151 = ['Date_Releve', 'HP', 'HC', 'HCH', 'HPH', 'HPB', 'HCB', 'BASE']
    
    releves_deb = (
        r151[r151['Date_Releve'].dt.date == deb.date()]
        .set_index('pdl')[colonnes_releve_r151]
        .assign(source_releve='R151')
        .add_suffix('_deb')
    )
    
    releves_fin = (
        r151[r151['Date_Releve'].dt.date == fin.date()]
        .set_index('pdl')[colonnes_releve_r151]
        .assign(source_releve='R151')
        .add_suffix('_fin')
    )
    
    r = df.copy()
    masque_deb = (r['source_releve_deb'].isna()) | (r['source_releve_deb'] == 'R151')
    masque_fin = (r['source_releve_fin'].isna()) | (r['source_releve_fin'] == 'R151')

    r.loc[masque_deb, releves_deb.columns] = releves_deb.reindex(r.loc[masque_deb, 'pdl']).values
    r.loc[masque_fin, releves_fin.columns] = releves_fin.reindex(r.loc[masque_fin, 'pdl']).values

    return r

def convert_base(df: DataFrame) -> DataFrame:
    to_convert = (
        [c+'_deb' for c in ['HP', 'HC', 'HCH', 'HPH', 'HPB', 'HCB', 'BASE']]
        + [c+'_fin' for c in ['HP', 'HC', 'HCH', 'HPH', 'HPB', 'HCB', 'BASE']]
    )
    to_convert = [c for c in to_convert if c in df.columns]
    for c in to_convert:
        df[c] = pd.to_numeric(df[c], errors="coerce")  # Convertir les valeurs
        df[c] = df[c].replace({pd.NA: np.nan})  # Remplace <NA> par np.nan

    return df

@pa.check_types
def base_from_electriflux(
    deb: pd.Timestamp, fin: pd.Timestamp, c15: DataFrame[C15Schema], r151: DataFrame[R151Schema]
) -> DataFrame[BaseFacturationModèle]:
    alors = base_facturation_perimetre(deb, fin, c15)
    indexes = ajout_relevés_R151(deb, fin, alors, r151)
    res = convert_base(indexes)
    return res
