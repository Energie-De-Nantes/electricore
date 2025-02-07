import pandas as pd
from zoneinfo import ZoneInfo
from electricore.core.energies import diviser_lignes_mct

def situation_périmetre(date: pd.Timestamp, c15: pd.DataFrame) -> pd.DataFrame:
    """
    Récupère la situation du périmètre fournisseur à une date donnée.

    Cette fonction extrait les lignes du DataFrame `c15` où la date de l'événement
    (`Date_Evenement`) est antérieure ou égale à la date spécifiée, puis trie ces lignes
    par `Date_Evenement` (ordre décroissant). Enfin, elle supprime les doublons en conservant 
    uniquement la situation la plus récente pour chaque couple (`pdl`, `Ref_Situation_Contractuelle`).

    Args:
        date (pd.Timestamp): La date de référence pour récupérer la situation du périmètre.
        c15 (pd.DataFrame): Le DataFrame contenant l'historique des situations contractuelles
            avec au minimum les colonnes suivantes :
            - "pdl" (str) : Identifiant du point de livraison.
            - "Ref_Situation_Contractuelle" (str) : Référence de la situation contractuelle.
            - "Date_Evenement" (pd.Timestamp) : Date de l'événement contractuel.

    Returns:
        pd.DataFrame: Un DataFrame filtré et nettoyé contenant la situation du périmètre à la date donnée.
    """
    return (
        c15[c15['Date_Evenement'] <= date]
        .copy()
        .sort_values(by='Date_Evenement', ascending=False)
        .drop_duplicates(subset=['pdl', 'Ref_Situation_Contractuelle'], keep='first')
    )

def base_facturation_perimetre(deb: pd.Timestamp, fin: pd.Timestamp, c15: pd.DataFrame) -> pd.DataFrame:
    """
    On veut les couples (usager.e, pdl) dont on doit estimer la consommation.

    Note : on s'intéresse aux couples (usager.e, pdl) qui sont dans le C15, dont le statut actuel est 'EN SERVICE' et celleux dont le statut est 'RESILIE' et dont la date de résiliation est dans la période de calcul.
    Args:
        deb (pd.Timestamp): début de la période de calcul
        fin (pd.Timestamp): fin de la période de calcul
        c15 (DataFrame): Flux de facturation C15 sous forme de DataFrame
    """
    # On ne veut pas considérer les lignes de C15 qui sont advenues après la fin de la période de calcul
    c15_fin = c15[c15['Date_Evenement'] <= fin]

    c15_periode = c15_fin[c15_fin['Date_Evenement'] >= deb]
    situation_fin = situation_périmetre(fin, c15)

    # 1)
    # on s'intéresse aux couples (usager.e, pdl) qui sont dans le C15, dont le statut actuel est 'EN SERVICE' et celleux dont le statut est 'RESILIE' et dont la date de résiliation est dans la période de calcul.
    _masque = (situation_fin['Etat_Contractuel'] == 'EN SERVICE') | ((situation_fin[ 'Etat_Contractuel'] == 'RESILIE') & (situation_fin['Date_Evenement'] >= deb))

    colonnes_releve = ['Date_Releve', 'Nature_Index', 'HP', 'HC', 'HCH', 'HPH', 'HPB', 'HCB', 'BASE']
    colonnes_evenement = ['Date_Derniere_Modification_FTA', 'Evenement_Declencheur', 'Type_Evenement', 'Date_Evenement', 'Ref_Demandeur', 'Id_Affaire']
    base_de_travail = (
        situation_fin[_masque]
        .drop(columns=colonnes_releve+colonnes_evenement)
        .copy()
        # .set_index('Ref_Situation_Contractuelle')
    )

    # 2) Entrées/sorties
    entrees = (
        c15_periode[c15_periode['Evenement_Declencheur']
        .isin(['MES', 'PMES', 'CFNE'])]
        .set_index('Ref_Situation_Contractuelle')[colonnes_releve]
        .add_suffix('_deb')
    )
    entrees['source_releve_deb'] = 'entree_C15'
    sorties = (
        c15_periode[c15_periode['Evenement_Declencheur']
        .isin(['RES', 'CFNS'])]
        .set_index('Ref_Situation_Contractuelle')[colonnes_releve]
        .add_suffix('_fin')
    )
    sorties['source_releve_fin'] = 'sortie_C15'
    # Fusion avec la base de travail
    base_de_travail = (
        base_de_travail
        .merge(entrees, how='left', left_on='Ref_Situation_Contractuelle', right_index=True)
        .merge(sorties, how='left', left_on='Ref_Situation_Contractuelle', right_index=True)
    )

    # 3) Prise en compte des MCT.
    mct = (
        c15_periode[c15_periode['Evenement_Declencheur']
        .isin(['MCT'])]
        # .set_index('Ref_Situation_Contractuelle')[colonnes_releve]
    )

    base_de_travail = diviser_lignes_mct(base_de_travail, mct, colonnes_releve)
    return base_de_travail


def ajout_relevés_R151(deb: pd.Timestamp, fin: pd.Timestamp, df: pd.DataFrame, r151: pd.DataFrame) -> pd.DataFrame:
    """
    Vient ajouter les relevés issus du R151 la ou il n'y a pas de relevé.

    Note : 
       Il peut rester des trous dans les relevés, même après cette opération.

    Args:
        deb (pd.Timestamp): La date de début de la période.
        fin (pd.Timestamp): La date de fin de la période.
        df (DataFrame): Le DataFrame à traiter.
    Returns:
        DataFrame: Le DataFrame enrichie avec les relevés issus du R151 ajoutés.
    """

    colonnes_releve_r151 = ['Date_Releve', 'HP', 'HC', 'HCH', 'HPH', 'HPB', 'HCB', 'BASE']
    r151['Date_Releve'] = (
        pd.to_datetime(r151['Date_Releve'])
        .dt.tz_localize(None)
        .dt.tz_localize(ZoneInfo("Europe/Paris"))
    )
    # Pour les débuts de période
    
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
    # Trouver les valeurs à mettre à jour dans df
    masque_deb = (r['source_releve_deb'].isna()) | (r['source_releve_deb'] == 'R151')
    masque_fin = (r['source_releve_fin'].isna()) | (r['source_releve_fin'] == 'R151')

    # MAJ des valeurs 
    r.loc[masque_deb, releves_deb.columns] = (
        releves_deb
        .reindex(r.loc[masque_deb, 'pdl'])
        .values
    )
    r.loc[masque_fin, releves_fin.columns] = (
        releves_fin
        .reindex(r.loc[masque_fin, 'pdl'])
        .values
    )
    return r

def base_from_electriflux(deb: pd.Timestamp, fin: pd.Timestamp, c15: pd.DataFrame, r151: pd.DataFrame) -> pd.DataFrame:
    alors = base_facturation_perimetre(deb, fin, c15)
    indexes = ajout_relevés_R151(deb, fin, alors, r151)
    return indexes