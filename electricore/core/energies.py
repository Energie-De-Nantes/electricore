import pandas as pd
import warnings

def diviser_lignes_mct(base_df: pd.DataFrame, mct_df: pd.DataFrame, colonnes_releve: list) -> pd.DataFrame:
    """
    Divise les lignes ayant une MCT en deux périodes : avant et après MCT.
    
    Args:
        base_df (pd.DataFrame): DataFrame original contenant les lignes à diviser
        mct_df (pd.DataFrame): DataFrame contenant les données MCT avec Ref_Situation_Contractuelle
        colonnes_releve (list): Liste des colonnes de relevé à considérer
    
    Returns:
        pd.DataFrame: DataFrame avec les lignes MCT divisées en deux périodes
    """
    # Identification des lignes avec MCT
    lignes_avec_mct = (
        base_df.index[
            base_df['Ref_Situation_Contractuelle']
            .isin(mct_df['Ref_Situation_Contractuelle'])
        ]
    )        
    if len(lignes_avec_mct) == 0:
        return base_df

    # Création des deux nouveaux jeux de lignes
    lignes_avant_mct = base_df.loc[lignes_avec_mct].copy()
    lignes_apres_mct = base_df.loc[lignes_avec_mct].copy()

    # Préparation des colonnes MCT avec les bons suffixes
    mct_fin = mct_df[['Ref_Situation_Contractuelle'] + colonnes_releve].copy()
    mct_fin.columns = ['Ref_Situation_Contractuelle'] + [f'{col}_fin' for col in colonnes_releve]
    
    mct_deb = mct_df[['Ref_Situation_Contractuelle'] + colonnes_releve].copy()
    mct_deb.columns = ['Ref_Situation_Contractuelle'] + [f'{col}_deb' for col in colonnes_releve]

    # Suppression des anciennes colonnes de fin/début avant la fusion
    colonnes_a_supprimer_fin = [f'{col}_fin' for col in colonnes_releve]
    colonnes_a_supprimer_deb = [f'{col}_deb' for col in colonnes_releve]
    
    lignes_avant_mct = lignes_avant_mct.drop(columns=colonnes_a_supprimer_fin, errors='ignore')
    lignes_apres_mct = lignes_apres_mct.drop(columns=colonnes_a_supprimer_deb, errors='ignore')

    # Fusion avec les données MCT
    lignes_avant_mct = lignes_avant_mct.merge(
        mct_fin,
        on='Ref_Situation_Contractuelle'
    )
    lignes_avant_mct['source_releve_fin'] = 'MCT'

    lignes_apres_mct = lignes_apres_mct.merge(
        mct_deb,
        on='Ref_Situation_Contractuelle'
    )
    lignes_apres_mct['source_releve_deb'] = 'MCT'

    # Construction du DataFrame final
    return pd.concat([
        base_df.drop(index=lignes_avec_mct),
        lignes_avant_mct,
        lignes_apres_mct
    ])

def ajout_dates_par_defaut(deb: pd.Timestamp, fin: pd.Timestamp, df: pd.DataFrame) -> pd.DataFrame:
    """
    Ajoute les valeurs par défaut pour les colonnes 'Date_Releve_deb', 'Date_Releve_fin', 'source_releve_deb', et 'source_releve_fin'.
    Args:
        df (DataFrame): Le DataFrame à traiter.
        deb (pd.Timestamp): La date de début par défaut.
        fin (pd.Timestamp): La date de fin par défaut.
    Returns:
        DataFrame: Le DataFrame avec les valeurs par défaut ajoutées.
    """
    df['Date_Releve_deb'] = df['Date_Releve_deb'].fillna(deb)
    df['Date_Releve_fin'] = df['Date_Releve_fin'].fillna(fin)
    return df

def calcul_energie(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcul de la consommation d'énergie en kWh, par cadran
    
    """
    # Calcul de la consommation d'énergie
    r = df.copy()

    cadrans = ['HPH', 'HPB', 'HCH', 'HCB', 'HP', 'HC',  'BASE']
    df['releve_manquant'] = df[['source_releve_fin', 'source_releve_deb']].isna().any(axis=1)
    for c in cadrans:
        colonne_deb = f"{c}_deb"
        colonne_fin = f"{c}_fin"
        if colonne_deb in r.columns and colonne_fin in r.columns:
            # Vérifie que les deux sources de relevé sont non nulles
            masque_valide = ~df['releve_manquant']
            diff = r.loc[masque_valide, colonne_fin] - r.loc[masque_valide, colonne_deb]
            
            pdls_negatifs = r.loc[masque_valide][diff < 0]['pdl'].tolist()
            if pdls_negatifs:
                warnings.warn(f"Valeurs négatives détectées pour le compteur {c} "
                            f"sur les PDLs: {pdls_negatifs}")
            
            # Initialise la colonne avec NaN
            r[c] = pd.NA
            # Applique le calcul uniquement où mask_valide est True
            r.loc[masque_valide, c] = diff
            
    # Calcul du nombre de jours entre les deux relevés
    r['j'] = (r['Date_Releve_fin'] - r['Date_Releve_deb']).dt.days

    # Calculer HP et HC en prenant la somme des colonnes correspondantes
    r['HP'] = r[['HPH', 'HPB', 'HP']].sum(axis=1, min_count=1)
    r['HC'] = r[['HCH', 'HCB', 'HC']].sum(axis=1, min_count=1)

    # Calculer BASE uniquement là où BASE est NaN
    r.loc[r['BASE'].isna(), 'BASE'] = r[['HP', 'HC']].sum(axis=1, min_count=1)
    
    return r