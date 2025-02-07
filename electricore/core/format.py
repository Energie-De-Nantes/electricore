import pandas as pd
def supprimer_colonnes(df: pd.DataFrame)-> pd.DataFrame:
    to_drop = ['turpe_fixe_annuel', 'turpe_fixe_j', 'cg', 'cc', 'b', 'CS_fixe'] + [c for c in df.columns if c.endswith('_rule')]
    to_drop = [c for c in to_drop if c in df.columns]
    return df.drop(columns=to_drop)


def fusion_des_sous_periode(df: pd.DataFrame)-> pd.DataFrame:
    def custom_agg(df):
        agg_dict = {}
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                agg_dict[col] = lambda x: x.sum(min_count=1)
            else:
                agg_dict[col] = "first"  # Prend la première valeur
        return agg_dict
    # Appliquer le groupby avec la fonction d'agrégation conditionnelle
    return df.groupby("Ref_Situation_Contractuelle").agg(custom_agg(df))

def validation(df: pd.DataFrame)-> pd.DataFrame:
    df['releve_manquant'] = df[['source_releve_fin', 'source_releve_deb']].isna().any(axis=1)
    return df