import pandas as pd
from pathlib import Path

from zoneinfo import ZoneInfo

import pandera.pandas as pa
from pandera.typing import DataFrame
from electricore.core.abonnements.modèles import PeriodeAbonnement

from icecream import ic
PARIS_TZ = ZoneInfo("Europe/Paris")

def load_turpe_rules() -> pd.DataFrame:
    """
    Charge les règles TURPE à partir du fichier CSV.
    """
    file_path = Path(__file__).parent / "turpe_rules.csv"
    turpe_rules = pd.read_csv(file_path, parse_dates=["start", "end"])

    # Convertir en date avec fuseau horaire
    
    turpe_rules["start"] = pd.to_datetime(turpe_rules["start"]).dt.tz_localize(PARIS_TZ)
    turpe_rules["end"] = pd.to_datetime(turpe_rules["end"]).dt.tz_localize(PARIS_TZ, ambiguous='NaT')

    # Convertir toutes les colonnes non-meta en float
    meta_columns = ["start", "end", "Formule_Tarifaire_Acheminement"]
    numeric_columns = [col for col in turpe_rules.columns if col not in meta_columns]
    turpe_rules[numeric_columns] = turpe_rules[numeric_columns].astype(float)
    return turpe_rules

def get_applicable_rules(start: pd.Timestamp, end: pd.Timestamp, rules: pd.DataFrame|None=None) -> pd.DataFrame:
    """
    Retourne les règles TURPE applicables pour une période donnée.

    :param start: Date de début de la période en pd.Timestamp.
    :param end: Date de fin de la période en pd.Timestamp.
    :param rules_df: DataFrame contenant les colonnes [Formule_Tarifaire_Acheminement, start, end, b, HPH, HCH, ...].
    :return: DataFrame avec les règles applicables.
    """
    if rules is None:
        rules = load_turpe_rules()

    # Gérer les valeurs NaT dans la colonne "end"
    rules["end"] = rules["end"].apply(lambda x: x if pd.notna(x) else pd.Timestamp.max.tz_localize(start.tz))

    # Filtrer les règles applicables à la période donnée
    applicable_rules = rules[
        (rules["start"] < end) &
        (rules["end"] > start)
    ]

    return applicable_rules

def compute_turpe(entries: pd.DataFrame, rules: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule le TURPE pour chaque entrée en utilisant les règles données.

    :param entries_df: DataFrame contenant les colonnes [start, end, FTA, Puissance_Souscrite, HPH, HCH, ...].
    :param rules_df: DataFrame contenant les colonnes [FTA, start, end, b, HPH, HCH, ...].
    :return: DataFrame avec les coûts calculés.
    """
    # Vérifier les doublons dans la colonne FTA
    duplicated = rules[rules.duplicated(subset=['Formule_Tarifaire_Acheminement'], keep=False)]
    if not duplicated.empty:
        raise ValueError(f"Doublons détectés dans la colonne Formule_Tarifaire_Acheminement :\n{duplicated}")

    # Fusionner les règles avec les entrées sur la clé FTA
    merged = pd.merge(entries, rules, on="Formule_Tarifaire_Acheminement", suffixes=("_entry", "_rule"))
    
    conso_cols = ["HPH", "HCH", "HPB", "HCB", "HP", "HC", "BASE"]
    # Calcul vectoriel des coûts fixes et variables
    merged["CS_fixe"] = merged["b"] * merged["Puissance_Souscrite"]
    for col in conso_cols:
        merged[f"turpe_{col}"] =  pd.to_numeric(merged[f"{col}_entry"] * merged[f"{col}_rule"] / 100, errors='coerce')

    merged["turpe_fixe_annuel"] = merged["CS_fixe"] + merged["cg"] + merged["cc"]
    merged["turpe_fixe_j"] = merged["turpe_fixe_annuel"] / 365

    merged["turpe_fixe"] = merged["turpe_fixe_j"] * merged["j"]

    merged["turpe_var"] = merged[['turpe_'+col for col in conso_cols]].sum(axis=1, min_count=1)
    merged["turpe"] = merged["turpe_fixe"] + merged["turpe_var"]
    
    columns_to_rename = {'start': 'Version_Turpe'} | {c+'_entry': c for c in conso_cols}
    merged = merged.rename(columns=columns_to_rename)
    
    columns_to_drop = [col for col in merged.columns if col.endswith('_entry')]+['end']

    merged = merged.drop(columns=columns_to_drop)

    merged['Version_Turpe'] = merged['Version_Turpe'].dt.date
    return merged.round(2)

@pa.check_types
def ajouter_turpe_fixe(
    periodes: DataFrame[PeriodeAbonnement],
    regles: pd.DataFrame
) -> DataFrame[PeriodeAbonnement]:
    """
    Calcule le TURPE fixe pour chaque période d'abonnement, selon les règles tarifaires.

    Args:
        periodes: Périodes d'abonnement homogènes (Formule_Tarifaire_Acheminement, Puissance_Souscrite, nb_jours)
        regles: Règles tarifaires TURPE, avec colonnes : FTA, b, cg, cc

    Returns:
        Périodes enrichies avec `turpe_fixe_journalier` et `turpe_fixe`
    """

    # Réplication croisée FTA entre périodes et règles
    df = pd.merge(
        periodes,
        regles,
        on="Formule_Tarifaire_Acheminement",
        how="left",
        suffixes=("", "_regle")
    )

    # Filtrage temporel
    df["end"] = df["end"].fillna(pd.Timestamp("2100-01-01").tz_localize(PARIS_TZ))

    mask = (df["periode_debut"] >= df["start"]) & (df["periode_debut"] < df["end"])
    df = df[mask].copy()

    # Calculs
    df["turpe_fixe_annuel"] = (df["b"] * df["Puissance_Souscrite"]) + df["cg"] + df["cc"]
    df["turpe_fixe_journalier"] = df["turpe_fixe_annuel"] / 365
    df["turpe_fixe"] = (df["turpe_fixe_journalier"] * df["nb_jours"]).round(2)

    return df[PeriodeAbonnement.to_schema().columns.keys()]
    
