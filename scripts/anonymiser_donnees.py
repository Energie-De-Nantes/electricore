#!/usr/bin/env python3
"""
Script d'anonymisation des données Enedis pour fixtures de tests.

Ce script prend des données réelles de production et les anonymise tout en
préservant la cohérence métier et les relations temporelles.

Usage:
    poetry run python scripts/anonymiser_donnees.py \
        --input-historique data/prod/historique.parquet \
        --input-releves data/prod/releves.parquet \
        --output-dir tests/fixtures/donnees_anonymisees \
        --cas-name "mct_changement_calendrier"

Principes d'anonymisation :
- PDL : remplacés par séquences génériques (PDL00001, PDL00002, ...)
- Dates : décalage aléatoire uniforme de 365-730 jours
- Index énergétiques : arrondis avec bruit pour masquer consommations exactes
- Références contractuelles : anonymisées séquentiellement
- Préservation : relations temporelles, séquences d'événements, cohérence métier
"""

import argparse
import random
from pathlib import Path
from datetime import timedelta
from typing import Tuple

import polars as pl


# =========================================================================
# CONFIGURATION ANONYMISATION
# =========================================================================

ANONYMIZATION_CONFIG = {
    "date_offset_min_days": 365,  # Décalage minimum en jours
    "date_offset_max_days": 730,  # Décalage maximum en jours
    "index_rounding": 10,          # Arrondir les index aux 10 kWh près
    "index_noise_percent": 2,      # Ajouter +/- 2% de bruit
    "preserve_pdl_count": True,    # Garder le même nombre de PDLs
    "seed": None,                  # Seed aléatoire (None = aléatoire)
}


# =========================================================================
# FONCTIONS ANONYMISATION
# =========================================================================


def generer_mapping_pdl(pdls_originaux: list[str], seed: int = None) -> dict[str, str]:
    """
    Génère un mapping PDL original → PDL anonymisé.

    Les PDLs anonymisés sont de la forme PDL00001, PDL00002, etc.

    Args:
        pdls_originaux: Liste des PDLs à anonymiser
        seed: Seed pour reproductibilité

    Returns:
        Dictionnaire de mapping {pdl_original: pdl_anonymise}
    """
    if seed:
        random.seed(seed)

    # Mélanger l'ordre pour éviter de révéler l'ordre original
    pdls_shuffled = pdls_originaux.copy()
    random.shuffle(pdls_shuffled)

    # Créer mapping avec numéros séquentiels
    mapping = {
        pdl_original: f"PDL{i:05d}"
        for i, pdl_original in enumerate(pdls_shuffled, start=1)
    }

    return mapping


def generer_offset_temporel(seed: int = None) -> int:
    """
    Génère un offset temporel aléatoire en jours.

    Returns:
        Nombre de jours de décalage
    """
    if seed:
        random.seed(seed)

    return random.randint(
        ANONYMIZATION_CONFIG["date_offset_min_days"],
        ANONYMIZATION_CONFIG["date_offset_max_days"]
    )


def anonymiser_pdl_column(df: pl.DataFrame, mapping: dict[str, str]) -> pl.DataFrame:
    """
    Anonymise la colonne 'pdl' d'un DataFrame.

    Args:
        df: DataFrame contenant une colonne 'pdl'
        mapping: Dictionnaire de mapping PDL

    Returns:
        DataFrame avec PDLs anonymisés
    """
    # Utiliser replace avec mapping
    return df.with_columns(
        pl.col("pdl").replace(mapping, default=None)
    )


def decaler_dates(df: pl.DataFrame, offset_days: int, colonnes_dates: list[str]) -> pl.DataFrame:
    """
    Décale toutes les colonnes de dates d'un offset fixe.

    Args:
        df: DataFrame
        offset_days: Nombre de jours de décalage
        colonnes_dates: Liste des colonnes à décaler

    Returns:
        DataFrame avec dates décalées
    """
    offset = timedelta(days=offset_days)

    for col_date in colonnes_dates:
        if col_date in df.columns:
            df = df.with_columns(
                (pl.col(col_date) + offset).alias(col_date)
            )

    return df


def anonymiser_index_energie(
    df: pl.DataFrame,
    colonnes_index: list[str]
) -> pl.DataFrame:
    """
    Anonymise les index énergétiques en arrondissant et ajoutant du bruit.

    Préserve les différences relatives entre index successifs.

    Args:
        df: DataFrame contenant des colonnes d'index
        colonnes_index: Liste des colonnes à anonymiser (BASE, HP, HC, etc.)

    Returns:
        DataFrame avec index anonymisés
    """
    rounding = ANONYMIZATION_CONFIG["index_rounding"]
    noise_pct = ANONYMIZATION_CONFIG["index_noise_percent"]

    for col_index in colonnes_index:
        if col_index in df.columns:
            # Arrondir puis ajouter bruit aléatoire
            df = df.with_columns(
                (
                    (pl.col(col_index) / rounding).round() * rounding
                    * (1 + pl.lit(random.uniform(-noise_pct/100, noise_pct/100)))
                ).alias(col_index)
            )

    return df


def anonymiser_ref_contractuelles(df: pl.DataFrame) -> pl.DataFrame:
    """
    Anonymise les références de situations contractuelles.

    Args:
        df: DataFrame contenant 'ref_situation_contractuelle'

    Returns:
        DataFrame avec références anonymisées
    """
    if "ref_situation_contractuelle" not in df.columns:
        return df

    # Créer mapping des refs uniques
    refs_uniques = df["ref_situation_contractuelle"].unique().to_list()
    mapping_refs = {
        ref: f"REF{i:03d}"
        for i, ref in enumerate(refs_uniques, start=1)
    }

    return df.with_columns(
        pl.col("ref_situation_contractuelle").replace(mapping_refs, default=None)
    )


# =========================================================================
# FONCTION PRINCIPALE D'ANONYMISATION
# =========================================================================


def anonymiser_cas_metier(
    historique_df: pl.DataFrame,
    releves_df: pl.DataFrame,
    seed: int = None
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Anonymise un cas métier complet (historique + relevés).

    Args:
        historique_df: DataFrame historique (flux C15)
        releves_df: DataFrame relevés (flux R151)
        seed: Seed pour reproductibilité

    Returns:
        Tuple (historique_anonymise, releves_anonymises)
    """
    # 1. Générer mapping PDL
    pdls_historique = historique_df["pdl"].unique().to_list()
    pdls_releves = releves_df["pdl"].unique().to_list()
    tous_pdls = list(set(pdls_historique + pdls_releves))

    mapping_pdl = generer_mapping_pdl(tous_pdls, seed=seed)

    # 2. Générer offset temporel unique pour tout le cas
    offset_days = generer_offset_temporel(seed=seed)

    # 3. Anonymiser historique
    historique_anonymise = historique_df.clone()

    # PDLs
    historique_anonymise = anonymiser_pdl_column(historique_anonymise, mapping_pdl)

    # Dates
    colonnes_dates_historique = [
        "Date_Evenement",
        "Date_Debut_Fourniture",
        "Date_Fin_Fourniture",
    ]
    historique_anonymise = decaler_dates(
        historique_anonymise,
        offset_days,
        colonnes_dates_historique
    )

    # Refs contractuelles
    historique_anonymise = anonymiser_ref_contractuelles(historique_anonymise)

    # 4. Anonymiser relevés
    releves_anonymises = releves_df.clone()

    # PDLs
    releves_anonymises = anonymiser_pdl_column(releves_anonymises, mapping_pdl)

    # Dates
    colonnes_dates_releves = ["date_releve", "Date_Releve"]
    releves_anonymises = decaler_dates(
        releves_anonymises,
        offset_days,
        colonnes_dates_releves
    )

    # Index énergétiques
    colonnes_index = ["BASE", "HP", "HC", "HPH", "HCH", "HPB", "HCB"]
    releves_anonymises = anonymiser_index_energie(releves_anonymises, colonnes_index)

    # Refs contractuelles
    releves_anonymises = anonymiser_ref_contractuelles(releves_anonymises)

    return historique_anonymise, releves_anonymises


# =========================================================================
# CLI
# =========================================================================


def main():
    """Point d'entrée CLI."""
    parser = argparse.ArgumentParser(
        description="Anonymise des données Enedis pour fixtures de tests"
    )

    parser.add_argument(
        "--input-historique",
        type=Path,
        required=True,
        help="Chemin vers le fichier historique (parquet)"
    )

    parser.add_argument(
        "--input-releves",
        type=Path,
        required=True,
        help="Chemin vers le fichier relevés (parquet)"
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Répertoire de sortie pour les fichiers anonymisés"
    )

    parser.add_argument(
        "--cas-name",
        type=str,
        required=True,
        help="Nom du cas métier (ex: mct_changement_calendrier)"
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Seed pour reproductibilité (optionnel)"
    )

    args = parser.parse_args()

    # Vérifier que les fichiers existent
    if not args.input_historique.exists():
        raise FileNotFoundError(f"Fichier historique introuvable: {args.input_historique}")

    if not args.input_releves.exists():
        raise FileNotFoundError(f"Fichier relevés introuvable: {args.input_releves}")

    # Créer répertoire de sortie si nécessaire
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print(f"🔐 Anonymisation du cas métier '{args.cas_name}'...")
    print(f"   Historique : {args.input_historique}")
    print(f"   Relevés    : {args.input_releves}")

    # Charger les données
    print("📥 Chargement des données...")
    historique_df = pl.read_parquet(args.input_historique)
    releves_df = pl.read_parquet(args.input_releves)

    print(f"   Historique : {historique_df.shape[0]} lignes, {len(historique_df['pdl'].unique())} PDLs")
    print(f"   Relevés    : {releves_df.shape[0]} lignes, {len(releves_df['pdl'].unique())} PDLs")

    # Anonymiser
    print("🎭 Anonymisation en cours...")
    historique_anonymise, releves_anonymises = anonymiser_cas_metier(
        historique_df,
        releves_df,
        seed=args.seed
    )

    # Sauvegarder
    output_historique = args.output_dir / f"{args.cas_name}_historique.parquet"
    output_releves = args.output_dir / f"{args.cas_name}_releves.parquet"

    print("💾 Sauvegarde des fichiers anonymisés...")
    historique_anonymise.write_parquet(output_historique)
    releves_anonymises.write_parquet(output_releves)

    print(f"✅ Anonymisation terminée !")
    print(f"   Historique : {output_historique}")
    print(f"   Relevés    : {output_releves}")
    print(f"\n📝 Vous pouvez maintenant mettre à jour la fixture pytest dans:")
    print(f"   tests/fixtures/cas_metier.py")


if __name__ == "__main__":
    main()