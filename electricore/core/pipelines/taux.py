"""
Le registre des taux régulés versionnés : lecture + sélection du taux en vigueur.

Deux faces d'un même concept (« le taux en vigueur à une date ») :
- `charger_regles_taux` lit un registre `*_rules.csv` en historique typé
  `(start, <taux>, reference)` ;
- `ajouter_taux_en_vigueur` attache à chaque ligne le taux applicable à sa date.

Consommé par les pipelines de taxes (Accise, CTA) via des adaptateurs qui
configurent uniquement le fichier de règles, la colonne de taux et la colonne de
date ; `millesimes.py` lit le millésime du même historique. TURPE est hors
périmètre — sa grille est 2-D (FTA × fenêtre `start`/`end`) et garde son propre
lecteur dans `turpe.py`.
"""

from pathlib import Path

import polars as pl

from electricore.core.models.historique_taux import historique_taux_schema

_CONFIG_DIR = Path(__file__).parent.parent.parent / "config"


def charger_regles_taux(nom_fichier: str, taux_col: str) -> pl.LazyFrame:
    """Charge un registre de taux régulé versionné depuis `electricore/config/`.

    Lecteur partagé des fichiers `*_rules.csv` à schéma « date d'entrée en vigueur
    → taux » (Accise, CTA). Parse `start` en datetime Europe/Paris et caste
    `taux_col` en Float64 ; la *Référence réglementaire* (`reference`, ADR-0024)
    traverse telle quelle.

    Args:
        nom_fichier: nom du CSV dans `electricore/config/` (ex. `"cta_rules.csv"`).
        taux_col: colonne du taux à caster en Float64 (ex. `"taux_cta_pct"`).

    Returns:
        LazyFrame `(start: Datetime[Europe/Paris], <taux_col>: Float64, reference)`,
        conforme à `historique_taux_schema(taux_col)` — la précondition de
        `ajouter_taux_en_vigueur`.
    """
    return (
        pl.scan_csv(_CONFIG_DIR / nom_fichier)
        .with_columns(pl.col("start").str.to_datetime().dt.replace_time_zone("Europe/Paris"))
        .with_columns(pl.col(taux_col).cast(pl.Float64))
    )


def ajouter_taux_en_vigueur(
    df: pl.LazyFrame,
    historique: pl.LazyFrame,
    *,
    date_col: str,
    taux_col: str,
) -> pl.LazyFrame:
    """
    Attache à chaque ligne de `df` le taux en vigueur à `df[date_col]`.

    Sémantique de `historique` : chaque ligne représente l'entrée en vigueur
    d'un nouveau taux à `start`, qui remplace le précédent jusqu'à la ligne
    suivante (la dernière s'étend indéfiniment).
    """
    historique_taux_schema(taux_col).validate(historique)

    joint = (
        df.sort(date_col)
        .join_asof(
            historique.sort("start"),
            left_on=date_col,
            right_on="start",
            strategy="backward",
        )
        .drop("start")
    )

    nb_orphelines = joint.filter(pl.col(taux_col).is_null()).select(pl.len()).collect().item()
    if nb_orphelines > 0:
        raise ValueError(
            f"{nb_orphelines} ligne(s) sans taux en vigueur — date(s) antérieure(s) au premier start de l'historique"
        )

    return joint
