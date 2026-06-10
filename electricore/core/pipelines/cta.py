"""
Calcul de la CTA (Contribution Tarifaire d'Acheminement).

La CTA est une taxe assise sur la part fixe du TURPE. Son taux est fixé par
arrêté ministériel (avec avis de la CRE) et peut changer en cours d'année,
parfois en milieu de trimestre.

Les règles sont chargées depuis electricore/config/cta_rules.csv qui définit
les plages de validité de chaque taux (format identique à accise_rules.csv).

Formule : cta_eur = turpe_fixe_eur × taux_cta_pct / 100
"""

from pathlib import Path

import polars as pl

from electricore.core.pipelines.taux import ajouter_taux_en_vigueur

# =============================================================================
# CHARGEMENT DES RÈGLES CTA
# =============================================================================


def load_cta_rules() -> pl.LazyFrame:
    """
    Charge l'historique des taux CTA depuis `electricore/config/cta_rules.csv`.

    Returns:
        LazyFrame avec colonnes `start` (datetime Europe/Paris) et
        `taux_cta_pct` (Float64). Chaque ligne représente l'entrée en vigueur
        d'un nouveau taux qui remplace le précédent.
    """
    file_path = Path(__file__).parent.parent.parent / "config" / "cta_rules.csv"

    return (
        pl.scan_csv(file_path)
        .with_columns(pl.col("start").str.to_datetime().dt.replace_time_zone("Europe/Paris"))
        .with_columns(pl.col("taux_cta_pct").cast(pl.Float64))
    )


# =============================================================================
# AJOUT DU TAUX ET DU MONTANT CTA
# =============================================================================


def ajouter_cta(
    df_facturation_mensuel: pl.LazyFrame,
    regles: pl.LazyFrame | None = None,
) -> pl.LazyFrame:
    """
    Ajoute `taux_cta_pct` et `cta_eur` à chaque ligne de facturation mensuelle.

    Args:
        df_facturation_mensuel: LazyFrame contenant au minimum `debut`
            (datetime Europe/Paris, typiquement le 1er du mois) et
            `turpe_fixe_eur`.
        regles: Historique des taux CTA. Chargé via `load_cta_rules()` si None.

    Returns:
        LazyFrame d'entrée enrichi de `taux_cta_pct` et `cta_eur`
        (arrondi à 2 décimales).
    """
    if regles is None:
        regles = load_cta_rules()

    colonnes_originales = df_facturation_mensuel.collect_schema().names()

    return (
        ajouter_taux_en_vigueur(
            df_facturation_mensuel,
            regles,
            date_col="debut",
            taux_col="taux_cta_pct",
        )
        .with_columns((pl.col("turpe_fixe_eur") * pl.col("taux_cta_pct") / 100).round(2).alias("cta_eur"))
        .select([*colonnes_originales, "taux_cta_pct", "cta_eur"])
    )


__all__ = [
    "load_cta_rules",
    "ajouter_cta",
]
