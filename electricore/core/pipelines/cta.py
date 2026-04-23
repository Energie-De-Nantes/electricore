"""
Pipeline de calcul de la CTA (Contribution Tarifaire d'Acheminement).

La CTA est une taxe assise sur la part fixe du TURPE. Son taux est fixé par
arrêté ministériel (avec avis de la CRE) et peut changer en cours d'année,
parfois en milieu de trimestre.

Les règles sont chargées depuis electricore/config/cta_rules.csv qui définit
les plages de validité de chaque taux (format identique à accise_rules.csv).

Le calcul se fait au niveau mensuel (granularité de df_facturation) puis
est agrégé par PDL. Cette granularité mensuelle gère automatiquement un
changement de taux en cours de trimestre : chaque mois reçoit le taux
applicable au 1er du mois, puis la somme mensuelle donne le total du
trimestre.

Formule : cta_eur = turpe_fixe_eur × taux_cta_pct / 100
"""

import polars as pl
from pathlib import Path
from typing import Optional

from electricore.core.pipelines.facturation import expr_calculer_trimestre


# =============================================================================
# CHARGEMENT DES RÈGLES CTA
# =============================================================================

def load_cta_rules() -> pl.LazyFrame:
    """
    Charge les règles CTA depuis electricore/config/cta_rules.csv.

    Returns:
        LazyFrame avec colonnes : start (datetime Europe/Paris),
        end (datetime Europe/Paris, nullable), taux_cta_pct (Float64).
    """
    file_path = Path(__file__).parent.parent.parent / "config" / "cta_rules.csv"

    return (
        pl.scan_csv(file_path)
        .with_columns([
            pl.col("start").str.to_datetime().dt.replace_time_zone("Europe/Paris"),
            pl.col("end").str.to_datetime().dt.replace_time_zone("Europe/Paris"),
        ])
        .with_columns(pl.col("taux_cta_pct").cast(pl.Float64))
    )


# =============================================================================
# EXPRESSIONS DE FILTRAGE TEMPOREL
# =============================================================================

def expr_filtrer_regles_temporelles() -> pl.Expr:
    """
    Filtre les règles CTA applicables pour une ligne de facturation mensuelle.

    Vérifie que `debut` (1er du mois) tombe dans la plage de validité :
    `start <= debut < end`, où `end=null` est traité comme « sans fin ».
    """
    return (
        (pl.col("debut") >= pl.col("start")) &
        (
            pl.col("debut") < pl.col("end").fill_null(
                pl.datetime(2100, 1, 1, time_zone="Europe/Paris")
            )
        )
    )


# =============================================================================
# AJOUT DU TAUX ET DU MONTANT CTA
# =============================================================================

def ajouter_cta(
    df_facturation_mensuel: pl.LazyFrame,
    regles: Optional[pl.LazyFrame] = None,
) -> pl.LazyFrame:
    """
    Ajoute `taux_cta_pct` et `cta_eur` à chaque ligne de facturation mensuelle.

    Args:
        df_facturation_mensuel: LazyFrame contenant au minimum `debut`
            (datetime Europe/Paris, typiquement le 1er du mois) et
            `turpe_fixe_eur`.
        regles: LazyFrame des règles CTA. Chargé via `load_cta_rules()`
            si None.

    Returns:
        LazyFrame identique en entrée, enrichi de `taux_cta_pct` et
        `cta_eur` (arrondi à 2 décimales).
    """
    if regles is None:
        regles = load_cta_rules()

    colonnes_originales = df_facturation_mensuel.collect_schema().names()

    facturation_triee = df_facturation_mensuel.sort("debut")
    regles_triees = regles.collect().sort("start").lazy()

    return (
        facturation_triee
        .join_asof(
            regles_triees,
            left_on="debut",
            right_on="start",
            strategy="backward",
        )
        .filter(pl.col("start").is_not_null())
        .filter(expr_filtrer_regles_temporelles())
        .with_columns(
            (pl.col("turpe_fixe_eur") * pl.col("taux_cta_pct") / 100)
            .round(2)
            .alias("cta_eur")
        )
        .select([*colonnes_originales, "taux_cta_pct", "cta_eur"])
    )


# =============================================================================
# PIPELINE AGRÉGÉ PAR PDL
# =============================================================================

def pipeline_cta(
    df_facturation: pl.DataFrame,
    df_pdl: pl.DataFrame,
    trimestre: str | None = None,
    regles: Optional[pl.LazyFrame] = None,
) -> pl.DataFrame:
    """
    Calcule la CTA par PDL sur la période choisie.

    Les taux sont lus depuis `cta_rules.csv` et appliqués au niveau mensuel
    puis sommés. Un changement de taux en milieu de trimestre est donc
    géré correctement (ex. janvier 2026 à 21,93 % + février-mars 2026 à 15 %).

    Args:
        df_facturation: Sortie de `facturation().facturation`. Colonnes
            requises : pdl, debut (datetime Europe/Paris), turpe_fixe_eur.
        df_pdl: Mapping pdl → order_name (typiquement depuis Odoo).
            Colonnes requises : pdl, order_name.
        trimestre: Filtre optionnel au format "YYYY-TX" (ex. "2026-T1").
        regles: LazyFrame des règles CTA (optionnel, utile pour les tests).

    Returns:
        DataFrame trié par cta décroissant avec les colonnes :
        pdl, order_name, turpe_fixe_total, cta, taux_cta_appliques.
        `taux_cta_appliques` est la liste triée des taux distincts
        rencontrés dans la période (plus d'une valeur ⇒ changement de taux).
    """
    lf_jointure = (
        df_facturation
        .join(df_pdl.select(["pdl", "order_name"]), on="pdl", how="inner")
        .lazy()
    )
    lf = ajouter_cta(lf_jointure, regles).with_columns(
        expr_calculer_trimestre().alias("trimestre")
    )

    if trimestre is not None:
        lf = lf.filter(pl.col("trimestre") == trimestre)

    return (
        lf
        .group_by("pdl")
        .agg([
            pl.col("order_name").first(),
            pl.col("turpe_fixe_eur").sum().alias("turpe_fixe_total"),
            pl.col("cta_eur").sum().round(2).alias("cta"),
            pl.col("taux_cta_pct").unique().sort().alias("taux_cta_appliques"),
        ])
        .sort("cta", descending=True)
        .collect()
    )


__all__ = [
    "load_cta_rules",
    "expr_filtrer_regles_temporelles",
    "ajouter_cta",
    "pipeline_cta",
]
