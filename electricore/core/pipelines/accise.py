"""
Expressions Polars pour le calcul de l'Accise sur l'électricité.

Ce module unifie toute la logique Accise en suivant l'architecture
fonctionnelle Polars avec des expressions composables et des pipelines optimisés.

L'Accise est calculée sur la base de la consommation mensuelle en appliquant
les taux réglementaires en vigueur selon la période.
"""

from pathlib import Path

import polars as pl

from electricore.core.pipelines.taux import ajouter_taux_en_vigueur

# =============================================================================
# CHARGEMENT DES RÈGLES ACCISE
# =============================================================================


def load_accise_rules() -> pl.LazyFrame:
    """
    Charge l'historique des taux Accise depuis `electricore/config/accise_rules.csv`.

    Returns:
        LazyFrame avec colonnes `start` (datetime Europe/Paris) et
        `taux_accise_eur_mwh` (Float64). Chaque ligne représente l'entrée en
        vigueur d'un nouveau taux qui remplace le précédent.
    """
    file_path = Path(__file__).parent.parent.parent / "config" / "accise_rules.csv"

    return (
        pl.scan_csv(file_path)
        .with_columns(pl.col("start").str.to_datetime().dt.replace_time_zone("Europe/Paris"))
        .with_columns(pl.col("taux_accise_eur_mwh").cast(pl.Float64))
    )


# =============================================================================
# EXPRESSIONS DE PRÉPARATION DES DONNÉES
# =============================================================================


def expr_calculer_mois_consommation() -> pl.Expr:
    """
    Expression pour calculer le mois de consommation à partir de invoice_date.

    Applique un décalage de -1 mois car la facture du mois M concerne
    les consommations du mois M-1.

    Returns:
        Expression Polars retournant le mois de consommation au format "YYYY-MM"

    Example:
        >>> df.with_columns(expr_calculer_mois_consommation().alias('mois_consommation'))
    """
    # Parser invoice_date, décaler -1 mois, formater en YYYY-MM
    date_conso = pl.col("invoice_date").str.to_date("%Y-%m-%d").dt.offset_by("-1mo")
    return date_conso.dt.strftime("%Y-%m")


def expr_calculer_trimestre_consommation() -> pl.Expr:
    """
    Expression pour calculer le trimestre de consommation à partir de invoice_date.

    Applique un décalage de -1 mois car la facture du mois M concerne
    les consommations du mois M-1.

    Returns:
        Expression Polars retournant le trimestre au format "YYYY-TX"

    Example:
        >>> df.with_columns(expr_calculer_trimestre_consommation().alias('trimestre'))
    """
    # Parser invoice_date et décaler -1 mois
    date_conso = pl.col("invoice_date").str.to_date("%Y-%m-%d").dt.offset_by("-1mo")

    # Extraire année et calculer trimestre
    annee = date_conso.dt.year().cast(pl.Utf8)
    quarter = ((date_conso.dt.month() - 1) // 3 + 1).cast(pl.Utf8)

    return annee + pl.lit("-T") + quarter


def agreger_consommations_mensuelles(lignes_factures: pl.LazyFrame) -> pl.LazyFrame:
    """
    Agrège les lignes de factures Odoo par PDL et mois de consommation.

    Filtre sur les catégories d'énergie (Base, HP, HC) et agrège la quantité
    totale par PDL et mois de consommation.

    Args:
        lignes_factures: LazyFrame issu de commandes_lignes() ou équivalent

    Returns:
        LazyFrame agrégé avec colonnes: pdl, order_name, mois_consommation,
        trimestre, energie_kwh

    Example:
        >>> from electricore.core.loaders import commandes_lignes
        >>> lignes = commandes_lignes(odoo).collect().lazy()
        >>> consos = agreger_consommations_mensuelles(lignes)
    """
    return (
        lignes_factures
        # Filtrer sur les catégories d'énergie
        .filter(pl.col("name_product_category").is_in(["Base", "HP", "HC"]))
        # Calculer mois et trimestre de consommation
        .with_columns(
            [
                expr_calculer_mois_consommation().alias("mois_consommation"),
                expr_calculer_trimestre_consommation().alias("trimestre"),
            ]
        )
        # Agréger par PDL et mois de consommation
        .group_by(["x_pdl", "mois_consommation", "trimestre"])
        .agg([pl.col("quantity").sum().alias("energie_kwh"), pl.col("name").first().alias("order_name")])
        # Renommer x_pdl en pdl
        .rename({"x_pdl": "pdl"})
    )


# =============================================================================
# PIPELINE PRINCIPAL
# =============================================================================


def ajouter_accise(consommations: pl.LazyFrame, regles: pl.LazyFrame | None = None) -> pl.LazyFrame:
    """
    Ajoute le taux d'Accise en vigueur et le montant calculé aux consommations.

    Args:
        consommations: LazyFrame avec au moins `mois_consommation` (str "YYYY-MM")
            et `energie_kwh` (Float64).
        regles: Historique des taux Accise. Chargé via `load_accise_rules()` si None.

    Returns:
        LazyFrame d'entrée enrichi de `taux_accise_eur_mwh`, `energie_mwh`,
        `accise_eur` (arrondi à 2 décimales).
    """
    if regles is None:
        regles = load_accise_rules()

    colonnes_originales = consommations.collect_schema().names()

    # `mois_consommation` est une string "YYYY-MM" ; conversion en datetime TZ Paris
    # pour matcher la précondition de `ajouter_taux_en_vigueur`.
    consommations_datees = consommations.with_columns(
        pl.col("mois_consommation")
        .str.to_datetime("%Y-%m")
        .dt.replace_time_zone("Europe/Paris")
        .alias("_date_taux")
    )

    return (
        ajouter_taux_en_vigueur(
            consommations_datees,
            regles,
            date_col="_date_taux",
            taux_col="taux_accise_eur_mwh",
        )
        .with_columns(
            [
                (pl.col("energie_kwh") / 1000).alias("energie_mwh"),
                ((pl.col("energie_kwh") / 1000) * pl.col("taux_accise_eur_mwh")).round(2).alias("accise_eur"),
            ]
        )
        .select([*colonnes_originales, "taux_accise_eur_mwh", "energie_mwh", "accise_eur"])
    )


def pipeline_accise(lignes_factures: pl.LazyFrame, regles: pl.LazyFrame | None = None) -> pl.DataFrame:
    """
    Pipeline complet de calcul de l'Accise depuis les lignes de factures.

    Pipeline complet :
    1. Filtrage sur les catégories d'énergie (Base, HP, HC)
    2. Agrégation par PDL et mois de consommation
    3. Jointure avec les règles Accise
    4. Calcul de l'Accise selon le taux applicable

    Args:
        lignes_factures: LazyFrame issu de commandes_lignes() ou équivalent
        regles: LazyFrame des règles Accise (optionnel, sera chargé si None)

    Returns:
        DataFrame avec toutes les consommations et leur Accise calculée

    Example:
        >>> from electricore.core.loaders import OdooReader, commandes_lignes
        >>> from electricore.core.pipelines.accise import pipeline_accise
        >>>
        >>> with OdooReader(config) as odoo:
        ...     lignes = commandes_lignes(odoo).collect().lazy()
        ...     df_accise = pipeline_accise(lignes)
    """
    # Étape 1 : Agrégation mensuelle
    consos_mensuelles = agreger_consommations_mensuelles(lignes_factures)

    # Étape 2 : Calcul de l'Accise
    consos_avec_accise = ajouter_accise(consos_mensuelles, regles)

    # Étape 3 : Collecte et tri
    return consos_avec_accise.sort(["pdl", "mois_consommation"]).collect()


# Export des fonctions principales
__all__ = [
    "load_accise_rules",
    "expr_calculer_mois_consommation",
    "expr_calculer_trimestre_consommation",
    "agreger_consommations_mensuelles",
    "ajouter_accise",
    "pipeline_accise",
]
