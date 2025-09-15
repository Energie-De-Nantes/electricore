"""
Fonctions TURPE pour Polars.

Ce module fournit des fonctions optimisées pour le calcul du TURPE fixe
en utilisant Polars pour des performances supérieures.
"""

import polars as pl
from pathlib import Path
from typing import Optional


def load_turpe_rules_polars() -> pl.LazyFrame:
    """
    Charge les règles TURPE en LazyFrame Polars pour des performances optimisées.

    Returns:
        LazyFrame Polars contenant les règles TURPE avec types correctement définis

    Example:
        >>> regles = load_turpe_rules_polars()
        >>> regles.collect()
    """
    file_path = Path(__file__).parent / "turpe_rules.csv"

    return (
        pl.scan_csv(file_path)
        # Conversion des colonnes de dates avec timezone
        .with_columns([
            pl.col("start").str.to_datetime().dt.replace_time_zone("Europe/Paris"),
            pl.col("end").str.to_datetime().dt.replace_time_zone("Europe/Paris")
        ])
        # Conversion des colonnes numériques avec trimming des espaces
        .with_columns([
            # b peut avoir des espaces, donc trimming
            pl.col("b").str.strip_chars().cast(pl.Float64),
            # cg et cc sont déjà des nombres
            pl.col("cg").cast(pl.Float64),
            pl.col("cc").cast(pl.Float64)
        ])
    )


def expr_calculer_turpe_fixe() -> list[pl.Expr]:
    """
    Expressions pour calculer le TURPE fixe.

    Ces expressions calculent :
    - TURPE fixe annuel = (b * Puissance_Souscrite) + cg + cc
    - TURPE fixe journalier = TURPE fixe annuel / 365
    - TURPE fixe = TURPE fixe journalier * nb_jours

    Returns:
        Liste d'expressions Polars pour le calcul du TURPE

    Example:
        >>> df.with_columns(expr_calculer_turpe_fixe())
    """
    # Calcul direct du TURPE fixe journalier et final
    turpe_annuel = (
        (pl.col("b") * pl.col("puissance_souscrite")) +
        pl.col("cg") +
        pl.col("cc")
    )

    return [
        # TURPE fixe annuel
        turpe_annuel.alias("turpe_fixe_annuel"),

        # TURPE fixe journalier
        (turpe_annuel / 365).alias("turpe_fixe_journalier"),

        # TURPE fixe pour la période
        ((turpe_annuel / 365) * pl.col("nb_jours"))
        .round(2)
        .alias("turpe_fixe")
    ]


def expr_filtrer_regles_temporelles() -> pl.Expr:
    """
    Expression pour filtrer les règles TURPE applicables temporellement.

    Cette expression vérifie que la date de début de la période
    est comprise dans la validité de la règle TURPE.

    Returns:
        Expression booléenne pour filtrer les règles applicables

    Example:
        >>> df_joint.filter(expr_filtrer_regles_temporelles())
    """
    return (
        (pl.col("debut") >= pl.col("start")) &
        (
            pl.col("debut") < pl.col("end").fill_null(
                pl.datetime(2100, 1, 1, time_zone="Europe/Paris")
            )
        )
    )


def ajouter_turpe_fixe_polars(
    periodes: pl.LazyFrame,
    regles: Optional[pl.LazyFrame] = None
) -> pl.LazyFrame:
    """
    Ajoute le calcul du TURPE fixe aux périodes d'abonnement.

    Cette fonction joint les périodes avec les règles TURPE et calcule
    le montant du TURPE fixe pour chaque période.

    Args:
        periodes: LazyFrame des périodes d'abonnement
        regles: LazyFrame des règles TURPE (optionnel, sera chargé si None)

    Returns:
        LazyFrame avec les colonnes TURPE ajoutées

    Example:
        >>> periodes_avec_turpe = ajouter_turpe_fixe_polars(periodes)
        >>> df = periodes_avec_turpe.collect()
    """
    if regles is None:
        regles = load_turpe_rules_polars()

    return (
        periodes
        # Jointure avec les règles TURPE sur la FTA
        .join(
            regles,
            left_on="formule_tarifaire_acheminement",
            right_on="Formule_Tarifaire_Acheminement",
            how="left"
        )

        # Filtrage temporel des règles applicables
        .filter(expr_filtrer_regles_temporelles())

        # Calcul du TURPE fixe
        .with_columns(expr_calculer_turpe_fixe())

        # Sélection des colonnes finales (exclure les colonnes de règles)
        .select([
            # Colonnes originales des périodes
            "ref_situation_contractuelle",
            "pdl",
            "mois_annee",
            "debut_lisible",
            "fin_lisible",
            "formule_tarifaire_acheminement",
            "puissance_souscrite",
            "nb_jours",
            "debut",
            "fin",
            # Colonnes TURPE calculées
            "turpe_fixe_journalier",
            "turpe_fixe"
        ])
    )


def pipeline_abonnements_avec_turpe(historique: pl.LazyFrame) -> pl.LazyFrame:
    """
    Pipeline complet pour les abonnements avec TURPE fixe.

    Cette fonction orchestre :
    1. La génération des périodes d'abonnement
    2. L'ajout du TURPE fixe

    Args:
        historique: LazyFrame contenant l'historique enrichi des événements

    Returns:
        LazyFrame avec les périodes d'abonnement enrichies du TURPE fixe

    Example:
        >>> from electricore.core.pipelines_polars.abonnements_polars import generer_periodes_abonnement
        >>> abonnements = (
        ...     historique
        ...     .pipe(generer_periodes_abonnement)
        ...     .pipe(ajouter_turpe_fixe_polars)
        ... )
        >>> df = abonnements.collect()
    """
    from ..pipelines_polars.abonnements_polars import generer_periodes_abonnement

    return (
        historique
        .pipe(generer_periodes_abonnement)
        .pipe(ajouter_turpe_fixe_polars)
    )


# =============================================================================
# FONCTIONS DE COMPATIBILITÉ ET VALIDATION
# =============================================================================

def valider_regles_turpe(regles: pl.LazyFrame) -> pl.LazyFrame:
    """
    Valide les règles TURPE et détecte les doublons.

    Args:
        regles: LazyFrame des règles TURPE

    Returns:
        LazyFrame validé

    Raises:
        ValueError: Si des doublons sont détectés sur la FTA
    """
    # Vérifier les doublons (à implémenter si nécessaire)
    # Pour l'instant, on retourne les règles telles quelles
    return regles


def convertir_vers_pandas(lf: pl.LazyFrame):
    """
    Convertit un LazyFrame Polars vers pandas pour compatibilité.

    Cette fonction peut être utilisée pour la transition ou les tests.

    Args:
        lf: LazyFrame Polars à convertir

    Returns:
        DataFrame pandas équivalent
    """
    return lf.collect().to_pandas()