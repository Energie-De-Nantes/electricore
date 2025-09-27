"""
Expressions Polars pour le calcul du TURPE (Tarif d'Utilisation des Réseaux Publics d'Électricité).

Ce module unifie toute la logique TURPE (fixe et variable) en suivant l'architecture
fonctionnelle Polars avec des expressions composables et des pipelines optimisés.

Le TURPE se décompose en :
- TURPE fixe : appliqué aux périodes d'abonnement (cg + cc + b*puissance)
- TURPE variable : appliqué aux périodes d'énergie (tarifs par cadran horaire)
"""

import polars as pl
from pathlib import Path
from typing import List, Optional


# =============================================================================
# CHARGEMENT DES RÈGLES TURPE
# =============================================================================

def load_turpe_rules() -> pl.LazyFrame:
    """
    Charge les règles tarifaires TURPE depuis le fichier CSV.

    Returns:
        LazyFrame Polars contenant toutes les règles TURPE avec types correctement définis

    Example:
        >>> regles = load_turpe_rules()
        >>> regles.collect()
    """
    file_path = Path(__file__).parent.parent.parent / "config" / "turpe_rules.csv"

    return (
        # Lire le CSV en forçant toutes les colonnes numériques comme string pour éviter les erreurs de parsing
        pl.scan_csv(
            file_path,
            schema_overrides={
                "cg": str, "cc": str, "b": str,
                "hph": str, "hch": str, "hpb": str, "hcb": str,
                "hp": str, "hc": str, "base": str
            }
        )
        # Conversion des colonnes de dates avec timezone Europe/Paris
        .with_columns([
            pl.col("start").str.to_datetime().dt.replace_time_zone("Europe/Paris"),
            pl.col("end").str.to_datetime().dt.replace_time_zone("Europe/Paris")
        ])
        # Conversion des colonnes numériques avec nettoyage des espaces
        .with_columns([
            # Composantes fixes (€/an)
            pl.col("b").str.strip_chars().cast(pl.Float64),
            pl.col("cg").str.strip_chars().cast(pl.Float64),
            pl.col("cc").str.strip_chars().cast(pl.Float64),

            # Tarifs variables par cadran (€/MWh)
            pl.col("hph").str.strip_chars().cast(pl.Float64),
            pl.col("hch").str.strip_chars().cast(pl.Float64),
            pl.col("hpb").str.strip_chars().cast(pl.Float64),
            pl.col("hcb").str.strip_chars().cast(pl.Float64),
            pl.col("hp").str.strip_chars().cast(pl.Float64),
            pl.col("hc").str.strip_chars().cast(pl.Float64),
            pl.col("base").str.strip_chars().cast(pl.Float64),
        ])
    )


# =============================================================================
# EXPRESSIONS TURPE FIXE (ABONNEMENTS)
# =============================================================================

def expr_calculer_turpe_fixe_annuel() -> pl.Expr:
    """
    Expression pour calculer le TURPE fixe annuel.

    Formule : (b * Puissance_Souscrite) + cg + cc

    Returns:
        Expression Polars retournant le TURPE fixe annuel en €

    Example:
        >>> df.with_columns(expr_calculer_turpe_fixe_annuel().alias("turpe_fixe_annuel"))
    """
    return (
        (pl.col("b") * pl.col("puissance_souscrite")) +
        pl.col("cg") +
        pl.col("cc")
    )


def expr_calculer_turpe_fixe_journalier() -> pl.Expr:
    """
    Expression pour calculer le TURPE fixe journalier.

    Formule : turpe_fixe_annuel / 365

    Returns:
        Expression Polars retournant le TURPE fixe journalier en €

    Example:
        >>> df.with_columns(expr_calculer_turpe_fixe_journalier().alias("turpe_fixe_journalier"))
    """
    return expr_calculer_turpe_fixe_annuel() / 365


def expr_calculer_turpe_fixe_periode() -> pl.Expr:
    """
    Expression pour calculer le TURPE fixe pour une période donnée.

    Formule : turpe_fixe_journalier * nb_jours

    Returns:
        Expression Polars retournant le TURPE fixe pour la période en €

    Example:
        >>> df.with_columns(expr_calculer_turpe_fixe_periode().alias("turpe_fixe"))
    """
    return (expr_calculer_turpe_fixe_journalier() * pl.col("nb_jours")).round(2)




# =============================================================================
# EXPRESSIONS TURPE VARIABLE (ÉNERGIE)
# =============================================================================

def expr_calculer_turpe_cadran(cadran: str) -> pl.Expr:
    """
    Expression pour calculer la contribution TURPE variable d'un cadran.

    Formule : energie_cadran * taux_turpe_cadran / 100
    Le /100 convertit les €/MWh en €/kWh comme requis par les règles tarifaires.

    Args:
        cadran: Nom du cadran (ex: "base", "hp", "hc", "hph", "hch", "hpb", "hcb")

    Returns:
        Expression Polars retournant la contribution TURPE du cadran en €

    Example:
        >>> df.with_columns(expr_calculer_turpe_cadran("base").alias("turpe_base"))
    """
    energie_col = f"{cadran}_energie"
    tarif_col = f"taux_turpe_{cadran}"  # Utiliser le nom préfixé pour éviter les collisions

    return (
        pl.when(pl.col(energie_col).is_not_null() & pl.col(tarif_col).is_not_null())
        .then((pl.col(energie_col) * pl.col(tarif_col) / 100))
        .otherwise(pl.lit(0.0))
    )


def expr_calculer_turpe_contributions_cadrans() -> List[pl.Expr]:
    """
    Expressions pour calculer les contributions TURPE de chaque cadran.

    Returns:
        Liste d'expressions pour les contributions individuelles

    Example:
        >>> df.with_columns(expr_calculer_turpe_contributions_cadrans())
    """
    cadrans = ["hph", "hch", "hpb", "hcb", "hp", "hc", "base"]

    return [
        expr_calculer_turpe_cadran(cadran).alias(f"turpe_{cadran}")
        for cadran in cadrans
    ]


def expr_sommer_turpe_cadrans() -> pl.Expr:
    """
    Expression pour sommer les contributions TURPE de tous les cadrans.

    Utilise sum_horizontal pour additionner toutes les colonnes turpe_*

    Returns:
        Expression Polars retournant la somme des contributions TURPE

    Example:
        >>> df.with_columns(expr_sommer_turpe_cadrans().alias("turpe_variable"))
    """
    cadrans = ["hph", "hch", "hpb", "hcb", "hp", "hc", "base"]
    contributions_cols = [f"turpe_{cadran}" for cadran in cadrans]

    return (
        pl.sum_horizontal([pl.col(col) for col in contributions_cols])
        .round(2)
    )


# =============================================================================
# EXPRESSIONS DE FILTRAGE TEMPOREL (COMMUNES)
# =============================================================================

def expr_filtrer_regles_temporelles() -> pl.Expr:
    """
    Expression pour filtrer les règles TURPE applicables temporellement.

    Cette expression vérifie que la date de début de la période est comprise
    dans la plage de validité de la règle TURPE (start <= debut < end).
    Les règles sans date de fin (end=null) sont considérées comme valides
    jusqu'en 2100.

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


def valider_regles_presentes(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Valide que toutes les FTA ont des règles TURPE correspondantes.

    Args:
        lf: LazyFrame joint avec les règles TURPE

    Returns:
        LazyFrame validé (sans les lignes avec règles manquantes)

    Raises:
        ValueError: Si des FTA n'ont pas de règles TURPE correspondantes

    Example:
        >>> df_valide = valider_regles_presentes(df_joint)
    """
    # Vérifier s'il y a des règles manquantes
    fta_manquantes = (
        lf
        .filter(pl.col("start").is_null())
        .select("formule_tarifaire_acheminement")
        .unique()
        .collect()
    )

    if fta_manquantes.shape[0] > 0:
        fta_list = fta_manquantes["formule_tarifaire_acheminement"].to_list()
        raise ValueError(f"❌ Règles TURPE manquantes pour : {fta_list}")

    return lf.filter(pl.col("start").is_not_null())


# =============================================================================
# FONCTIONS PIPELINE D'INTÉGRATION
# =============================================================================

def ajouter_turpe_fixe(
    periodes: pl.LazyFrame,
    regles: Optional[pl.LazyFrame] = None
) -> pl.LazyFrame:
    """
    Ajoute le calcul du TURPE fixe aux périodes d'abonnement.

    Cette fonction joint les périodes avec les règles TURPE et calcule
    le montant du TURPE fixe pour chaque période selon la puissance souscrite.

    Args:
        periodes: LazyFrame des périodes d'abonnement
        regles: LazyFrame des règles TURPE (optionnel, sera chargé si None)

    Returns:
        LazyFrame avec les colonnes TURPE fixe ajoutées

    Example:
        >>> periodes_avec_turpe = ajouter_turpe_fixe(periodes)
        >>> df = periodes_avec_turpe.collect()
    """
    if regles is None:
        regles = load_turpe_rules()

    return (
        periodes
        # Jointure avec les règles TURPE sur la FTA
        .join(
            regles,
            left_on="formule_tarifaire_acheminement",
            right_on="Formule_Tarifaire_Acheminement",
            how="left"
        )

        # Validation des règles présentes
        .pipe(valider_regles_presentes)

        # Filtrage temporel des règles applicables
        .filter(expr_filtrer_regles_temporelles())

        # Calcul du TURPE fixe
        .with_columns(expr_calculer_turpe_fixe_periode().alias("turpe_fixe"))

        # Sélection des colonnes finales (exclure les colonnes de règles intermédiaires)
        .select([
            # Colonnes originales des périodes
            col for col in periodes.collect_schema().names()
        ] + [
            # Colonne TURPE calculée
            "turpe_fixe"
        ])
    )


def ajouter_turpe_variable(
    periodes: pl.LazyFrame,
    regles: Optional[pl.LazyFrame] = None
) -> pl.LazyFrame:
    """
    Ajoute le calcul du TURPE variable aux périodes d'énergie.

    Cette fonction joint les périodes avec les règles TURPE et calcule
    le montant du TURPE variable pour chaque période selon les consommations
    par cadran horaire.

    Args:
        periodes: LazyFrame des périodes d'énergie avec consommations par cadran
        regles: LazyFrame des règles TURPE (optionnel, sera chargé si None)

    Returns:
        LazyFrame avec les colonnes TURPE variable ajoutées

    Example:
        >>> periodes_avec_turpe = ajouter_turpe_variable(periodes)
        >>> df = periodes_avec_turpe.collect()
    """
    if regles is None:
        regles = load_turpe_rules()

    # Récupérer la liste des colonnes originales
    colonnes_originales = periodes.collect_schema().names()

    return (
        periodes
        # Jointure avec les règles TURPE sur la FTA (préfixer les tarifs pour éviter collisions)
        .join(
            regles.rename({
                col: f"taux_turpe_{col}" for col in ["base", "hp", "hc", "hph", "hch", "hpb", "hcb"]
            }),
            left_on="formule_tarifaire_acheminement",
            right_on="Formule_Tarifaire_Acheminement",
            how="left"
        )

        # Validation des règles présentes
        .pipe(valider_regles_presentes)

        # Filtrage temporel des règles applicables
        .filter(expr_filtrer_regles_temporelles())

        # Calcul des contributions TURPE variable par cadran
        .with_columns(expr_calculer_turpe_contributions_cadrans())

        # Calcul du total TURPE variable
        .with_columns(expr_sommer_turpe_cadrans().alias("turpe_variable"))

        # Sélection des colonnes finales (exclure les colonnes de règles intermédiaires)
        .select([
            # Colonnes originales des périodes
            *colonnes_originales,
            # Colonnes TURPE calculées
            "turpe_variable"
        ])
    )


# =============================================================================
# FONCTIONS DE VALIDATION ET DEBUGGING
# =============================================================================

def debug_turpe_variable(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Ajoute des colonnes de debug pour analyser le calcul TURPE variable.

    Cette fonction est utile pour diagnostiquer les problèmes de calcul
    en exposant les valeurs intermédiaires par cadran.

    Args:
        lf: LazyFrame avec calculs TURPE

    Returns:
        LazyFrame avec colonnes de debug

    Example:
        >>> df_debug = debug_turpe_variable(df)
        >>> print(df_debug.collect())
    """
    cadrans = ["hph", "hch", "hpb", "hcb", "hp", "hc", "base"]
    debug_expressions = []

    # Ajouter les détails par cadran
    for cadran in cadrans:
        energie_col = f"{cadran}_energie"
        debug_expressions.extend([
            pl.col(energie_col).alias(f"debug_energie_{cadran}"),
            pl.col(f"taux_turpe_{cadran}").alias(f"debug_tarif_{cadran}"),
            (pl.col(energie_col) * pl.col(f"taux_turpe_{cadran}") / 100).alias(f"debug_contribution_{cadran}")
        ])

    return lf.with_columns(debug_expressions)


def comparer_avec_pandas(lf: pl.LazyFrame, df_pandas) -> dict:
    """
    Compare les résultats Polars avec pandas pour la validation de migration.

    Cette fonction aide à valider que la migration Polars produit
    les mêmes résultats que l'implémentation pandas existante.

    Args:
        lf: LazyFrame Polars avec résultats
        df_pandas: DataFrame pandas avec résultats de référence

    Returns:
        Dictionnaire avec statistiques de comparaison

    Example:
        >>> stats = comparer_avec_pandas(lf, df_pandas)
        >>> print(f"Différence moyenne TURPE: {stats['turpe_variable_diff_moyenne']}")
    """
    df = lf.collect().to_pandas()

    # Comparer les colonnes communes
    colonnes_communes = set(df.columns) & set(df_pandas.columns)

    stats = {}
    for col in colonnes_communes:
        if df[col].dtype in ['float64', 'int64'] and df_pandas[col].dtype in ['float64', 'int64']:
            diff = abs(df[col] - df_pandas[col])
            stats[f"{col}_diff_max"] = diff.max()
            stats[f"{col}_diff_moyenne"] = diff.mean()

    return stats