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
                # Composantes fixes
                "cg": str, "cc": str, "b": str,
                # Coefficients puissance C4 (€/kVA/an)
                "b_hph": str, "b_hch": str, "b_hpb": str, "b_hcb": str,
                # Coefficients énergie C4 (c€/kWh)
                "c_hph": str, "c_hch": str, "c_hpb": str, "c_hcb": str,
                # Coefficients énergie C5 (c€/kWh)
                "c_hp": str, "c_hc": str, "c_base": str,
                # Composante dépassement
                "cmdps": str
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

            # Coefficients puissance C4 (€/kVA/an)
            pl.col("b_hph").str.strip_chars().cast(pl.Float64),
            pl.col("b_hch").str.strip_chars().cast(pl.Float64),
            pl.col("b_hpb").str.strip_chars().cast(pl.Float64),
            pl.col("b_hcb").str.strip_chars().cast(pl.Float64),

            # Coefficients énergie C4 (c€/kWh)
            pl.col("c_hph").str.strip_chars().cast(pl.Float64),
            pl.col("c_hch").str.strip_chars().cast(pl.Float64),
            pl.col("c_hpb").str.strip_chars().cast(pl.Float64),
            pl.col("c_hcb").str.strip_chars().cast(pl.Float64),

            # Coefficients énergie C5 (c€/kWh)
            pl.col("c_hp").str.strip_chars().cast(pl.Float64),
            pl.col("c_hc").str.strip_chars().cast(pl.Float64),
            pl.col("c_base").str.strip_chars().cast(pl.Float64),

            # Composante dépassement (€/h)
            pl.col("cmdps").str.strip_chars().cast(pl.Float64),
        ])
    )


# =============================================================================
# EXPRESSIONS TURPE FIXE (ABONNEMENTS)
# =============================================================================

def expr_calculer_turpe_fixe_annuel() -> pl.Expr:
    """
    Expression pour calculer le TURPE fixe annuel avec détection automatique C4/C5.

    Formule C5 (BT ≤ 36 kVA) : (b × P) + cg + cc (par défaut)
    Formule C4 (BT > 36 kVA) : b_hph×P₁ + b_hch×(P₂-P₁) + b_hpb×(P₃-P₂) + b_hcb×(P₄-P₃) + cg + cc

    La détection se fait sur la présence des 4 coefficients C4 (b_hph, b_hch, b_hpb, b_hcb).
    Par défaut, le calcul C5 est appliqué.

    Returns:
        Expression Polars retournant le TURPE fixe annuel en €

    Example:
        >>> # C5 : une seule puissance souscrite
        >>> df.with_columns(expr_calculer_turpe_fixe_annuel().alias("turpe_fixe_annuel"))
        >>> # C4 : 4 puissances souscrites (hph, hch, hpb, hcb)
        >>> df.with_columns(expr_calculer_turpe_fixe_annuel().alias("turpe_fixe_annuel"))
    """
    # Calcul C5 (par défaut) : (b × P) + cg + cc
    turpe_c5 = (
        (pl.col("b") * pl.col("puissance_souscrite_kva")) +
        pl.col("cg") +
        pl.col("cc")
    )

    # Calcul C4 : b₁×P₁ + b₂×(P₂-P₁) + b₃×(P₃-P₂) + b₄×(P₄-P₃) + cg + cc
    turpe_c4 = (
        (pl.col("b_hph") * pl.col("puissance_souscrite_hph_kva")) +
        (pl.col("b_hch") * (pl.col("puissance_souscrite_hch_kva") - pl.col("puissance_souscrite_hph_kva"))) +
        (pl.col("b_hpb") * (pl.col("puissance_souscrite_hpb_kva") - pl.col("puissance_souscrite_hch_kva"))) +
        (pl.col("b_hcb") * (pl.col("puissance_souscrite_hcb_kva") - pl.col("puissance_souscrite_hpb_kva"))) +
        pl.col("cg") +
        pl.col("cc")
    )

    # Détection C4 : tous les coefficients b_* doivent être non-NULL
    is_c4 = (
        pl.col("b_hph").is_not_null() &
        pl.col("b_hch").is_not_null() &
        pl.col("b_hpb").is_not_null() &
        pl.col("b_hcb").is_not_null()
    )

    # Si C4 → calcul C4, sinon (défaut) → calcul C5
    return pl.when(is_c4).then(turpe_c4).otherwise(turpe_c5)


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


def expr_valider_puissances_croissantes_c4() -> pl.Expr:
    """
    Expression pour valider la contrainte réglementaire C4 : P₁ ≤ P₂ ≤ P₃ ≤ P₄.

    Vérifie que les 4 puissances souscrites C4 respectent l'ordre croissant imposé
    par la CRE (Délibération 2025-40, p.155).

    Returns:
        Expression booléenne : True si contrainte respectée, False sinon

    Example:
        >>> df.filter(expr_valider_puissances_croissantes_c4())  # Garder lignes valides
        >>> df.with_columns(
        ...     expr_valider_puissances_croissantes_c4().alias("puissances_valides")
        ... )
    """
    return (
        (pl.col("puissance_souscrite_hph_kva") <= pl.col("puissance_souscrite_hch_kva")) &
        (pl.col("puissance_souscrite_hch_kva") <= pl.col("puissance_souscrite_hpb_kva")) &
        (pl.col("puissance_souscrite_hpb_kva") <= pl.col("puissance_souscrite_hcb_kva"))
    )




# =============================================================================
# EXPRESSIONS TURPE VARIABLE (ÉNERGIE)
# =============================================================================

def expr_calculer_turpe_cadran(cadran: str) -> pl.Expr:
    """
    Expression pour calculer la contribution TURPE variable d'un cadran.

    Formule : energie_cadran * c_cadran / 100
    Le /100 convertit les c€/kWh (centimes) en €/kWh comme requis.

    Args:
        cadran: Nom du cadran (ex: "base", "hp", "hc", "hph", "hch", "hpb", "hcb")

    Returns:
        Expression Polars retournant la contribution TURPE du cadran en €

    Example:
        >>> df.with_columns(expr_calculer_turpe_cadran("base").alias("turpe_base"))
    """
    energie_col = f"energie_{cadran}_kwh"
    tarif_col = f"c_{cadran}"  # Nomenclature CRE officielle

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


def expr_calculer_composante_depassement() -> pl.Expr:
    """
    Expression pour calculer le coût des pénalités de dépassement (C4 uniquement).

    Cette composante s'applique aux points BT > 36 kVA (C4) qui dépassent leur
    puissance souscrite. Le calcul est basé sur la durée totale de dépassement
    (tous cadrans confondus) et le tarif CMDPS (Composante Mensuelle de Dépassement
    de Puissance Souscrite).

    Formule : depassement_puissance_h × cmdps

    Returns:
        Expression retournant le coût des pénalités en € (0 si cmdps null/C5 ou durée absente)

    Prérequis:
        - Colonne 'depassement_puissance_h' (heures) - fournie par l'appelant,
          somme des dépassements sur tous les cadrans HPH/HCH/HPB/HCB
        - Colonne 'cmdps' (€/h) - issue de turpe_rules.csv

    Notes:
        - Pour C5 (BT ≤ 36 kVA) : cmdps = NULL → pénalités = 0
        - Pour C4 (BT > 36 kVA) : cmdps défini (ex: 12.41 €/h) → pénalités calculées
        - Si depassement_puissance_h absent ou NULL → pénalités = 0 (pas d'erreur)
        - La responsabilité du calcul des dépassements par cadran incombe à l'appelant

    Example:
        >>> # C4 avec dépassement total de 10h à 12.41 €/h
        >>> df.with_columns(expr_calculer_composante_depassement().alias("turpe_depassement"))
        >>> # → turpe_depassement = 10 × 12.41 = 124.10 €
    """
    return (
        pl.when(
            pl.col('cmdps').is_not_null() &
            pl.col('depassement_puissance_h').is_not_null()
        )
        .then(pl.col('depassement_puissance_h') * pl.col('cmdps'))
        .otherwise(0.0)
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

    # Récupérer la liste des colonnes originales
    colonnes_originales = periodes.collect_schema().names()

    # S'assurer que les colonnes C4 existent (avec NULL par défaut pour C5)
    colonnes_c4 = ["puissance_souscrite_hph_kva", "puissance_souscrite_hch_kva",
                   "puissance_souscrite_hpb_kva", "puissance_souscrite_hcb_kva"]

    for col in colonnes_c4:
        if col not in colonnes_originales:
            periodes = periodes.with_columns(pl.lit(None, dtype=pl.Float64).alias(col))
            colonnes_originales.append(col)

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
        .with_columns(expr_calculer_turpe_fixe_periode().alias("turpe_fixe_eur"))

        # Sélection des colonnes finales (exclure les colonnes de règles intermédiaires)
        .select([
            # Colonnes originales des périodes
            *colonnes_originales,
            # Colonne TURPE calculée
            "turpe_fixe_eur"
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

    # S'assurer que depassement_puissance_h existe (pour C5 qui n'en a pas besoin)
    if "depassement_puissance_h" not in colonnes_originales:
        periodes = periodes.with_columns(pl.lit(None, dtype=pl.Float64).alias("depassement_puissance_h"))
        colonnes_originales.append("depassement_puissance_h")

    return (
        periodes
        # Jointure avec les règles TURPE sur la FTA (nomenclature CRE : c_*)
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

        # Calcul des contributions TURPE variable par cadran
        .with_columns(expr_calculer_turpe_contributions_cadrans())

        # Calcul du total TURPE variable (cadrans + dépassement)
        .with_columns(
            (expr_sommer_turpe_cadrans() + expr_calculer_composante_depassement())
            .alias("turpe_variable_eur")
        )

        # Sélection des colonnes finales (exclure les colonnes de règles intermédiaires)
        .select([
            # Colonnes originales des périodes
            *colonnes_originales,
            # Colonnes TURPE calculées
            "turpe_variable_eur"
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
        energie_col = f"energie_{cadran}_kwh"
        tarif_col = f"c_{cadran}"  # Nomenclature CRE officielle
        debug_expressions.extend([
            pl.col(energie_col).alias(f"debug_energie_{cadran}"),
            pl.col(tarif_col).alias(f"debug_tarif_{cadran}"),
            (pl.col(energie_col) * pl.col(tarif_col) / 100).alias(f"debug_contribution_{cadran}")
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