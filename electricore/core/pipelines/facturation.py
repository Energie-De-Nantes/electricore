"""
Expressions Polars pour le pipeline facturation.

Ce module contient des expressions composables suivant la philosophie
fonctionnelle de Polars. Les expressions sont des transformations pures
qui peuvent être composées entre elles pour générer les méta-périodes de facturation.
"""

import polars as pl
import pandera.polars as pa
from pandera.typing.polars import DataFrame, LazyFrame
from typing import Optional, List

# Import des modèles Pandera
from ..models.periode_meta import PeriodeMetaPolars
from ..models.periode_abonnement import PeriodeAbonnementPolars
from ..models.periode_energie import PeriodeEnergiePolars
from ..models.aggregates import AbonnementMensuelPolars, EnergieMensuelPolars


# =============================================================================
# EXPRESSIONS ATOMIQUES POUR CALCULS D'AGRÉGATION
# =============================================================================

def expr_puissance_moyenne() -> pl.Expr:
    """
    Calcule la puissance moyenne pondérée par le nombre de jours directement dans le groupby.

    Cette expression utilise les fonctions d'agrégation de Polars pour calculer
    sum(puissance * nb_jours) / sum(nb_jours) en une seule expression.

    Returns:
        Expression Polars retournant la puissance moyenne pondérée

    Example:
        >>> lf.group_by(["ref_situation_contractuelle", "pdl", "mois_annee"])
        ...   .agg(expr_puissance_moyenne().alias("puissance_moyenne"))
    """
    return (
        (pl.col("puissance_souscrite") * pl.col("nb_jours")).sum() /
        pl.col("nb_jours").sum()
    )


def expr_memo_puissance_simple() -> pl.Expr:
    """
    Construit un mémo simple pour une période d'abonnement.

    Format : "Xj à YkVA" (ex: "14j à 6kVA")

    Returns:
        Expression Polars retournant le mémo formaté

    Example:
        >>> lf.with_columns(expr_memo_puissance_simple().alias("memo_simple"))
    """
    return (
        pl.col("nb_jours").cast(pl.Utf8)
        + pl.lit("j à ")
        + pl.col("puissance_souscrite").cast(pl.Int32).cast(pl.Utf8)
        + pl.lit("kVA")
    )


def expr_coverage_temporelle(duree_totale_col: str = "nb_jours_total") -> pl.Expr:
    """
    Calcule le taux de couverture temporelle d'une période.

    Args:
        duree_totale_col: Nom de la colonne contenant la durée totale du mois

    Returns:
        Expression Polars retournant le taux de couverture (0.0 à 1.0)

    Example:
        >>> lf.with_columns(expr_coverage_temporelle().alias("coverage"))
    """
    return (
        pl.col("nb_jours").cast(pl.Float64) /
        pl.col(duree_totale_col).cast(pl.Float64)
    ).clip(0.0, 1.0)


def expr_has_changement() -> pl.Expr:
    """
    Détermine s'il y a eu des changements dans la période.

    Un changement est détecté s'il y a plus d'une sous-période.

    Returns:
        Expression Polars retournant True si changement détecté

    Example:
        >>> lf.with_columns(expr_has_changement().alias("has_changement"))
    """
    return pl.col("nb_sous_periodes") > 1


def expr_data_complete() -> pl.Expr:
    """
    Détermine si les données sont complètes.

    Les données sont complètes si coverage_abo=1.0 ET coverage_energie=1.0

    Returns:
        Expression Polars retournant True si données complètes

    Example:
        >>> lf.with_columns(expr_data_complete().alias("data_complete"))
    """
    return (pl.col("coverage_abo") == 1.0) & (pl.col("coverage_energie") == 1.0)


# =============================================================================
# EXPRESSIONS D'AGRÉGATION MENSUELLE
# =============================================================================

@pa.check_types(lazy=True)
def agreger_abonnements_mensuel(
    periodes: LazyFrame[PeriodeAbonnementPolars]
) -> LazyFrame[AbonnementMensuelPolars]:
    """
    Agrège les périodes d'abonnement par mois avec puissance moyenne pondérée.

    Utilise la propriété de linéarité de la tarification pour calculer
    une puissance moyenne pondérée par nb_jours, mathématiquement équivalente
    au calcul détaillé par sous-périodes.

    Args:
        lf: LazyFrame des périodes d'abonnement détaillées

    Returns:
        LazyFrame agrégé par mois avec puissance moyenne pondérée
    """

    # Ajouter le mémo simple pour chaque ligne avant agrégation
    periodes_avec_memo = periodes.with_columns(
        expr_memo_puissance_simple().alias("memo_simple")
    )

    return (
        periodes_avec_memo
        .group_by(["ref_situation_contractuelle", "pdl", "mois_annee"])
        .agg([
            # Agrégations numériques
            pl.col("nb_jours").sum(),
            expr_puissance_moyenne().alias("puissance_moyenne"),
            pl.col("turpe_fixe").sum(),

            # Métadonnées (première valeur car identique dans le groupe)
            pl.col("formule_tarifaire_acheminement").first(),

            # Bornes temporelles
            pl.col("debut").min(),
            pl.col("fin").max(),

            # Comptage des sous-périodes
            pl.col("ref_situation_contractuelle").len().alias("nb_sous_periodes_abo"),

            # Construction du mémo des puissances
            pl.col("memo_simple").str.join(", ").alias("memo_puissance_concat"),
        ])
        .with_columns([
            # Flag de changement si plusieurs sous-périodes
            (pl.col("nb_sous_periodes_abo") > 1).alias("has_changement_abo"),

            # Mémo final : vide si pas de changement de puissance réel
            pl.when(pl.col("nb_sous_periodes_abo") <= 1)
            .then(pl.lit(""))
            .otherwise(
                # TODO: Vérifier si changement réel de puissance
                pl.col("memo_puissance_concat")
            )
            .alias("memo_puissance"),

            # TODO: Calculer coverage_abo réel
            pl.lit(1.0).alias("coverage_abo")  # Placeholder
        ])
        .drop("memo_puissance_concat")
    )


@pa.check_types(lazy=True)
def agreger_energies_mensuel(
    periodes: LazyFrame[PeriodeEnergiePolars]
) -> LazyFrame[EnergieMensuelPolars]:
    """
    Agrège les périodes d'énergie par mois avec sommes simples.

    Les énergies sont additives, donc on peut simplement sommer
    toutes les valeurs par mois.

    Args:
        lf: LazyFrame des périodes d'énergie détaillées

    Returns:
        LazyFrame agrégé par mois avec énergies sommées
    """
    return (
        periodes
        .group_by(["ref_situation_contractuelle", "pdl", "mois_annee"])
        .agg([
            # Énergies par cadran (sommes simples)
            pl.col("base_energie").sum(),
            pl.col("hp_energie").sum(),
            pl.col("hc_energie").sum(),

            # Bornes temporelles
            pl.col("debut").min(),
            pl.col("fin").max(),

            # Montants TURPE
            pl.col("turpe_variable").sum(),

            # Qualité des données (True seulement si TOUTES les périodes sont complètes)
            pl.col("data_complete").all(),

            # Comptage des sous-périodes
            pl.col("ref_situation_contractuelle").len().alias("nb_sous_periodes_energie"),
        ])
        .with_columns([
            # Flag de changement si plusieurs sous-périodes
            (pl.col("nb_sous_periodes_energie") > 1).alias("has_changement_energie"),

            # TODO: Calculer coverage_energie réel
            pl.lit(1.0).alias("coverage_energie")  # Placeholder
        ])
    )


# =============================================================================
# EXPRESSIONS DE JOINTURE ET RÉCONCILIATION
# =============================================================================

@pa.check_types(lazy=True)
def joindre_meta_periodes(
    abo_mensuel: LazyFrame[AbonnementMensuelPolars],
    energie_mensuel: LazyFrame[EnergieMensuelPolars]
) -> LazyFrame[PeriodeMetaPolars]:
    """
    Joint les agrégats d'abonnement et d'énergie sur les clés communes.

    ⚠️  ATTEND toujours les deux datasets (abonnements ET énergies).
        La validation amont doit s'assurer qu'aucun n'est vide.

    Args:
        abo_mensuel_lf: LazyFrame agrégé des abonnements
        energie_mensuel_lf: LazyFrame agrégé des énergies

    Returns:
        LazyFrame joint avec toutes les données de facturation
    """
    # Clés de jointure
    cles_jointure = ["ref_situation_contractuelle", "pdl", "mois_annee"]

    # Jointure externe pour conserver toutes les périodes
    meta_periodes_lf = abo_mensuel.join(
        energie_mensuel,
        on=cles_jointure,
        how="full",
        suffix="_energie"
    )

    return (
        meta_periodes_lf
        .with_columns([
            # Réconciliation des clés de jointure (priorité aux abonnements)
            pl.coalesce([pl.col("ref_situation_contractuelle"), pl.col("ref_situation_contractuelle_energie")]).alias("ref_situation_contractuelle"),
            pl.coalesce([pl.col("pdl"), pl.col("pdl_energie")]).alias("pdl"),
            pl.coalesce([pl.col("mois_annee"), pl.col("mois_annee_energie")]).alias("mois_annee"),

            # Réconciliation des dates (priorité aux abonnements)
            pl.coalesce([pl.col("debut"), pl.col("debut_energie")]).alias("debut"),
            pl.coalesce([pl.col("fin"), pl.col("fin_energie")]).alias("fin"),

            # Réconciliation des valeurs manquantes
            pl.col("puissance_moyenne").fill_null(0.0),
            pl.col("formule_tarifaire_acheminement").fill_null("INCONNU"),
            pl.col("turpe_fixe").fill_null(0.0),
            pl.col("turpe_variable").fill_null(0.0),

            # Gestion des compteurs de sous-périodes
            pl.col("nb_sous_periodes_abo").fill_null(1),
            pl.col("nb_sous_periodes_energie").fill_null(0),

            # Gestion des flags de changement
            pl.col("has_changement_abo").fill_null(False),
            pl.col("has_changement_energie").fill_null(False),

            # Gestion des énergies
            pl.col("base_energie").fill_null(0.0),
            pl.col("hp_energie").fill_null(0.0),
            pl.col("hc_energie").fill_null(0.0),

            # Gestion de data_complete (False si pas d'énergie)
            pl.col("data_complete").fill_null(False),

            # Mémo puissance
            pl.col("memo_puissance").fill_null(""),
        ])
        .with_columns([
            # Recalculer nb_jours si manquant
            pl.when(pl.col("nb_jours").is_null())
            .then(
                (pl.col("fin").dt.date() - pl.col("debut").dt.date()).dt.total_days().cast(pl.Int32)
            )
            .otherwise(pl.col("nb_jours"))
            .alias("nb_jours"),

            # Flag de changement global
            (pl.col("has_changement_abo") | pl.col("has_changement_energie")).alias("has_changement"),

            # TODO: Calculer coverage_abo et coverage_energie
            pl.lit(1.0).alias("coverage_abo"),  # Placeholder
            pl.lit(1.0).alias("coverage_energie"),  # Placeholder
        ])
        .with_columns([
            # data_complete final basé sur les coverages
            expr_data_complete().alias("data_complete")
        ])
        .drop([
            "debut_energie", "fin_energie",
            "has_changement_abo", "has_changement_energie",
            "ref_situation_contractuelle_energie", "pdl_energie", "mois_annee_energie"
        ])
    )


# =============================================================================
# PIPELINE PRINCIPAL
# =============================================================================

@pa.check_types(lazy=True)
def pipeline_facturation(
    abonnements_lf: LazyFrame[PeriodeAbonnementPolars],
    energies_lf: LazyFrame[PeriodeEnergiePolars]
) -> DataFrame[PeriodeMetaPolars]:
    """
    Pipeline pur d'agrégation de facturation avec méta-périodes mensuelles - Version Polars.

    ⚠️  ATTEND des périodes déjà calculées (abonnements + énergie).
        Pour l'orchestration complète, utiliser l'orchestration appropriée

    Pipeline d'agrégation pur :
    1. Agrégation mensuelle des abonnements (puissance moyenne pondérée)
    2. Agrégation mensuelle des énergies (sommes simples)
    3. Jointure des agrégats avec réconciliation
    4. Calcul des métriques de couverture et complétude
    5. Formatage et tri final

    Args:
        abonnements_lf: LazyFrame des périodes d'abonnement avec TURPE fixe
        energies_lf: LazyFrame des périodes d'énergie avec TURPE variable

    Returns:
        DataFrame[PeriodeMetaPolars] avec les méta-périodes mensuelles de facturation
    """
    # Étape 1 : Agrégation mensuelle des abonnements
    abo_mensuel_lf = agreger_abonnements_mensuel(abonnements_lf)

    # Étape 2 : Agrégation mensuelle des énergies
    energie_mensuel_lf = agreger_energies_mensuel(energies_lf)

    # Étape 3 : Jointure et réconciliation
    meta_periodes_lf = joindre_meta_periodes(abo_mensuel_lf, energie_mensuel_lf)

    # Étape 4 : Formatage final et tri
    result_lf = (
        meta_periodes_lf
        .with_columns([
            # Formatage des dates lisibles (réutiliser les expressions existantes)
            # TODO: Implémenter expr_date_formatee_fr pour Polars
            pl.col("debut").dt.strftime("%d %B %Y").alias("debut_lisible"),
            pl.col("fin").dt.strftime("%d %B %Y").alias("fin_lisible"),
        ])
        .sort(["ref_situation_contractuelle", "debut"])
    )

    # Étape 5 : Collecte et validation Pandera
    return result_lf.collect()


# Export des fonctions principales
__all__ = [
    'pipeline_facturation',
    'agreger_abonnements_mensuel',
    'agreger_energies_mensuel',
    'joindre_meta_periodes',
    'expr_puissance_moyenne',
    'expr_memo_puissance_simple',
    'expr_coverage_temporelle',
    'expr_has_changement',
    'expr_data_complete'
]