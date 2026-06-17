"""
Expressions Polars pour le pipeline facturation.

Ce module contient des expressions composables suivant la philosophie
fonctionnelle de Polars. Les expressions sont des transformations pures
qui peuvent être composées entre elles pour générer les méta-périodes de facturation.
"""

import pandera.polars as pa
import polars as pl
from pandera.typing.polars import LazyFrame

from electricore.core.models.aggregates import AbonnementMensuel, EnergieMensuel
from electricore.core.models.periode_abonnement import PeriodeAbonnement
from electricore.core.models.periode_energie import PeriodeEnergie

# Import des modèles Pandera
from electricore.core.models.periode_meta import PeriodeMeta
from electricore.core.pipelines.periodes import expr_date_formatee_fr, expr_nb_jours

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
    return (pl.col("puissance_souscrite_kva") * pl.col("nb_jours")).sum() / pl.col("nb_jours").sum()


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
        + pl.col("puissance_souscrite_kva").cast(pl.Int32).cast(pl.Utf8)
        + pl.lit("kVA")
    )


def expr_calculer_trimestre() -> pl.Expr:
    """
    Expression pour calculer le trimestre à partir de la colonne debut.

    Utilise la colonne debut (datetime) qui représente le début de chaque
    période de facturation. Cette colonne est toujours présente et ne nécessite
    pas de parsing.

    Returns:
        Expression Polars retournant le trimestre au format "YYYY-QX"

    Example:
        >>> df.with_columns(expr_calculer_trimestre().alias("trimestre"))
    """
    # Utiliser directement la colonne debut (déjà en datetime)
    date_col = pl.col("debut")

    # Extraire année et calculer trimestre
    annee = date_col.dt.year().cast(pl.Utf8)
    quarter = ((date_col.dt.month() - 1) // 3 + 1).cast(pl.Utf8)

    return annee + pl.lit("-T") + quarter


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


# =============================================================================
# EXPRESSIONS D'AGRÉGATION MENSUELLE
# =============================================================================


@pa.check_types(lazy=True)
def agreger_abonnements_mensuel(periodes: LazyFrame[PeriodeAbonnement]) -> LazyFrame[AbonnementMensuel]:
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
    periodes_avec_memo = periodes.with_columns(expr_memo_puissance_simple().alias("memo_simple"))

    return (
        periodes_avec_memo.group_by(["ref_situation_contractuelle", "pdl", "mois_annee"])
        .agg(
            [
                # Agrégations numériques
                pl.col("nb_jours").sum(),
                expr_puissance_moyenne().alias("puissance_moyenne_kva"),
                pl.col("turpe_fixe_eur").sum(),
                # Métadonnées (première valeur car identique dans le groupe)
                pl.col("formule_tarifaire_acheminement").first(),
                # Bornes temporelles
                pl.col("debut").min(),
                pl.col("fin").max(),
                # Comptage des sous-périodes
                pl.col("ref_situation_contractuelle").len().alias("nb_sous_periodes_abo"),
                # Construction du mémo des puissances
                pl.col("memo_simple").str.join(", ").alias("memo_puissance_concat"),
            ]
        )
        .with_columns(
            [
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
            ]
        )
        .drop("memo_puissance_concat")
    )


@pa.check_types(lazy=True)
def agreger_energies_mensuel(periodes: LazyFrame[PeriodeEnergie]) -> LazyFrame[EnergieMensuel]:
    """
    Agrège les périodes d'énergie par mois avec sommes simples.

    Les énergies sont additives, donc on peut simplement sommer
    toutes les valeurs par mois.

    Args:
        lf: LazyFrame des périodes d'énergie détaillées

    Returns:
        LazyFrame agrégé par mois avec énergies sommées
    """
    # Vérifier si nb_jours existe dans le schema
    schema_cols = periodes.collect_schema().names()

    # Calculer nb_jours si pas présent
    if "nb_jours" not in schema_cols:
        periodes = periodes.with_columns([expr_nb_jours().alias("nb_jours")])
    else:
        periodes = periodes.with_columns(
            [
                pl.when(pl.col("nb_jours").is_null())
                .then(expr_nb_jours())
                .otherwise(pl.col("nb_jours"))
                .alias("nb_jours")
            ]
        )

    # Axes de statut jumeaux portés par les sous-périodes (ADR-0033 qualité / ADR-0036
    # communication) : complétés en null typé s'ils manquent (chronologie synthétique) →
    # la sous-période compte comme incalculable / non-communicante au rollup.
    for col in ("qualite", "statut_communication"):
        if col not in schema_cols:
            periodes = periodes.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))

    return (
        periodes.group_by(["ref_situation_contractuelle", "pdl", "mois_annee"])
        .agg(
            [
                # Énergies par cadran (sommes simples)
                pl.col("energie_base_kwh").sum(),
                pl.col("energie_hp_kwh").sum(),
                pl.col("energie_hc_kwh").sum(),
                # Bornes temporelles
                pl.col("debut").min(),
                pl.col("fin").max(),
                # Montants TURPE
                pl.col("turpe_variable_eur").sum(),
                # Qualité méta (ADR-0033) : rollup PIRE-GAGNE des sous-périodes
                # (incalculable > estimée > réelle ; null/inconnu compte incalculable).
                # Subsume l'ancien data_complete : un relevé bornant manquant → période
                # incalculable → mois incalculable (retrait data_complete/coverage, ADR-0033).
                pl.when((pl.col("qualite").is_null() | (pl.col("qualite") == "incalculable")).any())
                .then(pl.lit("incalculable"))
                .when((pl.col("qualite") == "estimée").any())
                .then(pl.lit("estimée"))
                .otherwise(pl.lit("réelle"))
                .alias("qualite"),
                # Communication méta (ADR-0036) : PLEIN-OU-RIEN — communicante ssi TOUTES les
                # sous-périodes du segment actif le sont (une bascule mid-mois écarte le mois ;
                # la troncature entrée/sortie reste éligible car elle n'introduit pas de
                # borne niveau-0). null/inconnu compte non-communicant.
                pl.when((pl.col("statut_communication") == "communicante").fill_null(False).all())
                .then(pl.lit("communicante"))
                .otherwise(pl.lit("non_communicante"))
                .alias("statut_communication"),
                # Nombre TOTAL de sous-périodes d'énergie du mois (>1 ⇒ changement).
                pl.len().alias("nb_sous_periodes_energie"),
            ]
        )
        .with_columns(
            # Flag de changement si plusieurs sous-périodes
            (pl.col("nb_sous_periodes_energie") > 1).alias("has_changement_energie"),
        )
    )


# =============================================================================
# EXPRESSIONS DE JOINTURE ET RÉCONCILIATION
# =============================================================================


@pa.check_types(lazy=True)
def joindre_meta_periodes(
    abo_mensuel: LazyFrame[AbonnementMensuel], energie_mensuel: LazyFrame[EnergieMensuel]
) -> LazyFrame[PeriodeMeta]:
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
    meta_periodes_lf = abo_mensuel.join(energie_mensuel, on=cles_jointure, how="full", suffix="_energie")

    # Verdicts jumeaux (ADR-0033 qualité / ADR-0036 communication) : portés par l'agrégat
    # énergie ; complétés en null s'ils manquent (frames synthétiques). Le fill_null
    # ci-dessous ramène un mois sans énergie au verdict « pas de donnée » (incalculable /
    # non-communicante).
    joined_cols = meta_periodes_lf.collect_schema().names()
    for col in ("qualite", "statut_communication"):
        if col not in joined_cols:
            meta_periodes_lf = meta_periodes_lf.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))

    return (
        meta_periodes_lf.with_columns(
            [
                # Réconciliation des clés de jointure (priorité aux abonnements)
                pl.coalesce(
                    [pl.col("ref_situation_contractuelle"), pl.col("ref_situation_contractuelle_energie")]
                ).alias("ref_situation_contractuelle"),
                pl.coalesce([pl.col("pdl"), pl.col("pdl_energie")]).alias("pdl"),
                pl.coalesce([pl.col("mois_annee"), pl.col("mois_annee_energie")]).alias("mois_annee"),
                # Réconciliation des dates (priorité aux abonnements)
                pl.coalesce([pl.col("debut"), pl.col("debut_energie")]).alias("debut"),
                pl.coalesce([pl.col("fin"), pl.col("fin_energie")]).alias("fin"),
                # Réconciliation des valeurs manquantes
                pl.col("puissance_moyenne_kva").fill_null(0.0),
                pl.col("formule_tarifaire_acheminement").fill_null("INCONNU"),
                pl.col("turpe_fixe_eur").fill_null(0.0),
                pl.col("turpe_variable_eur").fill_null(0.0),
                # Gestion des compteurs de sous-périodes
                pl.col("nb_sous_periodes_abo").fill_null(1),
                pl.col("nb_sous_periodes_energie").fill_null(0),
                # Gestion des flags de changement
                pl.col("has_changement_abo").fill_null(False),
                pl.col("has_changement_energie").fill_null(False),
                # Gestion des énergies
                pl.col("energie_base_kwh").fill_null(0.0),
                pl.col("energie_hp_kwh").fill_null(0.0),
                pl.col("energie_hc_kwh").fill_null(0.0),
                # Verdicts jumeaux : un mois sans énergie tombe au verdict « pas de donnée ».
                pl.col("qualite").fill_null("incalculable"),
                pl.col("statut_communication").fill_null("non_communicante"),
                # Mémo puissance
                pl.col("memo_puissance").fill_null(""),
            ]
        )
        .with_columns(
            [
                # Recalculer nb_jours si manquant
                pl.when(pl.col("nb_jours").is_null())
                .then(expr_nb_jours())
                .otherwise(pl.col("nb_jours"))
                .alias("nb_jours"),
                # Flag de changement global
                (pl.col("has_changement_abo") | pl.col("has_changement_energie")).alias("has_changement"),
            ]
        )
        .drop(
            [
                "debut_energie",
                "fin_energie",
                "has_changement_abo",
                "has_changement_energie",
                "ref_situation_contractuelle_energie",
                "pdl_energie",
                "mois_annee_energie",
            ]
        )
    )


# =============================================================================
# PIPELINE PRINCIPAL
# =============================================================================


@pa.check_types(lazy=True)
def pipeline_facturation(
    abonnements_lf: LazyFrame[PeriodeAbonnement], energies_lf: LazyFrame[PeriodeEnergie]
) -> LazyFrame[PeriodeMeta]:
    """
    Pipeline pur d'agrégation de facturation avec méta-périodes mensuelles - Version Polars.

    ⚠️  ATTEND des périodes déjà calculées (abonnements + énergie).
        Pour l'orchestration complète, utiliser le build approprié.

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
        LazyFrame[PeriodeMeta] avec les méta-périodes mensuelles de facturation.
        Matérialisation à la charge du caller (typiquement un build, cf. ADR-0019).
    """
    # Étape 1 : Agrégation mensuelle des abonnements
    abo_mensuel_lf = agreger_abonnements_mensuel(abonnements_lf)

    # Étape 2 : Agrégation mensuelle des énergies
    energie_mensuel_lf = agreger_energies_mensuel(energies_lf)

    # Étape 3 : Jointure et réconciliation
    meta_periodes_lf = joindre_meta_periodes(abo_mensuel_lf, energie_mensuel_lf)

    # Étape 4 : Formatage final et tri
    result_lf = meta_periodes_lf.with_columns(
        [
            # Dates lisibles en français, même convention que les sous-périodes (issue #178)
            expr_date_formatee_fr("debut").alias("debut_lisible"),
            expr_date_formatee_fr("fin").alias("fin_lisible"),
        ]
    ).sort(["ref_situation_contractuelle", "debut"])

    # Validation Pandera au seam (lazy=True dans le décorateur).
    # Pas de .collect() ici : matérialisation au boundary du caller (ADR-0019).
    return result_lf


# `rapprocher_facturation_mensuelle` a été déplacée vers
# `core.builds.contexte_mensuel.rapprocher` (cf. slice 2 de la refonte
# Contexte mensuel, issue #88). Le mapping catégorie → colonne Enedis est
# maintenant interne à `rapprocher()`.


# Export des fonctions principales
__all__ = [
    "pipeline_facturation",
    "agreger_abonnements_mensuel",
    "agreger_energies_mensuel",
    "joindre_meta_periodes",
    "expr_puissance_moyenne",
    "expr_memo_puissance_simple",
    "expr_calculer_trimestre",
    "expr_has_changement",
]
