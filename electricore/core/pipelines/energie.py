"""
Expressions Polars pour le pipeline énergie.

Ce module contient des expressions composables suivant la philosophie
fonctionnelle de Polars. Les expressions sont des transformations pures
qui peuvent être composées entre elles pour générer les périodes d'énergie.

Depuis ADR-0041 (#377), l'assemblage de la *Chronologie des relevés* (relevés C15
aux événements énergie + bornes FACTURATION appariées aux périodiques, dédoublonnage
priorité) vit **en dbt** (vue `chronologie_releves`, loader `chronologie_releves()`). L'énergie
ne garde que son **découpage** : `calculer_periodes_energie` (shift/diff par cadran,
verdicts qualité/communication) + TURPE variable. L'ex-`_assembler_chronologie` (et son
`join_asof` à tolérance) a disparu du cœur — l'appariement relevé↔borne est un equi-join
au grain jour côté dbt.
"""

import pandera.polars as pa
import polars as pl
from pandera.typing.polars import LazyFrame

from electricore.core.models.cadrans import CADRANS, SOUS_CADRANS, col_energie, col_index
from electricore.core.models.chronologie_releves import ChronologieReleves
from electricore.core.models.periode_energie import PeriodeEnergie

# Méta-colonnes de période partagées (issue #178, ADR-0023)
from electricore.core.pipelines.periodes import exprs_meta_periode

# Import du calcul TURPE variable
from electricore.core.pipelines.turpe import ajouter_turpe_variable

# =============================================================================
# EXPRESSIONS PURES ATOMIQUES POUR LE CALCUL D'ÉNERGIE
# =============================================================================


def expr_bornes_depuis_shift(over: str = "ref_situation_contractuelle") -> list[pl.Expr]:
    """
    Définit les bornes temporelles des périodes en utilisant shift sur les relevés.

    Cette expression utilise shift(1) pour créer les bornes debut/fin entre relevés consécutifs
    au sein d'une partition définie par 'over' (contrat ou PDL).

    Args:
        over: Colonne(s) définissant les partitions pour la window function

    Returns:
        Liste d'expressions pour les bornes temporelles et métadonnées

    Example:
        >>> df.with_columns(expr_bornes_depuis_shift())
    """
    return [
        pl.col("date_releve").shift(1).over(over).alias("debut"),
        pl.col("source").shift(1).over(over).alias("source_avant"),
        pl.col("date_releve").alias("fin"),
        pl.col("source").alias("source_apres"),
        # Propager le flag releve_manquant pour le début et la fin
        pl.col("releve_manquant").shift(1).over(over).alias("releve_manquant_debut"),
        pl.col("releve_manquant").alias("releve_manquant_fin"),
    ]


def expr_calculer_energie_cadran(index_col: str, over: str = "ref_situation_contractuelle") -> pl.Expr:
    """
    Calcule l'énergie pour un cadran donné (différence avec relevé précédent).

    Cette expression vectorise le calcul des énergies en utilisant la différence
    entre l'index actuel et l'index précédent (obtenu via shift).

    Args:
        index_col: Nom de la colonne d'index (ex: "index_base_kwh", "index_hp_kwh", "index_hc_kwh")
        over: Colonne(s) définissant les partitions pour la window function

    Returns:
        Expression Polars retournant l'énergie calculée

    Example:
        >>> df.with_columns(
        ...     expr_calculer_energie_cadran("index_base_kwh").alias("energie_base_kwh")
        ... )
    """
    current = pl.col(index_col)
    previous = current.shift(1).over(over)

    # Les index sont des kWh entiers (Int64, ADR-0034) ; l'énergie (leur différence) est
    # cast en Float64 : elle alimente la tarification (TURPE variable = énergie × prix),
    # PeriodeEnergie la porte en Float64. Le passage Int64 s'arrête au seam des relevés.
    return (
        pl.when(current.is_not_null() & previous.is_not_null())
        .then((current - previous).cast(pl.Float64))
        .otherwise(pl.lit(None))
    )


def expr_calculer_energies_tous_cadrans(index_cols: list[str]) -> list[pl.Expr]:
    """
    Expressions pour calculer les énergies de tous les cadrans présents.

    ⚠️ **Prérequis** : Les colonnes d'index doivent être présentes dans le LazyFrame

    Args:
        index_cols: Liste des colonnes d'index à traiter (ex: ["index_base_kwh", "index_hp_kwh"])

    Returns:
        Liste d'expressions pour le calcul des énergies

    Example:
        >>> expressions = expr_calculer_energies_tous_cadrans(["index_base_kwh"])
        >>> lf = lf.with_columns(expressions)
    """
    return [
        expr_calculer_energie_cadran(index_col, "ref_situation_contractuelle").alias(
            index_col.replace("index_", "energie_")
        )
        for index_col in index_cols
    ]


def expr_enrichir_cadrans_principaux() -> list[pl.Expr]:
    """
    Enrichit tous les cadrans principaux avec synthèse hiérarchique des énergies.

    ⚠️ **Prérequis** : Les données d'entrée doivent être validées avec Pandera
    ⚠️ **Assumption** : Toutes les colonnes d'énergie sont présentes (même si nulles)

    Effectue une synthèse en cascade pour créer une hiérarchie complète des cadrans :
    1. energie_hc_kwh = somme(energie_hc_kwh, energie_hch_kwh, energie_hcb_kwh) si au moins une non-null
    2. energie_hp_kwh = somme(energie_hp_kwh, energie_hph_kwh, energie_hpb_kwh) si au moins une non-null
    3. energie_base_kwh = somme(energie_base_kwh, energie_hp_kwh, energie_hc_kwh) si au moins une non-null

    Cette fonction gère tous les types de compteurs via min_count=1.

    Returns:
        Liste d'expressions pour l'enrichissement hiérarchique

    Example:
        >>> df.with_columns(expr_enrichir_cadrans_principaux())
    """
    # Toutes les expressions s'évaluent sur les colonnes d'origine (un seul
    # with_columns) : base somme les 7 cadrans bruts, pas les synthèses.
    syntheses_principaux = [
        pl.sum_horizontal([pl.col(col_energie(principal)), *(pl.col(col_energie(s)) for s in sous)]).alias(
            col_energie(principal)
        )
        for principal, sous in SOUS_CADRANS.items()
    ]
    synthese_base = pl.sum_horizontal([pl.col(col_energie(c)) for c in CADRANS]).alias(col_energie("base"))
    return [*syntheses_principaux, synthese_base]


def expr_filtrer_periodes_valides() -> pl.Expr:
    """
    Expression pour filtrer les périodes valides de manière déclarative.

    Une période est valide si :
    - Elle a une date de début (pas de relevé orphelin)
    - Sa durée est supérieure à 0 jour

    Returns:
        Expression booléenne pour filtrer les périodes valides

    Example:
        >>> df.filter(expr_filtrer_periodes_valides())
    """
    return pl.col("debut").is_not_null() & (pl.col("nb_jours") > 0)


# =============================================================================
# AXES DE STATUT DE PÉRIODE (jumeaux) : qualité (ADR-0033) & communication (ADR-0036)
# =============================================================================
# Plomberie commune : un attribut porté par les relevés (nature d'index / niveau
# d'ouverture) est shifté sur les deux bornes de la période, puis rollupé PIRE-GAGNE.


def expr_bornes_statut(over: str = "ref_situation_contractuelle") -> list[pl.Expr]:
    """Bornes des axes de statut : nature d'index (qualité) et niveau d'ouverture
    (communication) des relevés debut/fin. Plomberie *jumelle* de `expr_bornes_depuis_shift`
    pour les attributs de statut portés par les relevés (ADR-0033/0036)."""
    return [
        pl.col("nature_index").shift(1).over(over).alias("nature_index_debut"),
        pl.col("nature_index").alias("nature_index_fin"),
        pl.col("niveau_ouverture_services").shift(1).over(over).alias("niveau_debut"),
        pl.col("niveau_ouverture_services").alias("niveau_fin"),
    ]


def _expr_rang_qualite(col: str) -> pl.Expr:
    """Rang pire-gagne de la nature d'index d'une borne (ADR-0033) :
    réel/corrigé → 0 (réelle), estimé → 1 (estimée), null/manquant → 2 (incalculable)."""
    return (
        pl.when(pl.col(col).is_in(["réel", "corrigé"]))
        .then(pl.lit(0))
        .when(pl.col(col) == "estimé")
        .then(pl.lit(1))
        .otherwise(pl.lit(2))
    )


def expr_qualite_periode() -> pl.Expr:
    """Qualité d'une période d'énergie (ADR-0033) : rollup PIRE-GAGNE de la nature d'index
    de ses deux bornes — réel/corrigé → réelle ; estimé → estimée ; relevé manquant
    (nature null) → incalculable (incalculable > estimée > réelle)."""
    pire = pl.max_horizontal(_expr_rang_qualite("nature_index_debut"), _expr_rang_qualite("nature_index_fin"))
    return (
        pl.when(pire == 0)
        .then(pl.lit("réelle"))
        .when(pire == 1)
        .then(pl.lit("estimée"))
        .otherwise(pl.lit("incalculable"))
    )


def _expr_niveau_communicant(col: str) -> pl.Expr:
    """Borne communicante ssi son niveau d'ouverture ≥ 1 (ADR-0036). `niveau` est un
    `xsd:string ∈ {0,1,2}` → cast Int8 ; null (relevé manquant / avant tout C15) → non
    communicant."""
    return pl.col(col).cast(pl.Int8, strict=False) >= 1


def expr_statut_communication_periode() -> pl.Expr:
    """Statut d'ouverture (communication) d'une période (ADR-0036) : COMMUNICANTE ssi ses
    deux bornes sont à niveau ≥ 1 ; sinon non-communicante (une seule borne niveau-0
    suffit à écarter). Verdict jumeau de la qualité, calculé sur l'axe orthogonal niveau."""
    return (
        pl.when(_expr_niveau_communicant("niveau_debut") & _expr_niveau_communicant("niveau_fin"))
        .then(pl.lit("communicante"))
        .otherwise(pl.lit("non_communicante"))
    )


def expr_selectionner_colonnes_finales():
    """
    Sélection pour garder uniquement les colonnes finales pertinentes.

    Exclut les colonnes d'index bruts (base, hp, hc, etc.) pour ne garder que
    les métadonnées et les énergies calculées, comme dans le pipeline pandas.

    Returns:
        Liste d'expressions pour sélectionner les colonnes finales

    Example:
        >>> lf.select(expr_selectionner_colonnes_finales())
    """
    # Colonnes de base toujours présentes
    selection = [
        pl.col("pdl"),
        pl.col("debut"),
        pl.col("fin"),
        pl.col("nb_jours"),
        pl.col("debut_lisible"),
        pl.col("fin_lisible"),
        pl.col("mois_annee"),
        pl.col("source_avant"),
        pl.col("source_apres"),
        # Verdicts de période jumeaux (ADR-0033 qualité / ADR-0036 communication).
        pl.col("qualite"),
        pl.col("statut_communication"),
    ]

    # Ajouter les colonnes contractuelles si présentes
    selection.extend([pl.col("ref_situation_contractuelle"), pl.col("formule_tarifaire_acheminement")])

    # Ajouter toutes les colonnes d'énergie (format: energie_xxx_kwh)
    selection.append(pl.col("^energie_.*_kwh$"))

    return selection


@pa.check_types(lazy=True)
def calculer_periodes_energie(lf: pl.LazyFrame) -> LazyFrame[PeriodeEnergie]:
    """
    Pipeline de calcul des périodes d'énergie avec approche fonctionnelle Polars.

    🔄 **Version Polars optimisée** - Approche pipeline avec LazyFrame :
    - **Pipeline déclaratif** avec pipe() pour une meilleure lisibilité
    - **Vectorisation maximale** des calculs d'énergies
    - **Expressions pures** facilement testables et maintenables
    - **Performance optimisée** grâce aux optimisations Polars

    Pipeline de transformation :
    1. tri temporel des relevés
    2. Calcul des décalages par contrat avec window functions
    3. Calcul vectorisé des énergies tous cadrans
    4. Calcul des flags de qualité
    5. Filtrage des périodes valides
    6. Enrichissement hiérarchique des cadrans

    Args:
        lf: LazyFrame contenant les relevés d'index chronologiques

    Returns:
        LazyFrame avec périodes d'énergie calculées et validées

    Example:
        >>> periodes = calculer_periodes_energie(releves_lf).collect()
    """
    # Colonnes d'index des cadrans canoniques (index_{cadran}_kwh)
    colonnes_index = [col_index(c) for c in CADRANS]

    # Les attributs de statut portés par les relevés (nature d'index → qualité ; niveau
    # d'ouverture → communication) peuvent manquer sur une chronologie synthétique : les
    # compléter en null typé pour que les bornes/verdicts restent calculables (→ période
    # incalculable / non communicante, sémantiquement correct quand la donnée manque).
    schema = lf.collect_schema().names()
    manquantes = [c for c in ("nature_index", "niveau_ouverture_services") if c not in schema]
    if manquantes:
        lf = lf.with_columns([pl.lit(None, dtype=pl.Utf8).alias(c) for c in manquantes])

    return (
        lf
        # Tri par contrat et chronologique pour optimiser les .over("ref_situation_contractuelle")
        .sort(["ref_situation_contractuelle", "date_releve", "ordre_index"])
        .set_sorted("ref_situation_contractuelle")  # Indiquer explicitement que ref_situation_contractuelle est trié
        # Étape 1 : Bornes temporelles + bornes de statut (nature/niveau des 2 bornes). Les
        # index arrivent déjà en kWh entiers depuis le boundary dbt (ADR-0034).
        .with_columns(
            [
                *expr_bornes_depuis_shift(over="ref_situation_contractuelle"),
                *expr_bornes_statut(over="ref_situation_contractuelle"),
            ]
        )
        # Étape 2 : Calcul énergies + méta-colonnes de période (bundle partagé, cf. periodes.py)
        .with_columns([*expr_calculer_energies_tous_cadrans(colonnes_index), *exprs_meta_periode()])
        # Étape 3 : Verdicts de période jumeaux — qualité (ADR-0033) & communication
        # (ADR-0036). Remplacent l'ancien flag `data_complete` (retiré, ADR-0033).
        .with_columns(
            [
                expr_qualite_periode().alias("qualite"),
                expr_statut_communication_periode().alias("statut_communication"),
            ]
        )
        # Filtrage des périodes valides
        .filter(expr_filtrer_periodes_valides())
        # post traitement
        .with_columns([*expr_enrichir_cadrans_principaux()])
        # Note: La sélection finale des colonnes se fait après l'ajout du TURPE dans pipeline_energie
    )


@pa.check_types(lazy=True)
def pipeline_energie(chronologie: LazyFrame[ChronologieReleves]) -> LazyFrame[PeriodeEnergie]:
    """
    Pipeline principal pour générer les périodes d'énergie avec TURPE variable.

    Depuis ADR-0041 (#377), l'énergie consomme la *Chronologie des relevés* déjà
    assemblée en dbt (vue `chronologie_releves`, loader `chronologie_releves()`) : elle ne fait
    plus que son **découpage** :
    1. Le calcul des périodes d'énergie (shift/diff par cadran, verdicts qualité/communication) ;
    2. L'enrichissement avec calcul TURPE variable.

    L'assemblage (relevés C15 aux événements énergie + bornes FACTURATION appariées aux
    périodiques + dédoublonnage priorité) et le filtre `impacte_energie` vivent désormais
    dans le mart. Le filtre **horizon** est appliqué au boundary (`charger`/`_composer`)
    avant l'appel — pas ici (pureté #179).

    Args:
        chronologie: LazyFrame de la *Chronologie des relevés* (vue dbt, déjà bornée à
            l'horizon par l'appelant).

    Returns:
        LazyFrame avec les périodes d'énergie et TURPE variable

    Example:
        >>> from electricore.core.loaders import chronologie_releves
        >>> periodes_energie = pipeline_energie(chronologie_releves().lazy())
        >>> df = periodes_energie.collect()
    """
    return (
        chronologie.pipe(calculer_periodes_energie)
        .pipe(ajouter_turpe_variable)
        # Sélection finale des colonnes (exclut les index bruts BASE, HP, HC, etc.)
        .select(
            [
                *expr_selectionner_colonnes_finales(),
                pl.col("turpe_variable_eur"),  # Ajouté par le pipeline TURPE
            ]
        )
    )
