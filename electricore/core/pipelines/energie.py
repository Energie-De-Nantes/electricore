"""
Expressions Polars pour le pipeline énergie.

Ce module contient des expressions composables suivant la philosophie
fonctionnelle de Polars. Les expressions sont des transformations pures
qui peuvent être composées entre elles pour générer les périodes d'énergie.
"""

import pandera.polars as pa
import polars as pl
from pandera.typing.polars import LazyFrame

from electricore.core.models.cadrans import CADRANS, SOUS_CADRANS, col_energie, col_index
from electricore.core.models.chronologie_releves import ChronologieReleves
from electricore.core.models.historique import Historique
from electricore.core.models.periode_energie import PeriodeEnergie
from electricore.core.models.releve_index import RelevéIndex

# Méta-colonnes de période partagées (issue #178, ADR-0023)
from electricore.core.pipelines.periodes import exprs_meta_periode

# Import du calcul TURPE variable
from electricore.core.pipelines.turpe import ajouter_turpe_variable

# =============================================================================
# CONSTANTES DE LA CHRONOLOGIE DES RELEVÉS (issue #180, ADR-0028)
# =============================================================================

# Tolérance d'appariement dans le join_asof (stratégie "nearest") : absorbe le
# décalage horaire entre événements C15 (00:01) et relevés R151 (02:00). Au-delà,
# le relevé est considéré manquant. Constante nommée plutôt qu'un littéral noyé.
TOLERANCE_APPARIEMENT_RELEVES: str = "4h"

# Priorité **explicite** des sources quand un même relevé logique
# (ref_situation_contractuelle, date_releve, ordre_index) existe dans plusieurs flux.
# Rang croissant = priorité décroissante : la source de plus petit rang gagne.
#
#   flux_C15 > flux_R64 > flux_R151
#
# C15 reste prioritaire (relevés contractuels aux bornes de vie) ; R64 passe devant
# R151 car c'est la source de référence porteuse de la donnée corrigée. Remplace le
# tri alphabétique historique (`flux_C15 < flux_R151 < flux_R64`), qui faisait gagner
# R151 sur R64 par accident lexical (ADR-0028).
PRIORITE_SOURCES: dict[str, int] = {
    "flux_C15": 0,
    "flux_R64": 1,
    "flux_R151": 2,
    "flux_R15": 3,
}
# Rang attribué aux sources hors table (gagnent toujours après les sources connues).
_RANG_SOURCE_INCONNUE: int = 99


def expr_mint_releve_id(source: str, discriminant: pl.Expr) -> pl.Expr:
    """Mint la clé métier `releve_id` en core (ADR-0028, #232).

    Même contrat que le macro dbt `mint_releve_id` : `source|pdl|date_iso|discriminant`.
    Utilisé pour les relevés C15 avant/après, dérivés de l'événement + `ordre_index`
    (le cast booléen rend `true`/`false`, cohérent avec le rendu dbt). Déterministe :
    le même relevé logique produit toujours la même clé, sans dépendre d'un id natif
    (C15 Donnees_Releve n'en a pas) ni de la position fichier.
    """
    return pl.concat_str(
        [
            pl.lit(source),
            pl.col("pdl").cast(pl.Utf8),
            pl.col("date_releve").cast(pl.Utf8),
            discriminant.cast(pl.Utf8),
        ],
        separator="|",
    )


def expr_priorite_source() -> pl.Expr:
    """Expression du rang de priorité d'une source (cf. `PRIORITE_SOURCES`, ADR-0028).

    Plus petit rang = plus prioritaire. Sert de clé de tri avant le dédoublonnage
    `unique(keep="first")` sur le triplet métier.
    """
    return pl.col("source").replace_strict(
        PRIORITE_SOURCES,
        default=_RANG_SOURCE_INCONNUE,
        return_dtype=pl.Int32,
    )


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


def expr_arrondir_index_kwh(cadrans: list[str]) -> list[pl.Expr]:
    """
    Expressions pour arrondir les index à l'entier inférieur (kWh complets).

    Note: La conversion Wh -> kWh est maintenant gérée au niveau du loader DuckDB.
    Cette fonction ne fait plus que l'arrondissement final des valeurs déjà en kWh.

    Args:
        cadrans: Liste des colonnes de cadrans à arrondir

    Returns:
        Liste d'expressions Polars pour l'arrondissement

    Example:
        >>> expressions = expr_arrondir_index_kwh(["BASE", "HP", "HC"])
        >>> lf = lf.with_columns(expressions)
    """
    return [
        pl.when(pl.col(cadran).is_not_null()).then(pl.col(cadran).floor()).otherwise(pl.col(cadran)).alias(cadran)
        for cadran in cadrans
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

    return pl.when(current.is_not_null() & previous.is_not_null()).then(current - previous).otherwise(pl.lit(None))


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


def expr_data_complete() -> pl.Expr:
    """
    Expression pour déterminer si une période a des données complètes.

    Une période est considérée comme ayant des données complètes si :
    - Le relevé de début n'est pas manquant (ou null si pas de flag)
    - Le relevé de fin n'est pas manquant (ou null si pas de flag)

    Returns:
        Expression booléenne indiquant si la période a des données complètes

    Example:
        >>> lf = lf.with_columns(expr_data_complete().alias("data_complete"))
    """
    return (pl.col("releve_manquant_debut").is_null() | ~pl.col("releve_manquant_debut")) & (
        pl.col("releve_manquant_fin").is_null() | ~pl.col("releve_manquant_fin")
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
        pl.col("data_complete"),
    ]

    # Ajouter les colonnes contractuelles si présentes
    selection.extend([pl.col("ref_situation_contractuelle"), pl.col("formule_tarifaire_acheminement")])

    # Ajouter toutes les colonnes d'énergie (format: energie_xxx_kwh)
    selection.append(pl.col("^energie_.*_kwh$"))

    return selection


# =============================================================================
# FONCTIONS DE TRANSFORMATION LAZYFRAME
# =============================================================================


def extraire_releves_evenements(historique: pl.LazyFrame) -> pl.LazyFrame:
    """
    Génère des relevés d'index (avant/après) à partir d'un historique enrichi des événements contractuels - Version Polars.

    Convertit les colonnes Avant_* et Après_* des événements en relevés d'index séparés
    avec ordre_index=False pour "avant" et ordre_index=True pour "après" (discriminant
    booléen unifié, ADR-0028).

    Args:
        historique: LazyFrame contenant l'historique des événements contractuels validé Pandera

    Returns:
        LazyFrame des relevés d'index conformes au modèle RelevéIndex Polars

    Example:
        >>> releves = extraire_releves_evenements(evenements_lf)
    """
    # Colonnes d'index numériques et métadonnées (schéma fixe)
    index_cols = [
        "index_base_kwh",
        "index_hp_kwh",
        "index_hc_kwh",
        "index_hch_kwh",
        "index_hph_kwh",
        "index_hpb_kwh",
        "index_hcb_kwh",
    ]
    # Métadonnées dépivotées avant/après : id_calendrier + nature canonique (ADR-0028).
    metadata_cols = ["id_calendrier_distributeur", "nature_index"]
    identifiants = ["pdl", "ref_situation_contractuelle", "formule_tarifaire_acheminement"]

    # Tolérer l'absence des colonnes de nature (fixtures légères) : les créer en NULL.
    historique = historique.with_columns(
        [
            pl.col(f"{pos}_nature_index").cast(pl.Utf8)
            if f"{pos}_nature_index" in historique.collect_schema().names()
            else pl.lit(None, dtype=pl.Utf8).alias(f"{pos}_nature_index")
            for pos in ("avant", "apres")
        ]
    )

    def _depivot(prefixe: str, ordre: bool) -> pl.LazyFrame:
        """Dépivote un côté (avant/après) en relevé, avec identité mintée (ADR-0028)."""
        return (
            historique.select(
                identifiants
                + ["date_evenement"]
                + [f"{prefixe}_{col}" for col in index_cols]
                + [f"{prefixe}_{col}" for col in metadata_cols]
            )
            .rename(
                {
                    "date_evenement": "date_releve",
                    **{f"{prefixe}_{col}": col for col in index_cols},
                    **{f"{prefixe}_{col}": col for col in metadata_cols},
                }
            )
            .with_columns(
                [
                    pl.lit(ordre, dtype=pl.Boolean).alias("ordre_index"),
                    pl.lit("flux_C15").alias("source"),
                    pl.lit("kWh").alias("unite"),
                    pl.lit("kWh").alias("precision"),
                    # Assurer que id_calendrier_distributeur est en String
                    pl.col("id_calendrier_distributeur").cast(pl.Utf8, strict=False),
                ]
            )
            .with_columns(
                # Identité métier (ADR-0028) : releve_id minté depuis l'événement +
                # discriminant ordre_index ; pas d'Id_Releve natif côté C15.
                expr_mint_releve_id("flux_C15", pl.lit(ordre, dtype=pl.Boolean)).alias("releve_id"),
                pl.lit(None, dtype=pl.Utf8).alias("id_releve"),
            )
        )

    releves_avant = _depivot("avant", False)
    releves_apres = _depivot("apres", True)

    # Combiner et filtrer les lignes avec des index valides
    return (
        pl.concat([releves_avant, releves_apres], how="diagonal")
        # Forcer les types pour éviter les conflits de schéma (edge case : toutes valeurs null → type Null)
        .with_columns(
            [
                pl.col(col).cast(pl.Float64)
                for col in index_cols
                if col not in ["id_calendrier_distributeur"]  # Traiter l'ID séparément
            ]
        )
        .with_columns(pl.col("id_calendrier_distributeur").cast(pl.Utf8, strict=False))
        .filter(
            # Garder les lignes qui ont au moins un index non-null
            pl.any_horizontal([pl.col(col).is_not_null() for col in index_cols])
        )
    )


def interroger_releves(requete: pl.LazyFrame, releves: pl.LazyFrame) -> pl.LazyFrame:
    """
    Interroge les relevés avec tolérance temporelle et GARANTIT un résultat de même taille que la requête.

    Utilise join_asof avec tolérance `TOLERANCE_APPARIEMENT_RELEVES` pour gérer le
    décalage horaire entre :
    - Événements C15 : 00:01 (minuit et 1 minute)
    - Relevés R151 : 02:00 (2 heures du matin)

    Args:
        requete: LazyFrame avec colonnes pdl, date_releve
        releves: LazyFrame des relevés d'index disponibles

    Returns:
        LazyFrame de MÊME TAILLE que requête avec flag releve_manquant

    Example:
        >>> releves_avec_manquants = interroger_releves(requete_lf, releves_lf)
    """
    return (
        requete.sort(["pdl", "date_releve"])
        .set_sorted("pdl")
        .join_asof(
            releves.sort(["pdl", "date_releve"]).set_sorted("pdl"),
            on="date_releve",
            by="pdl",
            strategy="nearest",
            tolerance=TOLERANCE_APPARIEMENT_RELEVES,
        )
        .with_columns(
            [
                # Flag pour tracer les relevés manquants
                pl.col("source").is_null().alias("releve_manquant"),
                # ordre_index par défaut pour les relevés périodiques (pour déduplication) :
                # discriminant booléen unifié, False = relevé périodique / avant (ADR-0028).
                pl.col("ordre_index").cast(pl.Boolean, strict=False).fill_null(False).alias("ordre_index"),
                # Assurer que id_calendrier_distributeur est en String pour cohérence avec C15
                pl.col("id_calendrier_distributeur").cast(pl.Utf8, strict=False),
            ]
        )
    )


def _assembler_chronologie(evenements: pl.LazyFrame, releves: pl.LazyFrame) -> pl.LazyFrame:
    """Assemble la chronologie des relevés (implémentation, sans validation Pandera).

    Combine les relevés aux dates pertinentes en concentrant les cinq invariants
    derrière une seule fonction (issue #180, ADR-0028) :
    - relevés aux événements contractuels C15 (avant/après) ;
    - relevés périodiques (R151/R64) interrogés aux dates de facturation, avec la
      tolérance d'appariement `TOLERANCE_APPARIEMENT_RELEVES` ;
    - **attribution contractuelle** par forward-fill de la RSC sur le PDL (les relevés
      périodiques n'en portent pas) ;
    - **priorité des sources explicite** `flux_C15 > flux_R64 > flux_R151`
      (`PRIORITE_SOURCES`) — plus de tri alphabétique ;
    - **dédoublonnage** sur le triplet métier `(RSC, date_releve, ordre_index)`.

    Args:
        evenements: LazyFrame des événements contractuels + événements FACTURATION
        releves: LazyFrame des relevés d'index périodiques (flux R151/R64)

    Returns:
        LazyFrame chronologique (1 ligne par (RSC, date_releve, ordre_index)).
    """
    # 1. Séparer les événements contractuels des événements FACTURATION
    evt_contractuels = evenements.filter(pl.col("evenement_declencheur") != "FACTURATION")
    evt_facturation = evenements.filter(pl.col("evenement_declencheur") == "FACTURATION")

    # 2. Extraire les relevés des événements contractuels
    rel_evenements = extraire_releves_evenements(evt_contractuels)

    # 3. Pour FACTURATION : construire requête et interroger les relevés existants
    requete_facturation = evt_facturation.select(
        [
            "pdl",
            pl.col("date_evenement").alias("date_releve"),
            "ref_situation_contractuelle",
            "formule_tarifaire_acheminement",
        ]
    )

    rel_facturation = interroger_releves(requete_facturation, releves)

    # 4. Combiner les deux sources de relevés
    return (
        # how="diagonal" : accepte des colonnes différentes entre rel_evenements et rel_facturation
        # rel_evenements n'a pas releve_manquant → sera null
        # rel_facturation a releve_manquant → garde sa valeur
        pl.concat([rel_evenements, rel_facturation], how="diagonal")
        # Tri chronologique par PDL pour les opérations .over("pdl") (évite warning sortedness)
        .sort(["pdl", "date_releve", "ordre_index"])
        .set_sorted("pdl")  # Indiquer explicitement que PDL est trié
        # Attribution contractuelle : forward-fill de la RSC/FTA par PDL (les relevés
        # périodiques n'en portent pas → héritent de l'événement précédent).
        .with_columns(
            [
                pl.col("ref_situation_contractuelle").fill_null(strategy="forward").over("pdl"),
                pl.col("formule_tarifaire_acheminement").fill_null(strategy="forward").over("pdl"),
            ]
        )
        # Priorité des sources : tri sur le rang explicite (C15 > R64 > R151, ADR-0028),
        # plus l'alphabétique accidentel. La source de plus petit rang gagne le dédoublonnage.
        .with_columns(expr_priorite_source().alias("_priorite_source"))
        .sort(["pdl", "date_releve", "ordre_index", "_priorite_source"])
        .set_sorted("pdl")
        # Déduplication par triplet métier, gardant la première occurrence (= source prioritaire).
        .unique(subset=["ref_situation_contractuelle", "date_releve", "ordre_index"], keep="first")
        # Retirer la colonne de travail et trier chronologiquement
        .drop("_priorite_source")
        .sort(["pdl", "date_releve", "ordre_index"])
        .set_sorted("pdl")
    )


@pa.check_types(lazy=True)
def chronologie_releves(
    historique: LazyFrame[Historique], releves: LazyFrame[RelevéIndex]
) -> LazyFrame[ChronologieReleves]:
    """Chronologie des relevés d'un contrat : ligne de temps énergie dédoublonnée.

    Interface publique du module *Chronologie des relevés* (issue #180, ADR-0023,
    ADR-0028). Concentre derrière un contrat Pandera (`ChronologieReleves`) les cinq
    invariants jusqu'ici encodés incidemment dans l'implémentation : priorité de
    sources explicite, attribution RSC par forward-fill, dédoublonnage sur le triplet
    métier, tolérance d'appariement nommée, et discriminant `ordre_index` booléen.

    Le dépivotage C15 avant/après, la résolution asof et le dédoublonnage deviennent de
    l'*implémentation* (`_assembler_chronologie`) derrière cette interface.

    Args:
        historique: événements contractuels enrichis (C15 + événements FACTURATION).
        releves: relevés d'index périodiques disponibles (R151/R64).

    Returns:
        `LazyFrame[ChronologieReleves]` : 1 ligne par (RSC, date_releve, ordre_index),
        RSC non-null, source dans l'énumération, prêt pour `calculer_periodes_energie`.

    Example:
        >>> chronologie = chronologie_releves(historique_lf, releves_lf)
    """
    return _assembler_chronologie(historique, releves)


def reconstituer_chronologie_releves(evenements: pl.LazyFrame, releves: pl.LazyFrame) -> pl.LazyFrame:
    """Alias historique de `chronologie_releves` sans validation Pandera (compat).

    Conserve l'ancienne signature `LazyFrame -> LazyFrame` pour les appelants internes
    et tests qui passent des frames partiels. Préfère `chronologie_releves` pour la
    nouvelle interface contractuelle.
    """
    return _assembler_chronologie(evenements, releves)


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
    3. Arrondi des index à l'entier inférieur (kWh complets)
    4. Calcul vectorisé des énergies tous cadrans
    5. Calcul des flags de qualité
    6. Filtrage des périodes valides
    7. Enrichissement hiérarchique des cadrans

    Args:
        lf: LazyFrame contenant les relevés d'index chronologiques

    Returns:
        LazyFrame avec périodes d'énergie calculées et validées

    Example:
        >>> periodes = calculer_periodes_energie(releves_lf).collect()
    """
    # Colonnes d'index des cadrans canoniques (index_{cadran}_kwh)
    colonnes_index = [col_index(c) for c in CADRANS]

    return (
        lf
        # Tri par contrat et chronologique pour optimiser les .over("ref_situation_contractuelle")
        .sort(["ref_situation_contractuelle", "date_releve", "ordre_index"])
        .set_sorted("ref_situation_contractuelle")  # Indiquer explicitement que ref_situation_contractuelle est trié
        # Étape 1 : Bornes temporelles + arrondi index (indépendants)
        .with_columns(
            [*expr_bornes_depuis_shift(over="ref_situation_contractuelle"), *expr_arrondir_index_kwh(colonnes_index)]
        )
        # Étape 2 : Calcul énergies + méta-colonnes de période (bundle partagé, cf. periodes.py)
        .with_columns([*expr_calculer_energies_tous_cadrans(colonnes_index), *exprs_meta_periode()])
        # Étape 3 : Flag de complétude des données
        .with_columns([expr_data_complete().alias("data_complete")])
        # Filtrage des périodes valides
        .filter(expr_filtrer_periodes_valides())
        # post traitement
        .with_columns([*expr_enrichir_cadrans_principaux()])
        # Note: La sélection finale des colonnes se fait après l'ajout du TURPE dans pipeline_energie
    )


@pa.check_types(lazy=True)
def pipeline_energie(historique: LazyFrame[Historique], releves: LazyFrame[RelevéIndex]) -> LazyFrame[PeriodeEnergie]:
    """
    Pipeline principal pour générer les périodes d'énergie avec TURPE variable.

    Ce pipeline orchestre :
    1. Le filtrage des événements impactant l'énergie
    2. La reconstitution de chronologie des relevés
    3. Le calcul des périodes d'énergie
    4. L'enrichissement avec calcul TURPE variable

    Args:
        historique: LazyFrame contenant l'historique des événements contractuels
        releves: LazyFrame contenant les relevés d'index

    Returns:
        LazyFrame avec les périodes d'énergie et TURPE variable

    Example:
        >>> periodes_energie = pipeline_energie(historique_lf, releves_lf)
        >>> df = periodes_energie.collect()
    """
    return (
        historique.filter(pl.col("impacte_energie"))
        # Chronologie des relevés (issue #180) : assemblage interne sans re-validation
        # Pandera (historique/releves déjà typés à l'entrée du pipeline). L'interface
        # publique typée est `chronologie_releves`.
        .pipe(_assembler_chronologie, releves)
        .pipe(calculer_periodes_energie)
        .pipe(ajouter_turpe_variable)
        # Sélection finale des colonnes (exclut les index bruts BASE, HP, HC, etc.)
        .select(
            [
                *expr_selectionner_colonnes_finales(),
                pl.col("turpe_variable_eur"),  # Ajouté par le pipeline TURPE
            ]
        )
    )
