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


# =============================================================================
# FONCTIONS DE TRANSFORMATION LAZYFRAME
# =============================================================================


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

    Combine les relevés aux dates pertinentes en concentrant les invariants
    derrière une seule fonction (issue #180, ADR-0028) :
    - relevés aux événements contractuels C15 (avant/après) ;
    - relevés périodiques (R151/R64) interrogés aux dates de facturation, avec la
      tolérance d'appariement `TOLERANCE_APPARIEMENT_RELEVES` ;
    - **priorité des sources explicite** `flux_C15 > flux_R64 > flux_R151`
      (`PRIORITE_SOURCES`) — plus de tri alphabétique ;
    - **dédoublonnage** sur le triplet métier `(RSC, date_releve, ordre_index)`.

    L'**attribution des attributs de situation** (RSC/FTA/niveau) est garantie EN AMONT
    (ADR-0029/ADR-0039), plus par cette fonction : les relevés C15 portent leur valeur
    NATIVE, et les relevés périodiques entrent via `interroger_releves`, dont la requête
    FACTURATION porte la RSC/FTA/niveau du contrat (depuis le substrat d'événements, plus
    depuis le mart `releves` qui ne les recopie plus). L'entrée est donc attribuée (RSC
    renseignée) ; le contrat de sortie `ChronologieReleves` (RSC non-null) reste vérifié
    par Pandera.

    Args:
        evenements: LazyFrame des événements contractuels + événements FACTURATION
        releves: LazyFrame des relevés d'index périodiques (flux R151/R64)

    Returns:
        LazyFrame chronologique (1 ligne par (RSC, date_releve, ordre_index)).
    """
    # 1. Séparer les événements contractuels des événements FACTURATION
    evt_contractuels = evenements.filter(pl.col("evenement_declencheur") != "FACTURATION")
    evt_facturation = evenements.filter(pl.col("evenement_declencheur") == "FACTURATION")

    # 2. Relevés contractuels C15 : depuis le modèle canonique `releves` (dépivotés +
    #    releve_id minté au seam dbt, ADR-0029), restreints aux événements qui impactent
    #    l'énergie. L'historique est déjà filtré `impacte_energie` en amont du pipeline ;
    #    le semi-join garantit la parité avec l'ex-extraction historique.
    cles_impacte = evt_contractuels.select(["pdl", pl.col("date_evenement").alias("date_releve")]).unique()
    rel_evenements = releves.filter(pl.col("source") == "flux_C15").join(
        cles_impacte, on=["pdl", "date_releve"], how="semi"
    )

    # 3. Pour FACTURATION : interroger les relevés PÉRIODIQUES (tout sauf C15) aux dates
    #    de facturation. La priorité inter-sources est arbitrée au dédoublonnage ci-dessous.
    rel_periodiques = releves.filter(pl.col("source") != "flux_C15")
    # La requête FACTURATION porte les attributs de SITUATION du contrat — RSC, FTA et
    # niveau d'ouverture — depuis le substrat d'événements (`pipeline_historique`,
    # forward-fillé sur le flux C15 COMPLET, MDPRM compris). C'est par elle que les relevés
    # périodiques sont attribués : le `join_asof` met la requête à gauche, donc sa valeur
    # gagne et la version du mart `releves` (désormais `null` sur les périodiques, ADR-0039)
    # ne fait que rider en `_right`. Le niveau rejoint ainsi RSC/FTA (#324, ADR-0039), ce
    # qui corrige le niveau périmé recopié quand un MDPRM sans index changeait l'ouverture.
    # `niveau_ouverture_services` est ajouté DÉFENSIVEMENT (présent en prod via SCHEMA_C15 ;
    # absent de certaines chronologies synthétiques — `calculer_periodes_energie` le
    # null-fille alors en aval).
    colonnes_requete = [
        "pdl",
        pl.col("date_evenement").alias("date_releve"),
        "ref_situation_contractuelle",
        "formule_tarifaire_acheminement",
    ]
    if "niveau_ouverture_services" in evt_facturation.collect_schema().names():
        colonnes_requete.append("niveau_ouverture_services")
    requete_facturation = evt_facturation.select(colonnes_requete)

    rel_facturation = interroger_releves(requete_facturation, rel_periodiques)

    # 4. Combiner les deux sources de relevés
    return (
        # how="diagonal" : accepte des colonnes différentes entre rel_evenements et rel_facturation
        # rel_evenements n'a pas releve_manquant → sera null
        # rel_facturation a releve_manquant → garde sa valeur
        pl.concat([rel_evenements, rel_facturation], how="diagonal")
        # Tri chronologique par PDL pour les opérations .over("pdl") (évite warning sortedness)
        .sort(["pdl", "date_releve", "ordre_index"])
        .set_sorted("pdl")  # Indiquer explicitement que PDL est trié
        # Attribution des attributs de situation (RSC/FTA/niveau) : assurée EN AMONT
        # (ADR-0029/ADR-0039) — relevés C15 à valeur native, relevés périodiques entrés ici
        # par `interroger_releves`, dont la requête FACTURATION porte la RSC/FTA/niveau du
        # contrat (substrat d'événements, plus le mart). La chronologie n'a donc plus à
        # ré-attribuer : son entrée est déjà attribuée (RSC renseignée).
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
    ADR-0028). Concentre derrière un contrat Pandera (`ChronologieReleves`) les
    invariants jusqu'ici encodés incidemment dans l'implémentation : priorité de
    sources explicite, dédoublonnage sur le triplet métier, tolérance d'appariement
    nommée, et discriminant `ordre_index` booléen. L'attribution des attributs de situation
    (RSC/FTA/niveau) est présumée portée en amont (valeur native C15 + requête FACTURATION,
    ADR-0029/ADR-0039) ; le contrat de sortie garantit (et Pandera vérifie) que la RSC reste
    non-null.

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
