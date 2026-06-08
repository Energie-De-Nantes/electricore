"""
Expressions Polars pour le pipeline énergie.

Ce module contient des expressions composables suivant la philosophie
fonctionnelle de Polars. Les expressions sont des transformations pures
qui peuvent être composées entre elles pour générer les périodes d'énergie.
"""

import pandera.polars as pa
import polars as pl
from pandera.typing.polars import LazyFrame

from electricore.core.models.historique import Historique
from electricore.core.models.periode_energie import PeriodeEnergie
from electricore.core.models.releve_index import RelevéIndex

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
    return [
        # Étape 1 : Synthèse hc depuis les sous-cadrans hch et hcb
        pl.sum_horizontal([pl.col("energie_hc_kwh"), pl.col("energie_hch_kwh"), pl.col("energie_hcb_kwh")]).alias(
            "energie_hc_kwh"
        ),
        # Étape 2 : Synthèse hp depuis les sous-cadrans hph et hpb
        pl.sum_horizontal([pl.col("energie_hp_kwh"), pl.col("energie_hph_kwh"), pl.col("energie_hpb_kwh")]).alias(
            "energie_hp_kwh"
        ),
        # Étape 3 : Synthèse base depuis TOUS les cadrans (évite le problème d'évaluation parallèle)
        pl.sum_horizontal(
            [
                pl.col("energie_base_kwh"),
                pl.col("energie_hp_kwh"),
                pl.col("energie_hc_kwh"),
                pl.col("energie_hph_kwh"),
                pl.col("energie_hpb_kwh"),
                pl.col("energie_hch_kwh"),
                pl.col("energie_hcb_kwh"),
            ]
        ).alias("energie_base_kwh"),
    ]


def expr_nb_jours() -> pl.Expr:
    """
    Expression pour calculer le nombre de jours d'une période.

    Returns:
        Expression Polars pour calculer nb_jours

    Example:
        >>> lf = lf.with_columns(expr_nb_jours())
    """
    return (pl.col("fin").dt.date() - pl.col("debut").dt.date()).dt.total_days().cast(pl.Int32).alias("nb_jours")


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


def expr_date_formatee_fr(col: str, format_type: str = "complet") -> pl.Expr:
    """
    Formate une colonne de date en français.

    Cette expression reprend le formatage français du pipeline abonnements
    pour assurer la cohérence entre les différents pipelines.

    Args:
        col: Nom de la colonne à formater
        format_type: Type de format ("complet", "mois_annee")

    Returns:
        Expression Polars retournant la date formatée

    Example:
        >>> df.with_columns(expr_date_formatee_fr("debut", "complet").alias("debut_lisible"))
    """
    # Dictionnaire de correspondance anglais -> français
    mois_mapping = {
        "January": "janvier",
        "February": "février",
        "March": "mars",
        "April": "avril",
        "May": "mai",
        "June": "juin",
        "July": "juillet",
        "August": "août",
        "September": "septembre",
        "October": "octobre",
        "November": "novembre",
        "December": "décembre",
    }

    if format_type == "complet":
        # Format "1 mars 2025"
        expr = pl.col(col).dt.strftime("%d %B %Y")

        # Appliquer les remplacements séquentiellement
        for en_mois, fr_mois in mois_mapping.items():
            expr = expr.str.replace_all(en_mois, fr_mois)

        return expr

    elif format_type == "mois_annee":
        # Format "mars 2025"
        expr = pl.col(col).dt.strftime("%B %Y")

        # Appliquer les remplacements séquentiellement
        for en_mois, fr_mois in mois_mapping.items():
            expr = expr.str.replace_all(en_mois, fr_mois)

        return expr

    else:
        raise ValueError(f"Format non supporté : {format_type}")


# =============================================================================
# FONCTIONS DE TRANSFORMATION LAZYFRAME
# =============================================================================


def extraire_releves_evenements(historique: pl.LazyFrame) -> pl.LazyFrame:
    """
    Génère des relevés d'index (avant/après) à partir d'un historique enrichi des événements contractuels - Version Polars.

    Convertit les colonnes Avant_* et Après_* des événements en relevés d'index séparés
    avec ordre_index=0 pour "avant" et ordre_index=1 pour "après".

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
    metadata_cols = ["id_calendrier_distributeur"]
    identifiants = ["pdl", "ref_situation_contractuelle", "formule_tarifaire_acheminement"]

    # Relevés "avant" (ordre_index=0)
    releves_avant = (
        historique.select(
            identifiants
            + ["date_evenement"]
            + [f"avant_{col}" for col in index_cols]
            + [f"avant_{col}" for col in metadata_cols]
        )
        .rename(
            {
                "date_evenement": "date_releve",
                **{f"avant_{col}": col for col in index_cols},
                **{f"avant_{col}": col for col in metadata_cols},
            }
        )
        .with_columns(
            [
                pl.lit(0, dtype=pl.Int32).alias("ordre_index"),
                pl.lit("flux_C15").alias("source"),
                pl.lit("kWh").alias("unite"),
                pl.lit("kWh").alias("precision"),
                # Assurer que id_calendrier_distributeur est en String
                pl.col("id_calendrier_distributeur").cast(pl.Utf8, strict=False),
            ]
        )
    )

    # Relevés "après" (ordre_index=1)
    releves_apres = (
        historique.select(
            identifiants
            + ["date_evenement"]
            + [f"apres_{col}" for col in index_cols]
            + [f"apres_{col}" for col in metadata_cols]
        )
        .rename(
            {
                "date_evenement": "date_releve",
                **{f"apres_{col}": col for col in index_cols},
                **{f"apres_{col}": col for col in metadata_cols},
            }
        )
        .with_columns(
            [
                pl.lit(1, dtype=pl.Int32).alias("ordre_index"),
                pl.lit("flux_C15").alias("source"),
                pl.lit("kWh").alias("unite"),
                pl.lit("kWh").alias("precision"),
                # Assurer que id_calendrier_distributeur est en String
                pl.col("id_calendrier_distributeur").cast(pl.Utf8, strict=False),
            ]
        )
    )

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

    Utilise join_asof avec tolérance de 4h pour gérer le décalage horaire entre :
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
            tolerance="4h",  # Tolérance de 4 heures comme dans le pipeline pandas
        )
        .with_columns(
            [
                # Flag pour tracer les relevés manquants
                pl.col("source").is_null().alias("releve_manquant"),
                # Ajouter ordre_index par défaut pour les relevés R151 (pour déduplication)
                pl.when(pl.col("ordre_index").is_null())
                .then(pl.lit(0, dtype=pl.Int32))
                .otherwise(pl.col("ordre_index").cast(pl.Int32))
                .alias("ordre_index"),
                # Assurer que id_calendrier_distributeur est en String pour cohérence avec C15
                pl.col("id_calendrier_distributeur").cast(pl.Utf8, strict=False),
            ]
        )
    )


def reconstituer_chronologie_releves(evenements: pl.LazyFrame, releves: pl.LazyFrame) -> pl.LazyFrame:
    """
    Reconstitue la chronologie complète des relevés nécessaires pour la facturation - Version Polars.

    Assemble tous les relevés aux dates pertinentes en combinant :
    - Les relevés aux dates d'événements contractuels (flux C15 : MES, RES, MCT)
    - Les relevés aux dates de facturation (données depuis R151)

    Args:
        evenements: LazyFrame des événements contractuels + événements FACTURATION
        releves: LazyFrame des relevés d'index quotidiens complets (flux R151)

    Returns:
        LazyFrame chronologique avec priorité : flux_C15 > flux_R151

    Example:
        >>> chronologie = reconstituer_chronologie_releves(evt_lf, releves_lf)
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
        # Propager les références contractuelles avec forward fill par PDL
        .with_columns(
            [
                pl.col("ref_situation_contractuelle").fill_null(strategy="forward").over("pdl"),
                pl.col("formule_tarifaire_acheminement").fill_null(strategy="forward").over("pdl"),
            ]
        )
        # Appliquer priorité des sources (flux_C15 < flux_R151 alphabétiquement)
        .sort(["pdl", "date_releve", "source"])
        .set_sorted("pdl")
        # Déduplication par contrat, gardant la première occurrence (priorité alphabétique)
        .unique(subset=["ref_situation_contractuelle", "date_releve", "ordre_index"], keep="first")
        # Tri final chronologique
        .sort(["pdl", "date_releve", "ordre_index"])
        .set_sorted("pdl")
    )


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
    7. Formatage des dates en français
    8. Enrichissement hiérarchique des cadrans

    Args:
        lf: LazyFrame contenant les relevés d'index chronologiques

    Returns:
        LazyFrame avec périodes d'énergie calculées et validées

    Example:
        >>> periodes = calculer_periodes_energie(releves_lf).collect()
    """
    # Cadrans d'index électriques standard
    cadrans = ["base", "hp", "hc", "hph", "hpb", "hcb", "hch"]
    # Construire les noms de colonnes complets (index_{cadran}_kwh)
    colonnes_index = [f"index_{c}_kwh" for c in cadrans]

    return (
        lf
        # Tri par contrat et chronologique pour optimiser les .over("ref_situation_contractuelle")
        .sort(["ref_situation_contractuelle", "date_releve", "ordre_index"])
        .set_sorted("ref_situation_contractuelle")  # Indiquer explicitement que ref_situation_contractuelle est trié
        # Étape 1 : Bornes temporelles + arrondi index (indépendants)
        .with_columns(
            [*expr_bornes_depuis_shift(over="ref_situation_contractuelle"), *expr_arrondir_index_kwh(colonnes_index)]
        )
        # Étape 2 : Calcul énergies + nb_jours (énergies dépendent d'étape 1, nb_jours indépendant)
        .with_columns([*expr_calculer_energies_tous_cadrans(colonnes_index), expr_nb_jours()])
        # Étape 3 : Flag de complétude des données
        .with_columns([expr_data_complete().alias("data_complete")])
        # Filtrage des périodes valides
        .filter(expr_filtrer_periodes_valides())
        # post traitement
        .with_columns(
            [
                expr_date_formatee_fr("debut", "complet").alias("debut_lisible"),
                expr_date_formatee_fr("fin", "complet").alias("fin_lisible"),
                expr_date_formatee_fr("debut", "mois_annee").alias("mois_annee"),
                *expr_enrichir_cadrans_principaux(),
            ]
        )
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
        .pipe(reconstituer_chronologie_releves, releves)
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
