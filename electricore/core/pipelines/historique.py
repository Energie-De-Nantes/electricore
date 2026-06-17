"""
Expressions Polars pour le pipeline historique.

Ce module contient des expressions composables suivant la philosophie
fonctionnelle de Polars. Les expressions sont des transformations pures
qui peuvent être composées entre elles.

Le pipeline `pipeline_historique` produit l'Historique enrichi au sens du
glossaire (cf. `electricore/core/CONTEXT.md` et ADR-0013) : événements C15
+ détection des points de rupture + événements FACTURATION artificiels.

Horizon de facturation (issue #179) : le pipeline est paramétré par un unique
`horizon` (un `datetime` en Europe/Paris) qui sert *à la fois* de borne haute des
événements retenus *et* de fin par défaut des périodes ouvertes (génération des
FACTURATION). Cet horizon est l'unique source de temps du pipeline : aucune
lecture d'horloge n'a lieu dans le cœur pur — la résolution du défaut « 1er du
mois courant » est faite une seule fois, au boundary I/O (cf. `core/builds/
contexte_mensuel.py`, ADR-0019). Quand un caller direct (test, notebook) ne fournit
pas d'horizon, le pipeline résout ce défaut *une fois* et le propage partout, ce qui
rend structurellement impossible l'ancienne incohérence à deux fuseaux.
"""

import datetime as dt
from zoneinfo import ZoneInfo

import pandera.polars as pa
import polars as pl
from pandera.typing.polars import LazyFrame

from electricore.core.models.cadrans import CADRANS, col_index
from electricore.core.models.historique import Historique

_PARIS = ZoneInfo("Europe/Paris")


def horizon_par_defaut() -> dt.datetime:
    """Résout le défaut « 1er du mois courant » en *vraie* heure de Paris.

    Seul point de lecture d'horloge lié à l'horizon de facturation. Appelé au
    boundary I/O (build `contexte_mensuel`) ou, à défaut, une seule fois par
    `pipeline_historique` quand le caller ne fournit pas d'horizon.

    Contrairement au bug #179 (heure murale UTC renommée Paris → décalage 1–2 h),
    on lit l'instant courant *dans* le fuseau Paris avant d'en tronquer le mois.
    """
    maintenant = dt.datetime.now(_PARIS)
    return dt.datetime(maintenant.year, maintenant.month, 1, tzinfo=_PARIS)


def _horizon_expr(horizon: pl.Expr | dt.datetime | None) -> pl.Expr:
    """Normalise l'horizon en expression Polars (résout le défaut si `None`)."""
    if horizon is None:
        return pl.lit(horizon_par_defaut())
    if isinstance(horizon, dt.datetime):
        return pl.lit(horizon)
    return horizon


def expr_changement(col_name: str, over: str = "ref_situation_contractuelle") -> pl.Expr:
    """
    Détecte si une colonne a changé par rapport à sa valeur précédente dans la partition.

    Cette expression compare la valeur actuelle avec la valeur précédente
    (obtenue via shift) au sein d'une partition définie par 'over'.

    Seuls les changements entre deux valeurs non-nulles sont détectés,
    conformément à la logique de l'implémentation pandas existante.

    Args:
        col_name: Nom de la colonne à analyser
        over: Colonne(s) définissant les partitions pour la window function

    Returns:
        Expression Polars retournant un booléen (True si changement détecté)

    Example:
        >>> df.with_columns(
        ...     expr_changement("puissance_souscrite_kva").alias("puissance_change")
        ... )
    """
    current = pl.col(col_name)
    previous = current.shift(1).over(over)

    return pl.when(previous.is_not_null() & current.is_not_null()).then(previous != current).otherwise(False)


def expr_resume_changement(col_name: str, label: str, over: str = "ref_situation_contractuelle") -> pl.Expr:
    """
    Génère un texte résumant le changement d'une colonne.

    Cette expression compose expr_changement pour détecter les changements
    et génère un texte formaté "label: valeur_avant → valeur_après" quand
    un changement est détecté.

    Args:
        col_name: Nom de la colonne à analyser
        label: Préfixe à utiliser dans le résumé (ex: "P" pour Puissance)
        over: Colonne(s) définissant les partitions pour la window function

    Returns:
        Expression Polars retournant une chaîne de caractères (vide si pas de changement)

    Example:
        >>> df.with_columns(
        ...     expr_resume_changement("puissance_souscrite_kva", "P").alias("resume_puissance")
        ... )
        # Produit des valeurs comme "P: 6.0 → 9.0" ou ""
    """
    current = pl.col(col_name)
    previous = current.shift(1).over(over)

    return (
        pl.when(expr_changement(col_name, over))  # Composition avec expr_changement
        .then(pl.concat_str([pl.lit(f"{label}: "), previous.cast(pl.Utf8), pl.lit(" → "), current.cast(pl.Utf8)]))
        .otherwise(pl.lit(""))
    )


def expr_changement_avant_apres(col_avant: str, col_apres: str) -> pl.Expr:
    """
    Détecte si une valeur a changé entre deux colonnes existantes.

    Cette expression compare directement deux colonnes (par exemple Avant_ vs Après_)
    au lieu d'utiliser shift() comme dans expr_changement.

    Seuls les changements entre deux valeurs non-nulles sont détectés,
    suivant la même logique conservatrice que les autres expressions.

    Args:
        col_avant: Nom de la colonne contenant la valeur "avant"
        col_apres: Nom de la colonne contenant la valeur "après"

    Returns:
        Expression Polars retournant un booléen (True si changement détecté)

    Example:
        >>> df.with_columns(
        ...     expr_changement_avant_apres(
        ...         "avant_id_calendrier_distributeur",
        ...         "apres_id_calendrier_distributeur"
        ...     ).alias("calendrier_change")
        ... )
    """
    avant = pl.col(col_avant)
    apres = pl.col(col_apres)

    return pl.when(avant.is_not_null() & apres.is_not_null()).then(avant != apres).otherwise(False)


def expr_evenement_structurant() -> pl.Expr:
    """
    Détecte si un événement est structurant (entrée/sortie du périmètre).

    Les événements structurants ont toujours un impact sur la facturation,
    indépendamment des changements de données. Ce sont les événements qui
    modifient la structure même du périmètre contractuel :
    - CFNE : Changement de Fournisseur - Nouveau Entrant
    - MES : Mise En Service
    - PMES : Première Mise En Service
    - CFNS : Changement de Fournisseur - Nouveau Sortant
    - RES : RÉSiliation

    Returns:
        Expression Polars retournant True si l'événement est structurant

    Example:
        >>> df.with_columns(
        ...     expr_evenement_structurant().alias("evenement_structurant")
        ... )
    """
    evenements_structurants = ["CFNE", "MES", "PMES", "CFNS", "RES"]
    return pl.col("evenement_declencheur").is_in(evenements_structurants)


def expr_changement_index() -> pl.Expr:
    """
    Détecte si au moins une colonne d'index a changé entre Avant_ et Après_.

    Cette expression vérifie s'il y a des changements sur les colonnes d'index
    énergétique utilisées pour le calcul des consommations :
    - base, hp, hc : index de base (mono/double tarif)
    - hph, hch, hpb, hcb : index heures pleines/creuses (triple tarif)

    Utilise pl.any_horizontal() pour détecter si au moins une colonne a changé.

    Returns:
        Expression Polars retournant True si au moins un index a changé

    Example:
        >>> df.with_columns(
        ...     expr_changement_index().alias("index_change")
        ... )
    """
    # Créer une expression pour chaque colonne d'index (un cadran = un index)
    changements = [expr_changement_avant_apres(f"avant_{col_index(c)}", f"apres_{col_index(c)}") for c in CADRANS]

    # Retourner True si au moins un changement est détecté
    return pl.any_horizontal(changements)


def expr_impacte_abonnement(over: str = "ref_situation_contractuelle") -> pl.Expr:
    """
    Détecte si un changement impacte l'abonnement.

    Un changement impacte l'abonnement s'il y a :
    - Un changement de puissance souscrite OU
    - Un changement de formule tarifaire d'acheminement (FTA) OU
    - Un événement structurant (entrée/sortie du périmètre)

    Cette expression compose les détections de changement individuelles
    et les événements structurants en suivant la logique métier du calcul
    de facturation.

    Args:
        over: Colonne(s) définissant les partitions pour la window function

    Returns:
        Expression Polars retournant un booléen (True si impact détecté)

    Example:
        >>> df.with_columns(
        ...     expr_impacte_abonnement().alias("impacte_abonnement")
        ... )
    """
    changement_puissance = expr_changement("puissance_souscrite_kva", over)
    changement_fta = expr_changement("formule_tarifaire_acheminement", over)
    est_structurant = expr_evenement_structurant()

    return changement_puissance | changement_fta | est_structurant


def expr_impacte_energie(over: str = "ref_situation_contractuelle") -> pl.Expr:
    """
    Détecte si un changement impacte l'énergie/consommation.

    Un changement impacte l'énergie s'il y a :
    - Un changement de calendrier distributeur OU
    - Un changement sur au moins une colonne d'index OU
    - Un changement de formule tarifaire d'acheminement (FTA) OU
    - Un événement structurant (entrée/sortie du périmètre)

    Cette expression compose toutes les détections de changement
    qui peuvent affecter le calcul des énergies et consommations.

    Args:
        over: Colonne(s) définissant les partitions pour la window function

    Returns:
        Expression Polars retournant un booléen (True si impact détecté)

    Example:
        >>> df.with_columns(
        ...     expr_impacte_energie().alias("impacte_energie")
        ... )
    """
    changement_calendrier = expr_changement_avant_apres(
        "avant_id_calendrier_distributeur", "apres_id_calendrier_distributeur"
    )
    changement_index = expr_changement_index()
    changement_fta = expr_changement_avant_apres(
        "avant_formule_tarifaire_acheminement", "formule_tarifaire_acheminement"
    )
    est_structurant = expr_evenement_structurant()

    return changement_calendrier | changement_index | changement_fta | est_structurant


def expr_resume_modification() -> pl.Expr:
    """
    Génère un résumé textuel des modifications détectées.

    Cette expression compose les résumés de changements individuels
    pour créer une description lisible des modifications :
    - Changements de puissance et FTA (via expr_resume_changement)
    - Changements de calendrier
    - Mention "rupture index" si des index ont changé

    Returns:
        Expression Polars retournant une chaîne de caractères décrivant les modifications

    Example:
        >>> df.with_columns(
        ...     expr_resume_modification().alias("resume_modification")
        ... )
        # Produit: "P: 6.0 → 9.0, FTA: BTINFCU4 → BTINFMU4, Cal: CAL1 → CAL2"
    """
    resume_puissance = expr_resume_changement("puissance_souscrite_kva", "P")
    resume_fta_shift = expr_resume_changement("formule_tarifaire_acheminement", "FTA")

    # Résumé calendrier (entre colonnes Avant_/Après_)
    resume_calendrier = (
        pl.when(expr_changement_avant_apres("avant_id_calendrier_distributeur", "apres_id_calendrier_distributeur"))
        .then(
            pl.concat_str(
                [
                    pl.lit("Cal: "),
                    pl.col("avant_id_calendrier_distributeur").cast(pl.Utf8),
                    pl.lit(" → "),
                    pl.col("apres_id_calendrier_distributeur").cast(pl.Utf8),
                ]
            )
        )
        .otherwise(pl.lit(""))
    )

    # Mention rupture index si détectée
    resume_index = pl.when(expr_changement_index()).then(pl.lit("rupture index")).otherwise(pl.lit(""))

    # Combiner tous les résumés non-vides
    resumes = [resume_puissance, resume_fta_shift, resume_calendrier, resume_index]

    # Filtrer et joindre les résumés non-vides avec ", "
    return pl.concat_list(resumes).list.drop_nulls().list.eval(pl.element().filter(pl.element() != "")).list.join(", ")


def detecter_points_de_rupture(historique: pl.LazyFrame) -> pl.LazyFrame:
    """
    Enrichit l'historique avec détection des impacts via expressions Polars composables.

    Cette fonction utilise les expressions pures développées pour détecter
    les changements impactant l'abonnement et l'énergie. Elle remplace
    la version pandas en tirant parti de l'optimisation Polars.

    Étapes du pipeline :
    1. Tri par PDL et date d'événement
    2. Création des colonnes Avant_ avec window functions
    3. Détection des impacts via expressions composables
    4. Génération du résumé textuel des modifications

    Args:
        historique: LazyFrame contenant l'historique des événements contractuels

    Returns:
        LazyFrame enrichi avec colonnes impacte_abonnement, impacte_energie, resume_modification

    Example:
        >>> historique_enrichi = detecter_points_de_rupture(historique.lazy()).collect()
    """
    return (
        historique.sort(["ref_situation_contractuelle", "date_evenement"])
        .set_sorted("ref_situation_contractuelle")  # Indiquer explicitement que ref_situation_contractuelle est trié
        # Créer les colonnes Avant_ avec window functions
        .with_columns(
            [
                pl.col("puissance_souscrite_kva")
                .shift(1)
                .over("ref_situation_contractuelle")
                .alias("avant_puissance_souscrite"),
                pl.col("formule_tarifaire_acheminement")
                .shift(1)
                .over("ref_situation_contractuelle")
                .alias("avant_formule_tarifaire_acheminement"),
            ]
        )
        # Appliquer les détections d'impact avec nos expressions pures
        .with_columns(
            [
                expr_impacte_abonnement().alias("impacte_abonnement"),
                expr_impacte_energie().alias("impacte_energie"),
                expr_resume_modification().alias("resume_modification"),
            ]
        )
    )


# =============================================================================
# EXPRESSIONS POUR INSERTION D'ÉVÉNEMENTS DE FACTURATION
# =============================================================================


def expr_evenement_entree() -> pl.Expr:
    """
    Détecte si un événement correspond à une entrée dans le périmètre.

    Les événements d'entrée marquent le début d'une période d'activité
    d'un PDL dans le périmètre contractuel :
    - CFNE : Changement de Fournisseur - Nouveau Entrant
    - MES : Mise En Service
    - PMES : Première Mise En Service

    Returns:
        Expression Polars retournant True si l'événement est une entrée

    Example:
        >>> df.with_columns(
        ...     expr_evenement_entree().alias("est_entree")
        ... )
    """
    evenements_entree = ["CFNE", "MES", "PMES"]
    return pl.col("evenement_declencheur").is_in(evenements_entree)


def expr_evenement_sortie() -> pl.Expr:
    """
    Détecte si un événement correspond à une sortie du périmètre.

    Les événements de sortie marquent la fin d'une période d'activité
    d'un PDL dans le périmètre contractuel :
    - RES : RÉSiliation
    - CFNS : Changement de Fournisseur - Nouveau Sortant

    Returns:
        Expression Polars retournant True si l'événement est une sortie

    Example:
        >>> df.with_columns(
        ...     expr_evenement_sortie().alias("est_sortie")
        ... )
    """
    evenements_sortie = ["RES", "CFNS"]
    return pl.col("evenement_declencheur").is_in(evenements_sortie)


def colonnes_evenement_facturation() -> dict[str, pl.Expr]:
    """
    Crée les colonnes standard pour un événement de facturation artificiel.

    Génère un dictionnaire de colonnes avec les valeurs fixes
    utilisées pour tous les événements FACTURATION artificiels.

    Returns:
        Dictionnaire d'expressions pour les colonnes d'événement artificiel

    Example:
        >>> df.with_columns(**colonnes_evenement_facturation())
    """
    return {
        "evenement_declencheur": pl.lit("FACTURATION"),
        "type_evenement": pl.lit("artificiel"),
        "source": pl.lit("synthese_mensuelle"),
        "resume_modification": pl.lit("Facturation mensuelle"),
        "impacte_abonnement": pl.lit(True),
        "impacte_energie": pl.lit(True),
    }


def expr_date_entree_periode() -> pl.Expr:
    """
    Calcule la date d'entrée dans le périmètre (première date d'événement d'entrée).

    Returns:
        Expression retournant la date minimale des événements CFNE/MES/PMES

    Example:
        >>> df.group_by("ref_situation_contractuelle").agg(
        ...     expr_date_entree_periode().alias("debut")
        ... )
    """
    return pl.when(expr_evenement_entree()).then(pl.col("date_evenement")).min()


def expr_date_sortie_periode(horizon: pl.Expr | dt.datetime | None = None) -> pl.Expr:
    """
    Calcule la date de sortie du périmètre (dernière date d'événement de sortie ou défaut).

    Si aucune sortie, utilise la fin par défaut dérivée de l'`horizon` de
    facturation : le 1er du mois de l'horizon (issue #179). L'horizon est donc
    l'unique source de temps — plus aucune lecture d'horloge ici.

    Args:
        horizon: borne de facturation (`datetime` Europe/Paris, `pl.Expr`, ou
            `None` → 1er du mois courant résolu une fois en vraie heure de Paris).

    Returns:
        Expression retournant la date maximale des événements RES/CFNS ou la fin par défaut

    Example:
        >>> df.group_by("ref_situation_contractuelle").agg(
        ...     expr_date_sortie_periode(horizon).alias("fin")
        ... )
    """
    fin_par_defaut = _horizon_expr(horizon).dt.month_start()

    return pl.when(expr_evenement_sortie()).then(pl.col("date_evenement")).max().fill_null(fin_par_defaut)


def generer_dates_facturation(lf: pl.LazyFrame, horizon: pl.Expr | dt.datetime | None = None) -> pl.LazyFrame:
    """
    Génère un LazyFrame des événements de facturation mensuels.

    Pour chaque Ref_Situation_Contractuelle, génère des événements FACTURATION
    au 1er de chaque mois entre la date d'entrée (exclue) et la date de sortie (incluse).

    Args:
        lf: LazyFrame contenant l'historique des événements
        horizon: borne de facturation (`datetime` Europe/Paris, `pl.Expr`, ou
            `None` → 1er du mois courant). Sert de fin par défaut des périodes
            ouvertes (PDL non résiliés) — l'unique source de temps, plus aucune
            lecture d'horloge ici (issue #179).

    Returns:
        LazyFrame contenant uniquement les événements FACTURATION artificiels
        avec les colonnes minimales (Ref, pdl, date_evenement, colonnes génériques)

    Example:
        >>> evenements = generer_dates_facturation(historique_lf, horizon)
        >>> print(evenements.collect())
    """
    # Fin par défaut = 1er du mois de l'horizon de facturation
    fin_defaut = _horizon_expr(horizon).dt.month_start()

    return (
        lf
        # Grouper par Ref pour calculer les périodes d'activité
        .group_by("ref_situation_contractuelle")
        .agg(
            [
                # Date d'entrée : min des événements CFNE/MES/PMES
                pl.col("date_evenement").filter(expr_evenement_entree()).min().alias("date_entree"),
                # Date de sortie : max des événements RES/CFNS ou défaut
                pl.col("date_evenement")
                .filter(expr_evenement_sortie())
                .max()
                .fill_null(fin_defaut)
                .alias("date_sortie"),
                # Garder le pdl pour la jointure finale
                pl.col("pdl").first(),
            ]
        )
        # Filtrer les Ref qui ont une période d'activité valide
        .filter(pl.col("date_entree").is_not_null())
        # Calculer la plage de mois et générer les 1ers du mois
        .with_columns(
            [
                # Premier mois de facturation = mois suivant l'entrée
                pl.col("date_entree").dt.month_start().dt.offset_by("1mo").alias("premier_mois"),
                # Dernier mois de facturation = mois de sortie
                pl.col("date_sortie").dt.month_start().alias("dernier_mois"),
            ]
        )
        # Filtrer les références avec des plages valides (premier_mois <= dernier_mois)
        .filter(pl.col("premier_mois") <= pl.col("dernier_mois"))
        # Générer les dates mensuelles en commençant par le bon mois
        # Note: convertir en dates avant date_ranges pour éviter les problèmes de timezone
        .with_columns(
            pl.date_ranges(
                start=pl.col("premier_mois").dt.date(),  # Convertir en date
                end=pl.col("dernier_mois").dt.date() + pl.duration(days=1),  # +1 pour inclure le dernier mois
                interval="1mo",
                eager=False,
            ).alias("dates_facturation")
        )
        # Explode pour avoir une ligne par date
        .explode("dates_facturation")
        .rename({"dates_facturation": "date_evenement"})
        # Reconvertir en datetime avec timezone pour cohérence avec l'historique d'entrée
        .with_columns(
            [
                # Garder la même précision datetime que l'historique d'origine
                pl.col("date_evenement").cast(pl.Datetime).dt.replace_time_zone("Europe/Paris")
            ]
        )
        # Filtrer : les dates générées sont déjà dans la bonne plage
        # Pas besoin de filtrer davantage car on génère directement du premier_mois au dernier_mois
        # Sélectionner et ajouter les colonnes nécessaires
        .select(["ref_situation_contractuelle", "pdl", "date_evenement"])
        # Ajouter les colonnes génériques de facturation
        .with_columns(**colonnes_evenement_facturation())
    )


def expr_colonnes_a_propager(columns: list[str] | None = None) -> list[pl.Expr]:
    """
    Expressions pour propager les colonnes contractuelles par forward fill.

    Retourne une liste d'expressions qui appliquent un forward_fill groupé
    sur les colonnes qui doivent être propagées depuis le dernier événement réel
    vers les événements artificiels de facturation.

    Args:
        columns: Liste optionnelle des colonnes disponibles pour filtrer

    Returns:
        Liste d'expressions avec forward_fill().over() pour chaque colonne à propager

    Example:
        >>> lf.with_columns(expr_colonnes_a_propager())
    """
    # Colonnes non-nullable du modèle qui doivent être propagées
    colonnes_obligatoires = [
        "segment_clientele",
        "etat_contractuel",
        "puissance_souscrite_kva",
        "formule_tarifaire_acheminement",
        "type_compteur",
        "num_compteur",
    ]

    # Colonnes optionnelles qui peuvent être propagées si présentes. Le statut de
    # communication (niveau d'ouverture + date de bascule, épique #313) se reporte par
    # RSC comme les autres données contractuelles : déclaratif C15, stable entre deux
    # événements, donc forward-fill sur les FACTURATION artificielles.
    colonnes_optionnelles = [
        "categorie",
        "ref_demandeur",
        "id_affaire",
        "niveau_ouverture_services",
        "date_changement_niveau_ouverture_services",
    ]

    # Filtrer selon les colonnes disponibles si fourni
    if columns is not None:
        colonnes_obligatoires = [col for col in colonnes_obligatoires if col in columns]
        colonnes_optionnelles = [col for col in colonnes_optionnelles if col in columns]

    # Créer les expressions de forward fill groupées par Ref
    expressions = []

    # Colonnes obligatoires
    for col in colonnes_obligatoires:
        expressions.append(pl.col(col).forward_fill().over("ref_situation_contractuelle"))

    # Colonnes optionnelles (forward fill standard)
    for col in colonnes_optionnelles:
        expressions.append(pl.col(col).forward_fill().over("ref_situation_contractuelle"))

    return expressions


def inserer_evenements_facturation(lf: pl.LazyFrame, horizon: pl.Expr | dt.datetime | None = None) -> pl.LazyFrame:
    """
    Insère des événements de facturation artificiels au 1er de chaque mois.

    Version Polars fonctionnelle de inserer_evenements_facturation avec approche par expressions :
    1. Génère les événements FACTURATION via group_by + explode de date_ranges
    2. Fusionne avec l'historique original
    3. Propage les données contractuelles par forward fill groupé

    Args:
        lf: LazyFrame contenant l'historique des événements
        horizon: borne de facturation propagée à `generer_dates_facturation`
            (cf. issue #179).

    Returns:
        LazyFrame enrichi avec les événements de facturation

    Example:
        >>> lf_enrichi = (
        ...     lf
        ...     .pipe(detecter_points_de_rupture)
        ...     .pipe(inserer_evenements_facturation, horizon)
        ... )
    """
    # Étape 1 : Générer les événements artificiels
    evenements_facturation = generer_dates_facturation(lf, horizon)

    # Étape 2 : Fusionner avec l'historique original
    fusioned = pl.concat([lf, evenements_facturation], how="diagonal_relaxed")

    # Étape 3 : Trier et propager les données contractuelles
    return (
        fusioned
        # Trier par Ref et Date pour le forward fill
        .sort(["ref_situation_contractuelle", "date_evenement"])
        .set_sorted("ref_situation_contractuelle")  # Indiquer explicitement que ref_situation_contractuelle est trié
        # Propager les colonnes contractuelles via expressions (avec colonnes disponibles)
        .with_columns(expr_colonnes_a_propager(columns=fusioned.collect_schema().names()))
    )


@pa.check_types(lazy=True)
def pipeline_historique(
    historique: pl.LazyFrame, horizon: pl.Expr | dt.datetime | None = None
) -> LazyFrame[Historique]:
    """
    Pipeline complet de production de l'Historique enrichi.

    Ce pipeline orchestre :
    1. La détection des points de rupture
    2. L'insertion des événements FACTURATION artificiels (jusqu'à l'horizon)
    3. Le filtrage des événements postérieurs à l'horizon

    L'`horizon` de facturation (issue #179) est l'**unique** paramètre temporel :
    il borne à la fois les événements retenus (`date_evenement <= horizon`) *et* la
    fin par défaut des périodes ouvertes (génération des FACTURATION). À horizon
    fixé, deux exécutions donnent une sortie identique, indépendamment de l'heure
    murale — le pipeline est une transformation pure (ADR-0019). Il remplace et
    unifie l'ancien `date_limite` (qui ne bornait que les événements) avec le défaut
    de fin de période, supprimant le bug à deux fuseaux décrit en #179.

    Args:
        historique: LazyFrame contenant les événements contractuels bruts (sortie de `c15()`)
        horizon: borne de facturation, un `datetime` en Europe/Paris (ou `pl.Expr`).
            `None` → 1er du mois courant, résolu *une seule fois* en vraie heure de
            Paris puis propagé partout. Au boundary I/O (build `contexte_mensuel`),
            l'horizon est résolu explicitement et passé en argument.

    Returns:
        LazyFrame validé par le modèle `Historique`

    Example:
        >>> from datetime import datetime
        >>> from zoneinfo import ZoneInfo
        >>> import polars as pl
        >>>
        >>> # Défaut : 1er du mois courant (heure de Paris)
        >>> enrichi = pipeline_historique(historique_lf)
        >>>
        >>> # Avec horizon explicite (déterministe)
        >>> horizon = datetime(2024, 5, 1, tzinfo=ZoneInfo("Europe/Paris"))
        >>> enrichi = pipeline_historique(historique_lf, horizon)
    """
    # Résolution unique de l'horizon (défaut 1er du mois courant si None) : la même
    # expression borne les événements et alimente la fin de période par défaut.
    horizon_expr = _horizon_expr(horizon)

    historique_filtre = historique.filter(pl.col("date_evenement") <= horizon_expr)

    # Pipeline : détection ruptures + insertion événements facturation (bornés par l'horizon)
    return historique_filtre.pipe(detecter_points_de_rupture).pipe(inserer_evenements_facturation, horizon_expr)
