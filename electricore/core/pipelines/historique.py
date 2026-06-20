"""
Expressions Polars pour le pipeline historique (branche abonnement).

Ce module contient des expressions composables suivant la philosophie
fonctionnelle de Polars. Les expressions sont des transformations pures
qui peuvent être composées entre elles.

Depuis ADR-0041 (#378), `pipeline_historique` est *rétréci* : il consomme la **spine**
de la Chronologie du contrat (mart `spine_contrat`, loader `spine()`), déjà forward-fillée
et augmentée de la grille FACTURATION mensuelle **en dbt** (#375), et ne fait plus que
(1) **filtrer l'horizon** et (2) **détecter les ruptures d'abonnement**. La génération des
FACTURATION, le forward-fill de situation et la détection énergie (`impacte_energie`,
ruptures calendrier/index) ont quitté le cœur — portés par dbt (spine + *Chronologie des
relevés*, #375/#377).

Horizon de facturation (issue #179) : l'`horizon` (un `datetime` en Europe/Paris) reste un
**filtre** (`date_evenement <= horizon`). La spine pré-génère la grille FACTURATION jusqu'à
une borne généreuse — l'horizon ne génère rien, il borne. Aucune lecture d'horloge n'a lieu
dans le cœur pur : la résolution du défaut « 1er du mois courant » est faite une seule fois
au boundary I/O (cf. `core/builds/contexte_mensuel.py`, ADR-0019).
"""

import datetime as dt
from zoneinfo import ZoneInfo

import pandera.polars as pa
import polars as pl
from pandera.typing.polars import LazyFrame

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


def expr_impacte_abonnement(over: str = "ref_situation_contractuelle") -> pl.Expr:
    """
    Détecte si un changement impacte l'abonnement.

    Un changement impacte l'abonnement s'il y a :
    - Un changement de puissance souscrite OU
    - Un changement de formule tarifaire d'acheminement (FTA) OU
    - Un événement structurant (entrée/sortie du périmètre)

    Détection *pure* de rupture sur les vrais événements. Les bornes FACTURATION de la
    grille mensuelle (spine) sont des bornes de période par construction, pas des ruptures :
    leur prise en compte est portée par `detecter_points_de_rupture` (qui connaît la spine),
    pas par cette expression.

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


def expr_resume_modification() -> pl.Expr:
    """
    Génère un résumé textuel des modifications d'abonnement détectées.

    Compose les résumés de changements de la **branche abonnement** (puissance, FTA)
    en une description lisible. Les résumés calendrier/index (branche énergie) ne vivent
    plus ici : l'énergie est assemblée en dbt (*Chronologie des relevés*, ADR-0041 #377)
    et ne passe plus par `pipeline_historique`.

    Returns:
        Expression Polars retournant une chaîne de caractères décrivant les modifications

    Example:
        >>> df.with_columns(
        ...     expr_resume_modification().alias("resume_modification")
        ... )
        # Produit: "P: 6.0 → 9.0, FTA: BTINFCU4 → BTINFMU4"
    """
    resume_puissance = expr_resume_changement("puissance_souscrite_kva", "P")
    resume_fta_shift = expr_resume_changement("formule_tarifaire_acheminement", "FTA")

    # Combiner les résumés non-vides avec ", "
    resumes = [resume_puissance, resume_fta_shift]
    return pl.concat_list(resumes).list.drop_nulls().list.eval(pl.element().filter(pl.element() != "")).list.join(", ")


def detecter_points_de_rupture(spine: pl.LazyFrame) -> pl.LazyFrame:
    """
    Détecte les ruptures d'**abonnement** sur la spine de la Chronologie du contrat.

    Depuis ADR-0041 (#378), l'entrée est la **spine** (mart `spine_contrat`, déjà
    forward-fillée + grille FACTURATION en dbt, #375) : cette fonction ne fait plus que
    la détection de ruptures de la branche abonnement (changement puissance/FTA, événement
    structurant, bornes FACTURATION). La détection énergie (`impacte_energie`, ruptures
    calendrier/index) a quitté le cœur — elle est portée par la *Chronologie des relevés*
    (#376/#377).

    Étapes :
    1. Tri par RSC et date d'événement (départage : événement avant FACTURATION au même
       instant, comme le forward-fill de la spine)
    2. Colonnes `avant_*` (puissance/FTA) via window functions
    3. Détection `impacte_abonnement` + résumé textuel des modifications

    Args:
        spine: LazyFrame de la spine (épine + situation forward-fillée)

    Returns:
        LazyFrame enrichi avec `avant_puissance_souscrite`, `avant_formule_tarifaire_acheminement`,
        `impacte_abonnement`, `resume_modification`

    Example:
        >>> historique_enrichi = detecter_points_de_rupture(spine().lazy()).collect()
    """
    return (
        spine.sort(
            ["ref_situation_contractuelle", "date_evenement"],
            # Départage des ex-aequo de timestamp : événement avant FACTURATION, comme le
            # forward-fill SQL de la spine (garde-fou #374 : pas de collision de situation).
            maintain_order=True,
        )
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
        # Appliquer la détection d'impact abonnement avec nos expressions pures. Les bornes
        # FACTURATION (forward-fillées, donc « sans changement ») impactent toujours
        # l'abonnement : ce sont des bornes de période — on les force ici (concern de la spine).
        .with_columns(
            [
                (expr_impacte_abonnement() | (pl.col("evenement_declencheur") == "FACTURATION")).alias(
                    "impacte_abonnement"
                ),
                expr_resume_modification().alias("resume_modification"),
            ]
        )
    )


@pa.check_types(lazy=True)
def pipeline_historique(spine: pl.LazyFrame, horizon: pl.Expr | dt.datetime | None = None) -> LazyFrame[Historique]:
    """
    Pipeline *rétréci* de production de l'Historique enrichi (branche abonnement).

    Depuis ADR-0041 (#378), `pipeline_historique` ne ré-assemble plus rien : il consomme
    la **spine** (mart `spine_contrat`, loader `spine()`), déjà forward-fillée et augmentée
    de la grille FACTURATION mensuelle **en dbt** (#375). Il ne fait plus que :
    1. **Filtrer l'horizon** : `date_evenement <= horizon` (l'horizon reste un *filtre*,
       pureté #179 — la grille FACTURATION de la spine va jusqu'à une borne généreuse) ;
    2. **Détecter les ruptures d'abonnement** (`detecter_points_de_rupture`) : changement
       puissance/FTA, événement structurant, bornes FACTURATION.

    La génération des FACTURATION, le forward-fill de situation et la détection énergie
    ont quitté le cœur (portés par dbt — spine + *Chronologie des relevés*, #375/#377).

    Args:
        spine: LazyFrame de la spine de la Chronologie du contrat (sortie de `spine()`),
            horizon-indépendante.
        horizon: borne de facturation, un `datetime` en Europe/Paris (ou `pl.Expr`).
            `None` → 1er du mois courant, résolu *une seule fois* en vraie heure de Paris.
            Au boundary I/O (build `contexte_mensuel`), l'horizon est résolu explicitement
            et passé en argument.

    Returns:
        LazyFrame validé par le modèle `Historique`

    Example:
        >>> from datetime import datetime
        >>> from zoneinfo import ZoneInfo
        >>> from electricore.core.loaders import spine
        >>>
        >>> # Avec horizon explicite (déterministe)
        >>> horizon = datetime(2024, 5, 1, tzinfo=ZoneInfo("Europe/Paris"))
        >>> enrichi = pipeline_historique(spine().lazy(), horizon)
    """
    horizon_expr = _horizon_expr(horizon)

    return spine.filter(pl.col("date_evenement") <= horizon_expr).pipe(detecter_points_de_rupture)
