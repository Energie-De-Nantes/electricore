"""
Fonctions helpers pour DuckDB - API fonctionnelle.

Ce module fournit des shortcuts pour créer des DuckDBQuery sur les flux
Enedis les plus courants, ainsi que des utilitaires pour interagir avec DuckDB.

Toutes les fonctions factory sont pures : elles prennent des paramètres
optionnels et retournent un DuckDBQuery configuré.
"""

from pathlib import Path

import polars as pl

from .config import DuckDBConfig, duckdb_readonly_conn
from .descriptor import FluxDescriptor
from .query import DuckDBQuery, make_query
from .registry import FLUX_DESCRIPTORS, FluxInconnu
from .transforms import transform_dates

# =============================================================================
# API FLUIDE - FONCTIONS FACTORY PAR FLUX
# =============================================================================


def flux(nom: str, database_path: str | Path | None = None) -> DuckDBQuery:
    """Crée un DuckDBQuery pour un flux Enedis enregistré (résolution registre).

    Point d'entrée dynamique : résout `nom` dans `FLUX_DESCRIPTORS` et retourne le
    builder configuré. Pour les 5 flux Enedis (`c15`, `r151`, `r15`, `f15`,
    `r64`) ; le modèle de relevés canonique a sa propre factory `releves()`
    (ADR-0029), hors périmètre.

    Args:
        nom: Nom court du flux (clé de `FLUX_DESCRIPTORS`).
        database_path: Chemin vers la base DuckDB (optionnel).

    Returns:
        DuckDBQuery configuré pour le flux demandé.

    Raises:
        FluxInconnu: Si `nom` n'est pas un flux enregistré (le message nomme
            les flux disponibles). Le loader reste agnostique HTTP — c'est au
            caller transport de mapper sur un 404.

    Example:
        >>> df = flux("c15").filter({"pdl": ["PDL123"]}).limit(100).collect()
    """
    if nom not in FLUX_DESCRIPTORS:
        raise FluxInconnu(nom, sorted(FLUX_DESCRIPTORS))
    return make_query(FLUX_DESCRIPTORS[nom], database_path)


def c15(database_path: str | Path | None = None) -> DuckDBQuery:
    """
    Crée un DuckDBQuery pour les données flux C15 (historique périmètre).

    Args:
        database_path: Chemin vers la base DuckDB (optionnel)

    Returns:
        DuckDBQuery configuré pour flux C15

    Example:
        >>> # Récupérer les événements récents
        >>> df = c15().filter({"Date_Evenement": ">= '2024-01-01'"}).limit(100).collect()
        >>>
        >>> # Filtrer par PDL spécifiques
        >>> lazy_df = c15().filter({"pdl": ["PDL123", "PDL456"]}).lazy()
    """
    return flux("c15", database_path)


def r151(database_path: str | Path | None = None) -> DuckDBQuery:
    """
    Crée un DuckDBQuery pour les données flux R151 (relevés périodiques).

    Args:
        database_path: Chemin vers la base DuckDB (optionnel)

    Returns:
        DuckDBQuery configuré pour flux R151

    Example:
        >>> # Relevés récents avec limite
        >>> df = r151().filter({"date_releve": ">= '2024-01-01'"}).limit(1000).collect()
    """
    return flux("r151", database_path)


def r15(database_path: str | Path | None = None) -> DuckDBQuery:
    """
    Crée un DuckDBQuery pour les données flux R15 (relevés avec événements).

    Args:
        database_path: Chemin vers la base DuckDB (optionnel)

    Returns:
        DuckDBQuery configuré pour flux R15

    Example:
        >>> # Relevés avec situation contractuelle spécifique
        >>> df = r15().filter({"ref_situation_contractuelle": "REF123"}).collect()
    """
    return flux("r15", database_path)


def f15(database_path: str | Path | None = None) -> DuckDBQuery:
    """
    Crée un DuckDBQuery pour les données flux F15 (factures détaillées).

    Args:
        database_path: Chemin vers la base DuckDB (optionnel)

    Returns:
        DuckDBQuery configuré pour flux F15

    Example:
        >>> # Factures pour un PDL spécifique
        >>> df = f15().filter({"pdl": "PDL123"}).collect()
        >>>
        >>> # Factures sur une période
        >>> df = f15().filter({"date_facture": ">= '2024-01-01'"}).limit(100).collect()
    """
    return flux("f15", database_path)


def r64(database_path: str | Path | None = None) -> DuckDBQuery:
    """
    Crée un DuckDBQuery pour les données flux R64 (relevés JSON timeseries).

    Args:
        database_path: Chemin vers la base DuckDB (optionnel)

    Returns:
        DuckDBQuery configuré pour flux R64

    Example:
        >>> # Relevés R64 récents
        >>> df = r64().filter({"date_releve": ">= '2024-01-01'"}).limit(100).collect()
        >>>
        >>> # Filtrer par PDL et type de relevé
        >>> df = r64().filter({"pdl": "PDL123", "type_releve": "AQ"}).collect()
    """
    return flux("r64", database_path)


def affaires(database_path: str | Path | None = None) -> DuckDBQuery:
    """
    Crée un DuckDBQuery pour les affaires SGE (flux X12/X13, suivi opérationnel).

    Grain : un jalon d'avancement par ligne (dédupliqué sur (affaire_id, jalon_num)
    côté dbt). Lecture seule ; la vue « affaires ouvertes » (rollup non soldées +
    ancienneté) se calcule à la lecture via `core.pipelines.affaires.affaires_ouvertes`.

    Args:
        database_path: Chemin vers la base DuckDB (optionnel)

    Returns:
        DuckDBQuery configuré pour flux_affaires

    Example:
        >>> # Affaires initiées encore en cours
        >>> df = affaires().filter({"origine": "initiee", "statut": "COURS"}).collect()
    """
    return flux("affaires", database_path)


def releves(database_path: str | Path | None = None) -> DuckDBQuery:
    """
    Crée un DuckDBQuery pour le modèle de relevés canonique `releves` (ADR-0029).

    Modèle dbt transverse : union des sources de relevés harmonisées (C15, R64, R151),
    dédoublonnées même-source par clé métier. Depuis la bascule relevés canoniques
    (#248), c'est **l'unique** façon de lire les relevés côté cœur — l'ancien couple
    `releves()` (R151+R15) / `releves_harmonises()` (R151+R64), dont l'union vivait en
    SQL ici, a été retiré : l'arbitrage des sources est désormais porté par dbt.

    C'est l'entrée (côté cœur) de la chronologie qui arbitre la priorité des sources
    (C15 > R64 > R151), sélectionne les bornes de facturation et flag les manquants.

    Pas de validation Pandera ici : le contrat est porté par la chronologie en aval
    (`ChronologieReleves`), pas par le loader.

    Args:
        database_path: Chemin vers la base DuckDB (optionnel)

    Returns:
        DuckDBQuery configuré pour le modèle `releves`

    Example:
        >>> df = releves().collect()
        >>> r64_only = releves().filter({"source": "flux_R64"}).collect()
    """
    # `SELECT *` strict : le loader ne connaît aucun type (ADR-0035). Les index sont
    # matérialisés en BIGINT (kWh entiers, ADR-0034) par dbt et l'aval les lit en Int64 ;
    # le re-cast Float64 qui vivait ici est retiré. La parité dbt↔Pandera (RelevéIndex)
    # est prouvée par `test_releves_dbt_respecte_le_contrat_pandera`, plus assurée par ce
    # cast — c'est le garde-fou de frontière qui remplace la 3ᵉ déclaration de type.
    config = FluxDescriptor(
        flux_name="RELEVES_CANONIQUES",
        base_sql="SELECT * FROM flux_enedis.releves",
        transform=None,
        validator=None,
    )
    return DuckDBQuery(config=config, database_path=database_path)


def spine(database_path: str | Path | None = None) -> DuckDBQuery:
    """Crée un DuckDBQuery pour le mart *spine* de la Chronologie du contrat (ADR-0041).

    Spine relationnelle assemblée en dbt (Class-Table Inheritance) : épine commune
    `(pdl, ref_situation_contractuelle, date_evenement, source, type_fait)` des faits d'une
    RSC — événements C15 ∪ grille FACTURATION (1ᵉʳ de chaque mois) — avec les attributs de
    **situation** (FTA, puissance, niveau d'ouverture, segment…) forward-fillés **en SQL**
    sur la timeline d'événements complète. C'est le **substrat** dont dérivent les branches
    abonnement (#378) et énergie (via la *Chronologie des relevés*, #376/#377).

    L'**horizon** reste un *filtre* côté cœur (`date_evenement <= horizon`) : la spine est
    pré-générée jusqu'à une borne généreuse, le cœur filtre (pureté #179 préservée).

    Le loader reste fin (ADR-0019) : `SELECT *`, seul `date_evenement` est harmonisé en
    Europe/Paris (instant préservé) pour un dtype stable quel que soit le fuseau de session.
    Le contrat est porté par `SpineContrat` (validation activée par défaut).

    Args:
        database_path: Chemin vers la base DuckDB (optionnel)

    Returns:
        DuckDBQuery configuré pour le mart `spine_contrat`

    Example:
        >>> df = spine().collect()
        >>> facturation = spine().filter({"type_fait": "facturation"}).collect()
    """
    from electricore.core.models.spine_contrat import SpineContrat

    config = FluxDescriptor(
        flux_name="SPINE_CONTRAT",
        base_sql="SELECT * FROM flux_enedis.spine_contrat",
        transform=transform_dates(("date_evenement",)),
        validator=SpineContrat,
    )
    return DuckDBQuery(config=config, database_path=database_path)


def chronologie(database_path: str | Path | None = None) -> DuckDBQuery:
    """Crée un DuckDBQuery pour le mart *Chronologie des relevés* (ADR-0041, #376).

    Projection ÉNERGIE de la spine, assemblée entièrement en dbt : relevés contractuels
    C15 aux événements qui impactent l'énergie + bornes FACTURATION mensuelles appariées
    aux relevés périodiques (R151/R64) au **grain JOUR** (equi-join `(pdl, jour)` qui
    remplace l'asof « nearest 4h » du cœur), dédoublonnées par priorité de source
    (C15 > R64 > R151, ADR-0028) via `QUALIFY`. Grain : 1 ligne par
    `(ref_situation_contractuelle, date_releve, ordre_index)`.

    Read fin (ADR-0019) : `SELECT *`, `date_releve` harmonisé Europe/Paris (instant
    préservé). Contrat `ChronologieReleves` (validation activée). Remplace l'assemblage
    interne `_assembler_chronologie` (retiré du chemin énergie en #377).

    Args:
        database_path: Chemin vers la base DuckDB (optionnel)

    Returns:
        DuckDBQuery configuré pour le mart `chronologie_releves`

    Example:
        >>> df = chronologie().collect()
    """
    from electricore.core.models.chronologie_releves import ChronologieReleves

    config = FluxDescriptor(
        flux_name="CHRONOLOGIE_RELEVES",
        base_sql="SELECT * FROM flux_enedis.chronologie_releves",
        transform=transform_dates(("date_releve",)),
        validator=ChronologieReleves,
    )
    return DuckDBQuery(config=config, database_path=database_path)


# =============================================================================
# UTILITAIRES
# =============================================================================


def get_available_tables(database_path: str | Path | None = None) -> list[str]:
    """
    Liste les tables disponibles dans la base DuckDB.

    Args:
        database_path: Chemin vers la base DuckDB

    Returns:
        Liste des noms de tables avec schéma (ex: ["flux_enedis.flux_c15"])
    """
    config = DuckDBConfig.from_path(database_path)

    with duckdb_readonly_conn(config.database_path) as conn:
        result = conn.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema != 'information_schema'
            ORDER BY table_schema, table_name
        """).fetchall()

        return [f"{schema}.{table}" for schema, table in result]


def execute_custom_query(
    query: str, database_path: str | Path | None = None, lazy: bool = True
) -> pl.DataFrame | pl.LazyFrame:
    """
    Exécute une requête SQL personnalisée sur DuckDB.

    Args:
        query: Requête SQL à exécuter
        database_path: Chemin vers la base DuckDB
        lazy: Si True, retourne un LazyFrame, sinon un DataFrame

    Returns:
        DataFrame ou LazyFrame selon le paramètre lazy
    """
    config = DuckDBConfig.from_path(database_path)

    with duckdb_readonly_conn(config.database_path) as conn:
        if lazy:
            return pl.read_database(query=query, connection=conn).lazy()
        else:
            return pl.read_database(query=query, connection=conn)
