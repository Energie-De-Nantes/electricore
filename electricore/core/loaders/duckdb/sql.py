"""
Génération SQL fonctionnelle pour requêtes DuckDB.

Ce module fournit les primitives pour construire des requêtes SQL de manière
fonctionnelle et type-safe avec dataclasses immutables.

Au lieu de constantes SQL de 300 lignes, ce module génère le SQL
depuis des définitions structurées.
"""

from dataclasses import dataclass
from typing import Dict, Optional


# =============================================================================
# DATACLASSES IMMUTABLES POUR DÉFINITION SQL
# =============================================================================

@dataclass(frozen=True)  # Immutable
class Column:
    """
    Définition immutable d'une colonne SQL.

    Attributes:
        name: Nom de la colonne dans le résultat
        sql_expr: Expression SQL pour cette colonne
        alias: Alias optionnel (si différent du name)
    """
    name: str
    sql_expr: str
    alias: Optional[str] = None

    def to_sql(self) -> str:
        """Convertit en fragment SQL SELECT."""
        # Si l'expression SQL est différente du nom, ajouter l'alias
        if self.sql_expr != self.name:
            alias_name = self.alias if self.alias else self.name
            return f"{self.sql_expr} as {alias_name}"
        # Sinon, utiliser directement le nom (colonne simple)
        return self.sql_expr


@dataclass(frozen=True)  # Immutable
class FluxSchema:
    """
    Schéma immutable complet d'un flux Enedis.

    Attributes:
        flux_name: Nom du flux (ex: "C15", "R151")
        table: Nom complet de la table DuckDB
        columns: Tuple immutable de colonnes
        where_clause: Clause WHERE optionnelle
        comments: Commentaires SQL optionnels
    """
    flux_name: str
    table: str
    columns: tuple[Column, ...]
    where_clause: Optional[str] = None
    comments: Optional[str] = None


# =============================================================================
# FONCTIONS PURES DE GÉNÉRATION SQL
# =============================================================================

def build_select_clause(columns: tuple[Column, ...]) -> str:
    """
    Construit la clause SELECT depuis une liste de colonnes.

    Fonction pure : Fn(tuple[Column]) -> str

    Args:
        columns: Tuple de définitions de colonnes

    Returns:
        Clause SELECT formatée
    """
    return ",\n    ".join(col.to_sql() for col in columns)


def build_base_query(schema: FluxSchema) -> str:
    """
    Construit la requête SQL complète depuis un schéma.

    Fonction pure : Fn(FluxSchema) -> str

    Args:
        schema: Schéma de flux complet

    Returns:
        Requête SQL complète formatée
    """
    # Construire les parties de la requête
    parts = []

    # Commentaires optionnels
    if schema.comments:
        parts.append(schema.comments)

    # Clause SELECT
    select = build_select_clause(schema.columns)
    parts.append(f"SELECT\n    {select}")

    # Clause FROM
    parts.append(f"FROM {schema.table}")

    # Clause WHERE optionnelle
    if schema.where_clause:
        parts.append(f"WHERE {schema.where_clause}")

    return "\n".join(parts)


# =============================================================================
# HELPERS POUR CONSTRUIRE DES COLONNES COMMUNES
# =============================================================================

def col_simple(name: str) -> Column:
    """Colonne simple sans transformation."""
    return Column(name=name, sql_expr=name)


def col_cast_double(name: str) -> Column:
    """Colonne castée en DOUBLE."""
    return Column(name=name, sql_expr=f"CAST({name} AS DOUBLE)")


def col_cast_timestamp(name: str) -> Column:
    """Colonne castée en TIMESTAMP."""
    return Column(name=name, sql_expr=f"CAST({name} AS TIMESTAMP)")


def col_cast_null_varchar(alias: str) -> Column:
    """Colonne NULL castée en VARCHAR avec alias."""
    return Column(name=alias, sql_expr="CAST(NULL AS VARCHAR)", alias=alias)


def col_literal(value: str, alias: str) -> Column:
    """Colonne littérale avec alias."""
    return Column(name=alias, sql_expr=f"'{value}'", alias=alias)


def col_literal_bool(value: bool, alias: str) -> Column:
    """Colonne booléenne littérale."""
    sql_value = "TRUE" if value else "FALSE"
    return Column(name=alias, sql_expr=sql_value, alias=alias)


# =============================================================================
# DÉFINITIONS DES SCHÉMAS PAR FLUX (Immutables)
# =============================================================================

# Flux C15 - Historique périmètre contractuel
SCHEMA_C15 = FluxSchema(
    flux_name="C15",
    table="flux_enedis.flux_c15",
    columns=(
        # Colonnes principales
        col_simple("date_evenement"),
        col_simple("pdl"),
        col_simple("ref_situation_contractuelle"),
        col_simple("segment_clientele"),
        col_simple("etat_contractuel"),
        col_simple("evenement_declencheur"),
        col_simple("type_evenement"),
        col_simple("categorie"),
        col_cast_double("puissance_souscrite"),
        col_simple("formule_tarifaire_acheminement"),
        col_simple("type_compteur"),
        col_simple("num_compteur"),
        col_simple("ref_demandeur"),
        col_simple("id_affaire"),
        # Relevés "Avant"
        col_simple("avant_date_releve"),
        col_simple("avant_nature_index"),
        col_simple("avant_id_calendrier_fournisseur"),
        col_simple("avant_id_calendrier_distributeur"),
        col_cast_double("avant_hp"),
        col_cast_double("avant_hc"),
        col_cast_double("avant_hch"),
        col_cast_double("avant_hph"),
        col_cast_double("avant_hpb"),
        col_cast_double("avant_hcb"),
        col_cast_double("avant_base"),
        # Relevés "Après"
        col_simple("apres_date_releve"),
        col_simple("apres_nature_index"),
        col_simple("apres_id_calendrier_fournisseur"),
        col_simple("apres_id_calendrier_distributeur"),
        col_cast_double("apres_hp"),
        col_cast_double("apres_hc"),
        col_cast_double("apres_hch"),
        col_cast_double("apres_hph"),
        col_cast_double("apres_hpb"),
        col_cast_double("apres_hcb"),
        col_cast_double("apres_base"),
        # Métadonnées
        col_literal("flux_C15", "source"),
    )
)


# Flux R151 - Relevés périodiques avec harmonisation date
SCHEMA_R151 = FluxSchema(
    flux_name="R151",
    table="flux_enedis.flux_r151",
    columns=(
        # Date avec harmonisation (convention fin de journée → début de journée)
        Column(
            name="date_releve",
            sql_expr="CAST(date_releve AS TIMESTAMP) + INTERVAL '1 day'",
            alias="date_releve"
        ),
        col_simple("pdl"),
        col_cast_null_varchar("ref_situation_contractuelle"),
        col_cast_null_varchar("formule_tarifaire_acheminement"),
        col_simple("id_calendrier_fournisseur"),
        col_simple("id_calendrier_distributeur"),
        col_simple("id_affaire"),
        # Cadrans
        col_cast_double("hp"),
        col_cast_double("hc"),
        col_cast_double("hch"),
        col_cast_double("hph"),
        col_cast_double("hpb"),
        col_cast_double("hcb"),
        col_cast_double("base"),
        # Métadonnées
        col_literal("flux_R151", "source"),
        col_literal_bool(False, "ordre_index"),
        col_simple("unite"),
        Column(name="precision", sql_expr="unite", alias="precision"),
    ),
    where_clause="date_releve IS NOT NULL",
    comments="""-- HARMONISATION DES CONVENTIONS DE DATE
-- Problème : R151 utilise convention "fin de journée" (date J = index fin jour J)
--           alors que R64, R15, C15 utilisent "début de journée" (date J = index début jour J)
-- Solution : R151 date J → J+1 pour aligner sur convention majoritaire "début de journée"
-- Résultat : après ajustement, R151 et R64 correspondent parfaitement (244 matches exacts testés)"""
)


# Flux R15 - Relevés avec événements
SCHEMA_R15 = FluxSchema(
    flux_name="R15",
    table="flux_enedis.flux_r15",
    columns=(
        col_simple("date_releve"),
        col_simple("pdl"),
        col_simple("ref_situation_contractuelle"),
        col_cast_null_varchar("formule_tarifaire_acheminement"),
        col_cast_null_varchar("id_calendrier_fournisseur"),
        Column(name="id_calendrier_distributeur", sql_expr="id_calendrier", alias="id_calendrier_distributeur"),
        col_simple("id_affaire"),
        # Cadrans
        col_cast_double("hp"),
        col_cast_double("hc"),
        col_cast_double("hch"),
        col_cast_double("hph"),
        col_cast_double("hpb"),
        col_cast_double("hcb"),
        col_cast_double("base"),
        # Métadonnées
        col_literal("flux_R15", "source"),
        col_literal_bool(False, "ordre_index"),
        col_literal("kWh", "unite"),
        col_literal("kWh", "precision"),
    ),
    where_clause="date_releve IS NOT NULL"
)


# Flux F15 - Factures détaillées
SCHEMA_F15 = FluxSchema(
    flux_name="F15",
    table="flux_enedis.flux_f15_detail",
    columns=(
        col_cast_timestamp("date_facture"),
        col_simple("pdl"),
        col_simple("num_facture"),
        col_simple("type_facturation"),
        col_simple("ref_situation_contractuelle"),
        col_simple("type_compteur"),
        col_simple("id_ev"),
        col_simple("nature_ev"),
        col_simple("formule_tarifaire_acheminement"),
        col_simple("taux_tva_applicable"),
        col_simple("unite"),
        col_cast_double("prix_unitaire"),
        col_cast_double("quantite"),
        col_cast_double("montant_ht"),
        col_cast_timestamp("date_debut"),
        col_cast_timestamp("date_fin"),
        col_simple("libelle_ev"),
        col_literal("flux_F15", "source"),
    )
)


# Flux R64 - Relevés JSON timeseries
SCHEMA_R64 = FluxSchema(
    flux_name="R64",
    table="flux_enedis.flux_r64",
    columns=(
        col_cast_timestamp("date_releve"),
        col_simple("pdl"),
        col_simple("etape_metier"),
        col_simple("contexte_releve"),
        col_simple("type_releve"),
        col_simple("grandeur_physique"),
        col_simple("grandeur_metier"),
        col_simple("unite"),
        # Métadonnées header
        col_simple("id_demande"),
        col_simple("si_demandeur"),
        col_simple("code_flux"),
        col_simple("format"),
        # Cadrans (format WIDE)
        col_cast_double("hpb"),
        col_cast_double("hph"),
        col_cast_double("hch"),
        col_cast_double("hcb"),
        col_cast_double("hp"),
        col_cast_double("hc"),
        col_cast_double("base"),
        # Métadonnées système
        col_simple("modification_date"),
        col_simple("_source_zip"),
        col_simple("_flux_type"),
        col_simple("_json_name"),
        col_literal("flux_R64", "source"),
    ),
    where_clause="date_releve IS NOT NULL"
)


# =============================================================================
# REGISTRE FONCTIONNEL (Mapping immutable)
# =============================================================================

FLUX_SCHEMAS: Dict[str, FluxSchema] = {
    "c15": SCHEMA_C15,
    "r151": SCHEMA_R151,
    "r15": SCHEMA_R15,
    "f15": SCHEMA_F15,
    "r64": SCHEMA_R64,
}


# =============================================================================
# REQUÊTES COMPOSÉES (UNION)
# =============================================================================

# Note: Les requêtes UNION (releves, releves_harmonises) sont construites
# comme des schémas spéciaux ou via composition de requêtes.
# Pour l'instant, on les garde comme constantes SQL simples jusqu'à
# avoir un besoin de les paramétrer davantage.

BASE_QUERY_RELEVES_UNIFIES = """
WITH releves_unifies AS (
    -- Flux R151 (relevés périodiques)
    SELECT
        CAST(date_releve AS TIMESTAMP) as date_releve,
        pdl,
        CAST(NULL AS VARCHAR) as ref_situation_contractuelle,
        CAST(NULL AS VARCHAR) as formule_tarifaire_acheminement,
        id_calendrier_fournisseur,
        id_calendrier_distributeur,
        id_affaire,
        CAST(hp AS DOUBLE) as hp,
        CAST(hc AS DOUBLE) as hc,
        CAST(hch AS DOUBLE) as hch,
        CAST(hph AS DOUBLE) as hph,
        CAST(hpb AS DOUBLE) as hpb,
        CAST(hcb AS DOUBLE) as hcb,
        CAST(base AS DOUBLE) as base,
        'flux_R151' as source,
        FALSE as ordre_index,
        unite,
        unite as precision
    FROM flux_enedis.flux_r151
    WHERE date_releve IS NOT NULL

    UNION ALL

    -- Flux R15 (relevés avec événements)
    SELECT
        date_releve,
        pdl,
        ref_situation_contractuelle,
        CAST(NULL AS VARCHAR) as formule_tarifaire_acheminement,
        CAST(NULL AS VARCHAR) as id_calendrier_fournisseur,
        id_calendrier as id_calendrier_distributeur,
        id_affaire,
        CAST(hp AS DOUBLE) as hp,
        CAST(hc AS DOUBLE) as hc,
        CAST(hch AS DOUBLE) as hch,
        CAST(hph AS DOUBLE) as hph,
        CAST(hpb AS DOUBLE) as hpb,
        CAST(hcb AS DOUBLE) as hcb,
        CAST(base AS DOUBLE) as base,
        'flux_R15' as source,
        FALSE as ordre_index,
        'kWh' as unite,
        'kWh' as precision
    FROM flux_enedis.flux_r15
    WHERE date_releve IS NOT NULL
)
SELECT * FROM releves_unifies
"""


BASE_QUERY_RELEVES_HARMONISES = """
-- HARMONISATION DES RELEVÉS D'INDEX ENEDIS
-- ===========================================
--
-- PROBLÉMATIQUE DES CONVENTIONS DE DATE :
-- • R151 : convention "fin de journée" (date J = index mesuré en fin de jour J)
-- • R64  : convention "début de journée" (date J = index mesuré en début de jour J)
-- • Écart systématique : R64 ≈ 0.4% < R151 sur même PDL+date sans harmonisation
--
-- SOLUTION ADOPTÉE :
-- • Harmonisation vers convention majoritaire "début de journée"
-- • Ajustement R151 : date J → J+1 (fin jour J devient début jour J+1)
-- • Résultat validé : 244 correspondances exactes R151/R64 après harmonisation
--
-- EXCLUSIONS :
-- • R15 exclu (cause erreurs TURPE sur clients professionnels)
--
WITH releves_harmonises AS (
    -- Flux R151 (relevés périodiques) - HARMONISATION DATE
    -- Convention R151 : date J = fin jour J
    -- Convention harmonisée : date J = début jour J
    -- Donc R151 : date J → J+1 pour harmoniser
    SELECT
        CAST(date_releve AS TIMESTAMP) + INTERVAL '1 day' as date_releve,
        pdl,
        CAST(false AS BOOLEAN) as ordre_index,
        CAST(hp AS DOUBLE) as hp,
        CAST(hc AS DOUBLE) as hc,
        CAST(hpb AS DOUBLE) as hpb,
        CAST(hcb AS DOUBLE) as hcb,
        CAST(hph AS DOUBLE) as hph,
        CAST(hch AS DOUBLE) as hch,
        CAST(base AS DOUBLE) as base,
        unite,
        unite as precision,
        CAST('flux_R151' AS VARCHAR) as source,
        id_calendrier_distributeur,
        id_calendrier_fournisseur,
        CAST(NULL AS VARCHAR) as ref_situation_contractuelle,
        id_affaire,
        CAST(NULL AS VARCHAR) as type_releve,
        CAST(NULL AS VARCHAR) as contexte_releve,
        CAST(NULL AS VARCHAR) as etape_metier,
        CAST(NULL AS VARCHAR) as grandeur_physique,
        CAST(NULL AS VARCHAR) as grandeur_metier,
        'R151' as flux_origine,
        CAST(true AS BOOLEAN) as date_ajustee
    FROM flux_enedis.flux_r151
    WHERE date_releve IS NOT NULL

    UNION ALL

    -- Flux R64 (relevés JSON timeseries)
    SELECT
        CAST(date_releve AS TIMESTAMP) as date_releve,
        pdl,
        CAST(false AS BOOLEAN) as ordre_index,
        CAST(hp AS DOUBLE) as hp,
        CAST(hc AS DOUBLE) as hc,
        CAST(hpb AS DOUBLE) as hpb,
        CAST(hcb AS DOUBLE) as hcb,
        CAST(hph AS DOUBLE) as hph,
        CAST(hch AS DOUBLE) as hch,
        CAST(base AS DOUBLE) as base,
        unite,
        unite as precision,
        CAST('flux_R64' AS VARCHAR) as source,
        CAST(NULL AS VARCHAR) as id_calendrier_distributeur,
        CAST(NULL AS VARCHAR) as id_calendrier_fournisseur,
        CAST(NULL AS VARCHAR) as ref_situation_contractuelle,
        CAST(NULL AS VARCHAR) as id_affaire,
        type_releve,
        contexte_releve,
        etape_metier,
        grandeur_physique,
        grandeur_metier,
        'R64' as flux_origine,
        CAST(false AS BOOLEAN) as date_ajustee
    FROM flux_enedis.flux_r64
    WHERE date_releve IS NOT NULL
)
SELECT * FROM releves_harmonises
"""