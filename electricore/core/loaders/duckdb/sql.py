"""
Génération SQL fonctionnelle pour requêtes DuckDB.

Ce module fournit les primitives pour construire des requêtes SQL de manière
fonctionnelle et type-safe avec dataclasses immutables.

Au lieu de constantes SQL de 300 lignes, ce module génère le SQL
depuis des définitions structurées.
"""

from dataclasses import dataclass

# =============================================================================
# DATACLASSES IMMUTABLES POUR DÉFINITION SQL
# =============================================================================


@dataclass(frozen=True, slots=True)
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
    alias: str | None = None

    def to_sql(self) -> str:
        """Convertit en fragment SQL SELECT."""
        # Si l'expression SQL est différente du nom, ajouter l'alias
        if self.sql_expr != self.name:
            alias_name = self.alias if self.alias else self.name
            return f"{self.sql_expr} as {alias_name}"
        # Sinon, utiliser directement le nom (colonne simple)
        return self.sql_expr


@dataclass(frozen=True, slots=True)
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
    where_clause: str | None = None
    comments: str | None = None


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


def col_paris(name: str) -> Column:
    """Colonne temporelle naïve (DATE/TIMESTAMP sans fuseau) ancrée en Europe/Paris.

    Les sources Enedis sans offset sont des heures-mur Paris : l'ancrage explicite
    (timezone(...)) rend l'instant correct quel que soit le fuseau de session DuckDB
    (poste local, CI, VPS). Les colonnes déjà TIMESTAMPTZ n'en ont pas besoin.
    """
    return Column(name=name, sql_expr=f"timezone('Europe/Paris', CAST({name} AS TIMESTAMP))")


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
        col_cast_double("puissance_souscrite_kva"),
        col_simple("formule_tarifaire_acheminement"),
        col_simple("type_compteur"),
        col_simple("num_compteur"),
        col_simple("ref_demandeur"),
        col_simple("id_affaire"),
        # Relevés "Avant" (index de compteurs)
        col_simple("avant_date_releve"),
        col_simple("avant_nature_index"),
        col_simple("avant_id_calendrier_fournisseur"),
        col_simple("avant_id_calendrier_distributeur"),
        col_cast_double("avant_index_hp_kwh"),
        col_cast_double("avant_index_hc_kwh"),
        col_cast_double("avant_index_hch_kwh"),
        col_cast_double("avant_index_hph_kwh"),
        col_cast_double("avant_index_hpb_kwh"),
        col_cast_double("avant_index_hcb_kwh"),
        col_cast_double("avant_index_base_kwh"),
        # Relevés "Après" (index de compteurs)
        col_simple("apres_date_releve"),
        col_simple("apres_nature_index"),
        col_simple("apres_id_calendrier_fournisseur"),
        col_simple("apres_id_calendrier_distributeur"),
        col_cast_double("apres_index_hp_kwh"),
        col_cast_double("apres_index_hc_kwh"),
        col_cast_double("apres_index_hch_kwh"),
        col_cast_double("apres_index_hph_kwh"),
        col_cast_double("apres_index_hpb_kwh"),
        col_cast_double("apres_index_hcb_kwh"),
        col_cast_double("apres_index_base_kwh"),
        # Métadonnées
        col_literal("flux_C15", "source"),
    ),
)


# Flux R151 - Relevés périodiques avec harmonisation date
SCHEMA_R151 = FluxSchema(
    flux_name="R151",
    table="flux_enedis.flux_r151",
    columns=(
        # Date avec harmonisation (convention fin de journée → début de journée)
        Column(
            name="date_releve",
            sql_expr="timezone('Europe/Paris', CAST(date_releve AS TIMESTAMP) + INTERVAL '1 day')",
            alias="date_releve",
        ),
        col_simple("pdl"),
        col_cast_null_varchar("ref_situation_contractuelle"),
        col_cast_null_varchar("formule_tarifaire_acheminement"),
        col_simple("id_calendrier_fournisseur"),
        col_simple("id_calendrier_distributeur"),
        col_simple("id_affaire"),
        # Cadrans (index de compteurs). kWh entiers (ADR-0034) : flux_r151 émet du bigint
        # floor ; le loader ne re-caste plus (Int64 natif, ADR-0035), il trust dbt.
        col_simple("index_hp_kwh"),
        col_simple("index_hc_kwh"),
        col_simple("index_hch_kwh"),
        col_simple("index_hph_kwh"),
        col_simple("index_hpb_kwh"),
        col_simple("index_hcb_kwh"),
        col_simple("index_base_kwh"),
        # Métadonnées
        col_literal("flux_R151", "source"),
        col_literal_bool(False, "ordre_index"),
        col_simple("unite"),
        Column(name="precision", sql_expr="unite", alias="precision"),
    ),
    where_clause="date_releve IS NOT NULL AND id_calendrier_distributeur IN ('DI000001', 'DI000002', 'DI000003')",
    comments="""-- HARMONISATION DES CONVENTIONS DE DATE
-- Problème : R151 utilise convention "fin de journée" (date J = index fin jour J)
--           alors que R64, R15, C15 utilisent "début de journée" (date J = index début jour J)
-- Solution : R151 date J → J+1 pour aligner sur convention majoritaire "début de journée"
-- Résultat : après ajustement, R151 et R64 correspondent parfaitement (244 matches exacts testés)""",
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
        # Cadrans (index de compteurs). kWh entiers (ADR-0034) : flux_r15 émet du bigint ;
        # le loader ne re-caste plus (Int64 natif, ADR-0035), il trust dbt.
        col_simple("index_hp_kwh"),
        col_simple("index_hc_kwh"),
        col_simple("index_hch_kwh"),
        col_simple("index_hph_kwh"),
        col_simple("index_hpb_kwh"),
        col_simple("index_hcb_kwh"),
        col_simple("index_base_kwh"),
        # Métadonnées
        col_literal("flux_R15", "source"),
        col_literal_bool(False, "ordre_index"),
        col_literal("kWh", "unite"),
        col_literal("kWh", "precision"),
    ),
    where_clause="date_releve IS NOT NULL",
)


# Flux F15 - Factures détaillées
SCHEMA_F15 = FluxSchema(
    flux_name="F15",
    table="flux_enedis.flux_f15_detail",
    columns=(
        col_paris("date_facture"),
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
        col_paris("date_debut"),
        col_paris("date_fin"),
        col_simple("libelle_ev"),
        col_literal("flux_F15", "source"),
    ),
)


# Flux R64 - Relevés JSON timeseries
SCHEMA_R64 = FluxSchema(
    flux_name="R64",
    table="flux_enedis.flux_r64",
    columns=(
        col_paris("date_releve"),
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
        # Cadrans (format WIDE - index de compteurs)
        col_cast_double("index_hpb_kwh"),
        col_cast_double("index_hph_kwh"),
        col_cast_double("index_hch_kwh"),
        col_cast_double("index_hcb_kwh"),
        col_cast_double("index_hp_kwh"),
        col_cast_double("index_hc_kwh"),
        col_cast_double("index_base_kwh"),
        # Métadonnées système
        col_simple("modification_date"),
        col_simple("_source_zip"),
        col_simple("_flux_type"),
        col_simple("_json_name"),
        col_literal("flux_R64", "source"),
    ),
    where_clause="date_releve IS NOT NULL",
)


# Flux X12/X13 - Affaires SGE (suivi opérationnel, grain = un jalon)
SCHEMA_AFFAIRES = FluxSchema(
    flux_name="AFFAIRES",
    table="flux_enedis.flux_affaires",
    columns=(
        col_simple("affaire_id"),
        col_simple("affaire_jalon_id"),
        col_simple("origine"),
        col_simple("prestation"),
        col_simple("prestation_libelle"),
        col_simple("statut"),
        col_simple("pdl"),
        col_simple("segment"),
        col_simple("jalon_num"),
        # jalon_date_heure est déjà TIMESTAMPTZ (XSD dateTime) — ne pas l'écraser en
        # naïf (cf. test_loaders_sur_base_dbt : décalerait l'instant d'1-2 h).
        col_simple("jalon_date_heure"),
        col_simple("affaire_date_effet"),
        col_simple("affaire_etat"),
        col_simple("affaire_etat_libelle"),
        col_literal("flux_X12X13", "source"),
    ),
)


# =============================================================================
# REGISTRE FONCTIONNEL (Mapping immutable)
# =============================================================================

FLUX_SCHEMAS: dict[str, FluxSchema] = {
    "c15": SCHEMA_C15,
    "r151": SCHEMA_R151,
    "r15": SCHEMA_R15,
    "f15": SCHEMA_F15,
    "r64": SCHEMA_R64,
    "affaires": SCHEMA_AFFAIRES,
}
