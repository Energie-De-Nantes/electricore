"""
Registre des descripteurs de flux DuckDB.

Ce module centralise tous les descripteurs de flux Enedis (`FluxDescriptor`) :
colonnes SQL + transformation Polars résiduelle + validation Pandera, dans un
**seul** type (#389). C'est l'unique registre — il remplace l'ancien couple
schéma SQL (`FLUX_SCHEMAS`) / config appariée (`FLUX_CONFIGS`).
"""

# Imports de validation
from electricore.core.models.releve_index import RelevéIndex

from .descriptor import FluxDescriptor
from .sql import (
    Column,
    col_cast_null_varchar,
    col_jour,
    col_literal,
    col_literal_bool,
    col_offset,
    col_paris,
    col_simple,
)

# =============================================================================
# CONSTANTES MÉTIER
# =============================================================================

# Codes événementiels C15 — cf. electricore/core/CONTEXT.md
ENTREES_C15: tuple[str, ...] = ("PMES", "MES", "CFNE")
SORTIES_C15: tuple[str, ...] = ("RES", "CFNS")


class FluxInconnu(ValueError):
    """Flux demandé absent du registre `FLUX_DESCRIPTORS`.

    Levée au seam du loader (`flux(nom)`) — le loader reste agnostique HTTP
    (ADR-0016/0019) ; un caller transport mappe sur un 404. Pendant inverse de
    `DuckDBLockError`. Porte `nom` et `disponibles` pour un consommateur qui ne
    veut pas parser le message.
    """

    def __init__(self, nom: str, disponibles: list[str]):
        self.nom = nom
        self.disponibles = disponibles
        super().__init__(f"Flux '{nom}' inconnu. Flux disponibles : {disponibles}")


# =============================================================================
# DESCRIPTEURS PAR FLUX (Immutables)
# =============================================================================

# Flux C15 - Historique périmètre contractuel
DESCRIPTOR_C15 = FluxDescriptor(
    flux_name="C15",
    table="flux_enedis.flux_c15",
    columns=(
        # Colonnes principales. date_evenement = xsd:dateTime à offset (TIMESTAMPTZ).
        col_offset("date_evenement"),
        col_simple("pdl"),
        col_simple("ref_situation_contractuelle"),
        col_simple("segment_clientele"),
        col_simple("etat_contractuel"),
        col_simple("evenement_declencheur"),
        col_simple("type_evenement"),
        col_simple("categorie"),
        col_simple("puissance_souscrite_kva"),
        col_simple("formule_tarifaire_acheminement"),
        col_simple("type_compteur"),
        col_simple("num_compteur"),
        col_simple("ref_demandeur"),
        col_simple("id_affaire"),
        # Statut de communication (épique #313) : niveau d'ouverture aux services et sa
        # date de bascule, portés tels quels par dbt (Utf8 / Date) — le loader ne re-type
        # pas (ADR-0035). La date de bascule est un jour nu (xs:date).
        col_simple("niveau_ouverture_services"),
        col_jour("date_changement_niveau_ouverture_services"),
        # Relevés "Avant" (index de compteurs). avant_date_releve = xsd:dateTime à offset.
        col_offset("avant_date_releve"),
        col_simple("avant_nature_index"),
        col_simple("avant_id_calendrier_fournisseur"),
        col_simple("avant_id_calendrier_distributeur"),
        col_simple("avant_index_hp_kwh"),
        col_simple("avant_index_hc_kwh"),
        col_simple("avant_index_hch_kwh"),
        col_simple("avant_index_hph_kwh"),
        col_simple("avant_index_hpb_kwh"),
        col_simple("avant_index_hcb_kwh"),
        col_simple("avant_index_base_kwh"),
        # Relevés "Après" (index de compteurs). apres_date_releve = xsd:dateTime à offset.
        col_offset("apres_date_releve"),
        col_simple("apres_nature_index"),
        col_simple("apres_id_calendrier_fournisseur"),
        col_simple("apres_id_calendrier_distributeur"),
        col_simple("apres_index_hp_kwh"),
        col_simple("apres_index_hc_kwh"),
        col_simple("apres_index_hch_kwh"),
        col_simple("apres_index_hph_kwh"),
        col_simple("apres_index_hpb_kwh"),
        col_simple("apres_index_hcb_kwh"),
        col_simple("apres_index_base_kwh"),
        # Métadonnées. unite/precision = littéraux SQL (anciennement ajoutés en Polars).
        col_literal("flux_C15", "source"),
        col_literal("kWh", "unite"),
        col_literal("kWh", "precision"),
    ),
    transform=None,
    validator=None,
)


# Flux R151 - Relevés périodiques. SELECT * (ADR-0042, #395) : la forme résiduelle
# (source, ordre_index, placeholders null RSC/FTA, precision) et l'instant J+1 vivent
# désormais dans le modèle dbt `flux_r151`. date_releve sert l'INSTANT harmonisé (révise
# l'amendement #294 d'ADR-0003 : flux_r151 n'est plus « fidèle au label brut » mais
# « fidèle à l'instant de relevé » ; endpoint déprécié). releve_id reste minté sur la date
# brute en amont. Le loader ne projette ni ne re-type plus.
DESCRIPTOR_R151 = FluxDescriptor(
    flux_name="R151",
    table="flux_enedis.flux_r151",
    where_clause="date_releve IS NOT NULL AND id_calendrier_distributeur IN ('DI000001', 'DI000002', 'DI000003')",
    transform=None,
    validator=RelevéIndex,
)


# Flux R15 - Relevés avec événements
DESCRIPTOR_R15 = FluxDescriptor(
    flux_name="R15",
    table="flux_enedis.flux_r15",
    columns=(
        # date_releve = xsd:dateTime à offset (TIMESTAMPTZ) — instant absolu.
        col_offset("date_releve"),
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
    transform=None,
    validator=RelevéIndex,
)


# Flux F15 - Factures détaillées
DESCRIPTOR_F15 = FluxDescriptor(
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
        col_simple("prix_unitaire"),
        col_simple("quantite"),
        col_simple("montant_ht"),
        col_paris("date_debut"),
        col_paris("date_fin"),
        col_simple("libelle_ev"),
        col_literal("flux_F15", "source"),
    ),
    transform=None,
    validator=None,  # Pas encore de modèle Pandera pour les factures
)


# Flux R64 - Relevés JSON timeseries. SELECT * (ADR-0042, #394) : la forme résiduelle
# (source, ancrage date naïf → instant TIMESTAMPTZ) vit désormais dans le modèle dbt
# `flux_r64`. Le loader ne projette ni ne re-type plus ; le fuseau de session (#393) rend
# l'instant déterministe. La garde loader↔mart (test_loaders_sur_base_dbt) reste verte :
# un SELECT * lie exactement les colonnes du mart.
DESCRIPTOR_R64 = FluxDescriptor(
    flux_name="R64",
    table="flux_enedis.flux_r64",
    where_clause="date_releve IS NOT NULL",
    transform=None,
    validator=None,  # Pas encore de modèle Pandera pour R64
)


# Flux X12/X13 - Affaires SGE (suivi opérationnel, grain = un jalon)
DESCRIPTOR_AFFAIRES = FluxDescriptor(
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
        # jalon_date_heure est déjà TIMESTAMPTZ (XSD dateTime, offset) — instant absolu :
        # forme OFFSET (affichage ramené Europe/Paris, instant préservé). NE PAS l'écraser
        # en naïf (cf. test_loaders_sur_base_dbt : décalerait l'instant d'1-2 h).
        col_offset("jalon_date_heure"),
        # affaire_date_effet = jour nu (xs:date) — pas d'instant.
        col_jour("affaire_date_effet"),
        col_simple("affaire_etat"),
        col_simple("affaire_etat_libelle"),
        col_literal("flux_X12X13", "source"),
    ),
    transform=None,
    validator=None,  # opérationnel (suivi des affaires SGE), pas de schéma Pandera
)


# =============================================================================
# REGISTRE FONCTIONNEL (Mapping immutable)
# =============================================================================

FLUX_DESCRIPTORS: dict[str, FluxDescriptor] = {
    "c15": DESCRIPTOR_C15,
    "r151": DESCRIPTOR_R151,
    "r15": DESCRIPTOR_R15,
    "f15": DESCRIPTOR_F15,
    "r64": DESCRIPTOR_R64,
    "affaires": DESCRIPTOR_AFFAIRES,
}
