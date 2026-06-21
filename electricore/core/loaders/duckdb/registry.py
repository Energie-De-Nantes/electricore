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
    col_literal,
    col_literal_bool,
    col_paris,
    col_simple,
)
from .transforms import (
    transform_dates,
    transform_factures,
    transform_historique,
    transform_r64,
    transform_releves,
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
        # Colonnes principales
        col_simple("date_evenement"),
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
        # pas (ADR-0035).
        col_simple("niveau_ouverture_services"),
        col_simple("date_changement_niveau_ouverture_services"),
        # Relevés "Avant" (index de compteurs)
        col_simple("avant_date_releve"),
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
        # Relevés "Après" (index de compteurs)
        col_simple("apres_date_releve"),
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
        # Métadonnées
        col_literal("flux_C15", "source"),
    ),
    transform=transform_historique,
    validator=None,
)


# Flux R151 - Relevés périodiques, date BRUTE (fidèle source, ADR-0003 amendé / #294)
DESCRIPTOR_R151 = FluxDescriptor(
    flux_name="R151",
    table="flux_enedis.flux_r151",
    columns=(
        # Date BRUTE, fidèle à la source (convention Enedis « fin de journée ») : ancrée
        # heure-mur Paris, SANS le +1 jour. L'harmonisation J → J+1 (ADR-0003) vit désormais
        # en UN seul endroit, le mart `releves` (#294) ; l'endpoint /flux/r151 sert la date
        # nue et est candidat à dépréciation (l'usage direct des flux bruts incombe à
        # l'appelant, qui doit connaître la convention fin-de-journée de R151).
        Column(
            name="date_releve",
            sql_expr="timezone('Europe/Paris', CAST(date_releve AS TIMESTAMP))",
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
    comments="""-- DATE BRUTE (fidèle source, ADR-0003 amendé / #294)
-- R151 utilise la convention "fin de journée" (date J = index fin du jour J). L'endpoint
-- /flux/r151 sert cette date NUE, sans le +1 jour d'harmonisation. L'harmonisation J → J+1
-- (qui aligne R151 sur la convention "début de journée" de R64/R15/C15) vit en UN endroit :
-- le mart `releves` consommé par la chaîne énergie. Cet endpoint brut est dépréciable.""",
    transform=transform_releves,
    validator=RelevéIndex,
)


# Flux R15 - Relevés avec événements
DESCRIPTOR_R15 = FluxDescriptor(
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
    transform=transform_releves,
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
    transform=transform_factures,
    validator=None,  # Pas encore de modèle Pandera pour les factures
)


# Flux R64 - Relevés JSON timeseries
DESCRIPTOR_R64 = FluxDescriptor(
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
        col_simple("index_hpb_kwh"),
        col_simple("index_hph_kwh"),
        col_simple("index_hch_kwh"),
        col_simple("index_hcb_kwh"),
        col_simple("index_hp_kwh"),
        col_simple("index_hc_kwh"),
        col_simple("index_base_kwh"),
        # Métadonnées système (#333) : modification_date / _source_zip / _flux_type /
        # _json_name ne sont PAS projetées par le mart `flux_r64` (modification_date n'y
        # sert qu'en interne, clé de tri du qualify de dédup ; les `_*` sont des colonnes de
        # landing dlt). Les déclarer ici déclenchait un Binder Error sur tout /flux/r64
        # depuis la refonte `releves` (#241/#304/#285). Le test de garde loader↔mart
        # (test_loaders_sur_base_dbt) verrouille l'alignement.
        col_literal("flux_R64", "source"),
    ),
    where_clause="date_releve IS NOT NULL",
    transform=transform_r64,
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
        # jalon_date_heure est déjà TIMESTAMPTZ (XSD dateTime) — ne pas l'écraser en
        # naïf (cf. test_loaders_sur_base_dbt : décalerait l'instant d'1-2 h). On le
        # convertit en Europe/Paris (instant préservé) pour un dtype stable quel que soit
        # le fuseau de session (poste local vs CI/VPS en UTC).
        col_simple("jalon_date_heure"),
        col_simple("affaire_date_effet"),
        col_simple("affaire_etat"),
        col_simple("affaire_etat_libelle"),
        col_literal("flux_X12X13", "source"),
    ),
    transform=transform_dates(("jalon_date_heure",)),
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
