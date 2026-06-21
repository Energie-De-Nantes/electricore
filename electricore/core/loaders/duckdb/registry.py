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

# Flux C15 - Historique périmètre contractuel. SELECT * (ADR-0042, #397) : la forme
# résiduelle (littéraux source/unite/precision) vit dans le modèle dbt `flux_c15`. Les
# dates sont déjà bien typées à la source (date_evenement/avant/apres = TIMESTAMPTZ offset ;
# date_changement_niveau / date_derniere_modification_fta = DATE) — rien à ré-ancrer ; le
# fuseau de session (#393) rend les instants déterministes. Le loader ne projette plus.
DESCRIPTOR_C15 = FluxDescriptor(
    flux_name="C15",
    table="flux_enedis.flux_c15",
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


# Flux R15 - Relevés avec événements. SELECT * (ADR-0042, #397) : la forme résiduelle
# (rename id_calendrier → id_calendrier_distributeur, placeholders null FTA/calendrier
# fournisseur, littéraux source/ordre_index/unite/precision) vit dans le modèle dbt
# `flux_r15`. date_releve est déjà un instant TIMESTAMPTZ (offset). Le loader ne projette plus.
DESCRIPTOR_R15 = FluxDescriptor(
    flux_name="R15",
    table="flux_enedis.flux_r15",
    where_clause="date_releve IS NOT NULL",
    transform=None,
    validator=RelevéIndex,
)


# Flux F15 - Factures détaillées. SELECT * (ADR-0042, #396) : la forme résiduelle (source)
# vit dans le modèle dbt `flux_f15_detail`. Les dates F15 (date_facture/date_debut/date_fin)
# sont des JOURS CIVILS (DATE) servis tels quels — plus d'ancrage en instant Paris (le loader
# les traitait à tort comme des naïves). Le loader ne projette ni ne re-type plus.
DESCRIPTOR_F15 = FluxDescriptor(
    flux_name="F15",
    table="flux_enedis.flux_f15_detail",
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


# Flux X12/X13 - Affaires SGE (suivi opérationnel, grain = un jalon). SELECT * (ADR-0042,
# #397) : la forme résiduelle (littéral source) vit dans le modèle dbt `flux_affaires`.
# jalon_date_heure (TIMESTAMPTZ offset) et affaire_date_effet (DATE) sont déjà bien typés ;
# le fuseau de session (#393) rend l'instant déterministe. Le loader ne projette plus.
DESCRIPTOR_AFFAIRES = FluxDescriptor(
    flux_name="AFFAIRES",
    table="flux_enedis.flux_affaires",
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
