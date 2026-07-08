"""
Registre des descripteurs de flux DuckDB.

Ce module centralise tous les descripteurs de flux Enedis (`FluxDescriptor`) :
colonnes SQL + transformation Polars résiduelle + validation Pandera, dans un
**seul** type (#389). C'est l'unique registre — il remplace l'ancien couple
schéma SQL (`FLUX_SCHEMAS`) / config appariée (`FLUX_CONFIGS`).
"""

# Imports de validation
from electricore.core.models.affaire_jalon import AffaireJalon
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


# Flux C12 - Description contractuelle C2-C4 (>36 kVA, ADR-0051). SELECT * (ADR-0042) :
# la forme résiduelle (littéral source) vit dans le modèle dbt `flux_c12`. Les puissances
# sont en BIGINT (kVA entiers). Pas de validateur Pandera : pas de seam de calcul établi
# à ce stade (ADR-0035 §5 — même raisonnement que F15/R64 ; le premier consommateur
# établira le contrat).
DESCRIPTOR_C12 = FluxDescriptor(
    flux_name="C12",
    table="flux_enedis.flux_c12",
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
# Audit registre (#295) : seam **déjà gardé** (`validator=RelevéIndex`) ; la question de
# l'unité native R15 (source d'un bug Wh latent) a été tranchée par #286 — rien à ajouter ici.
DESCRIPTOR_R15 = FluxDescriptor(
    flux_name="R15",
    table="flux_enedis.flux_r15",
    where_clause="date_releve IS NOT NULL",
    transform=None,
    validator=RelevéIndex,
)


# Extraction R15_ACC - Relevés d'autoconsommation (énergies), seconde linéarisation du flux
# R15 (`flux_name="R15"`, comme DESCRIPTOR_R15 : deux extractions d'un même flux ingéré, cf.
# glossaire *Flux vs Extraction*, ADR-0053). SELECT * (ADR-0042) sur le modèle dbt
# `flux_r15_acc`. Pas de validateur Pandera : même raisonnement que F12/R64 (ADR-0035 §5) —
# aucun seam de calcul core n'établit encore de contrat sur sa forme. Ajouté (#595) parce
# que le menu bot offrait déjà l'extraction `r15_acc` alors que le registre l'ignorait.
DESCRIPTOR_R15_ACC = FluxDescriptor(
    flux_name="R15",
    table="flux_enedis.flux_r15_acc",
    transform=None,
    validator=None,
)


# Flux F12 - Synthèse mensuelle de facturation distributeur (volumes agrégés). SELECT *
# (ADR-0042) : la forme résiduelle vit dans le modèle dbt `flux_f12_detail`. Pas de
# validateur Pandera : même raisonnement que F15 (ADR-0035 §5) — pas de seam de calcul
# core établi à ce stade, le premier consommateur établira le contrat.
# ponytail: symétrie de registre, arbitrage revue d'architecture 07/2026 — ne pas purger
# comme code mort (précédent #476) : F12 est ingéré + servi par l'API mais n'a, à sa
# création, encore aucun appelant côté core.
DESCRIPTOR_F12 = FluxDescriptor(
    flux_name="F12",
    table="flux_enedis.flux_f12_detail",
    transform=None,
    validator=None,
)


# Flux F15 - Factures détaillées. SELECT * (ADR-0042, #396) : la forme résiduelle (source)
# vit dans le modèle dbt `flux_f15_detail`. Les dates F15 (date_facture/date_debut/date_fin)
# sont des JOURS CIVILS (DATE) servis tels quels — plus d'ancrage en instant Paris (le loader
# les traitait à tort comme des naïves). Le loader ne projette ni ne re-type plus.
DESCRIPTOR_F15 = FluxDescriptor(
    flux_name="F15",
    table="flux_enedis.flux_f15_detail",
    transform=None,
    # validator=None **par audit** (#295, ADR-0035 §5) : un contrat appartient à un *seam
    # de consommation* où le cœur dépend de la FORME du flux pour un calcul. F15 n'a pas un
    # tel seam — son unique consommateur, `contexte_mensuel.documents()`, le **filtre**
    # (`date_facture`, `unite`) puis le **verse tel quel** dans des onglets XLSX (« F15
    # complet » / « F15 prestations »), sans calcul typé. Une passe-plat vers tableur ne
    # justifie pas un miroir Pandera (≠ R151/R15 qui alimentent l'énergie). Le rapprochement
    # TURPE↔F15 reste une idée future (#468), non implémentée → pas de seam aujourd'hui.
    validator=None,
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


# Flux R67 - Mesures facturantes (énergie par période, ADR-0047). Asset PARALLÈLE :
# jamais unioné dans `releves`/`chronologie_releves` ; requêtable directement par ce loader.
# SELECT * (la forme résiduelle vit dans le modèle dbt `flux_r67`) ; debut/fin sont des DATE
# (jour civil, ADR-0042), periode_debut/periode_fin des TIMESTAMPTZ servis tels quels. Pas
# encore de modèle Pandera (frontière énergie-par-période différée, ADR-0047 §9) → comme R64.
DESCRIPTOR_R67 = FluxDescriptor(
    flux_name="R67",
    table="flux_enedis.flux_r67",
    transform=None,
    validator=None,  # frontière Pandera propre différée (ADR-0047 §9)
)


# Flux X12/X13 - Affaires SGE (suivi opérationnel, grain = un jalon). SELECT * (ADR-0042,
# #397) : la forme résiduelle (littéral source) vit dans le modèle dbt `flux_affaires`.
# jalon_date_heure (TIMESTAMPTZ offset) et affaire_date_effet (DATE) sont déjà bien typés ;
# le fuseau de session (#393) rend l'instant déterministe. Le loader ne projette plus.
DESCRIPTOR_AFFAIRES = FluxDescriptor(
    flux_name="AFFAIRES",
    table="flux_enedis.flux_affaires",
    transform=None,
    # Seam `affaires_ouvertes` ← `flux_affaires` gardé par `AffaireJalon` (#295, ADR-0035) :
    # c'était la dernière lacune de registre de la discovery #147. La parité dbt↔Pandera est
    # prouvée par `test_dbt_affaires_respecte_le_contrat_pandera`.
    validator=AffaireJalon,
)


# =============================================================================
# REGISTRE FONCTIONNEL (Mapping immutable)
# =============================================================================

# Keyé par *extraction* (table interrogeable/exportable = modèle dbt = entrée menu bot =
# segment `/flux/{extraction}`), pas par *flux* ingéré (ADR-0053, #595). Un flux donne
# 1..N extractions : F15 → `f15_detail` (l'agrégat `f15` est réservé, extraction future,
# absente tant que `flux_f15` n'est pas matérialisé) ; R15 → `r15` (index) + `r15_acc`
# (autoconsommation). Le flux parent vit dans `FluxDescriptor.flux_name`, distinct de la clé.
FLUX_DESCRIPTORS: dict[str, FluxDescriptor] = {
    "c15": DESCRIPTOR_C15,
    "c12": DESCRIPTOR_C12,
    "r151": DESCRIPTOR_R151,
    "r15": DESCRIPTOR_R15,
    "r15_acc": DESCRIPTOR_R15_ACC,
    "f15_detail": DESCRIPTOR_F15,
    "f12_detail": DESCRIPTOR_F12,
    "r64": DESCRIPTOR_R64,
    "r67": DESCRIPTOR_R67,
    "affaires": DESCRIPTOR_AFFAIRES,
}
