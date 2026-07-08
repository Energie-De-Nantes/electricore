"""Modèles de contrat des méta-périodes mensuelles (contrat v3, ADR-0027/0038).

Miroir de `meta_periodes_service.COLONNES_CONTRAT` + `source_hash` + le tableau
imbriqué `releves_utilises` (ADR-0038). Conventions de typage (ADR-0034) : les
index de compteur (`index_*_kwh`) sont des **entiers** (kWh entier au boundary
dbt) ; les énergies (`energie_*_kwh`) et montants (`*_eur`) sont des **flottants**.

`model_config = extra="ignore"` : le contrat est additif-tolérant — un serveur
plus récent qui ajoute une colonne ne casse pas un client plus ancien.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

# Version du contrat (cf. meta_periodes_service.CONTRAT_VERSION). Source unique.
CONTRAT_VERSION_META_PERIODES = 3


class ObjetReleve(BaseModel):
    """Un relevé d'index utilisé pour borner une méta-période (ADR-0038).

    Seuls les registres réellement affichés par le compteur ressortent (les
    autres sont absents, pas null). `evenement` n'est présent que pour un relevé
    d'origine événementielle (C15).
    """

    model_config = ConfigDict(extra="ignore")

    releve_id: str
    date_releve: str
    nature_index: str | None = None
    origine_releve: str | None = None
    evenement: str | None = None

    # Registres réels (index_*_kwh) — entiers (ADR-0034), seuls les non-nuls émis.
    index_base_kwh: int | None = None
    index_hp_kwh: int | None = None
    index_hc_kwh: int | None = None
    index_hph_kwh: int | None = None
    index_hch_kwh: int | None = None
    index_hpb_kwh: int | None = None
    index_hcb_kwh: int | None = None


class PeriodeMeta(BaseModel):
    """Méta-période mensuelle d'un contrat — agrégat valorisé (contrat v3).

    Quantités physiques + montants réseau (pas de prix fournisseur). Porte la
    trace d'index imbriquée `releves_utilises` (ADR-0038) et `source_hash`
    (intégrité de contenu, ADR-0027/0038).
    """

    model_config = ConfigDict(extra="ignore")

    ref_situation_contractuelle: str
    pdl: str
    mois_annee: str
    debut: str
    fin: str
    nb_jours: int
    puissance_moyenne_kva: float | None = None
    formule_tarifaire_acheminement: str | None = None

    # Énergies (kWh) — flottants (ADR-0034).
    energie_base_kwh: float | None = None
    energie_hp_kwh: float | None = None
    energie_hc_kwh: float | None = None

    # Montants réseau (€) — flottants.
    turpe_fixe_eur: float | None = None
    turpe_variable_eur: float | None = None
    cta_eur: float | None = None

    # Taux accise (€/MWh) — flottant (taux, pas montant : assiette possédée par l'ERP).
    taux_accise_eur_mwh: float | None = None

    has_changement: bool | None = None

    # Verdicts méta jumeaux (qualité ADR-0033 / communication ADR-0036) — valeurs
    # closes du contrat (#589), accents et underscore exacts.
    qualite: Literal["réelle", "estimée", "incalculable"] | None = None
    statut_communication: Literal["communicante", "non_communicante"] | None = None

    # Trace d'index légale (ADR-0038) — imbriquée, requise (peut être vide).
    releves_utilises: list[ObjetReleve] = []

    # Intégrité de contenu (ADR-0027/0038) — requise.
    source_hash: str
