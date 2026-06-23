"""Modèles de contrat de la chronologie facturiste (contrat v1, ADR-0039/0041).

Miroir de `chronologie_service` : la frise d'un point/contrat est une suite de
lignes hétérogènes — **faits** (événements C15, relevés) tissés avec les
**verdicts** des périodes d'énergie. Une ligne est donc une **union discriminée**
(pydantic v2) sur `type_ligne` : `evenement | releve | periode_energie`.

Tout est optionnel (sauf le discriminant + `date`) : registres et énergies ne
sont émis que non-nuls (jamais de cadran synthétisé). Typage (ADR-0034) :
`index_*_kwh` entiers, `energie_*_kwh` flottants. **Pas de montant tarifaire**
(différenciateur vs méta-périodes).
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

# Version du contrat (cf. chronologie_service.CONTRAT_VERSION). Source unique.
CONTRAT_VERSION_CHRONOLOGIE = 1


class _LigneBase(BaseModel):
    """Champs partagés par toute ligne de frise (additive-tolérante)."""

    model_config = ConfigDict(extra="ignore")

    date: str
    pdl: str | None = None
    ref_situation_contractuelle: str | None = None


class LigneEvenement(_LigneBase):
    """Un fait événementiel : événement C15 (y compris hors-comptage) ou borne FACTURATION.

    Porte la situation au moment du fait (puissance, FTA, niveau d'ouverture) et
    les annotations de rupture d'abonnement.
    """

    type_ligne: Literal["evenement"]
    source: str | None = None
    type_fait: str | None = None
    evenement_declencheur: str | None = None
    puissance_souscrite_kva: float | None = None
    formule_tarifaire_acheminement: str | None = None
    niveau_ouverture_services: str | None = None
    impacte_abonnement: bool | None = None
    resume_modification: str | None = None


class LigneReleve(_LigneBase):
    """Un fait de relevé : index utilisé, avec origine (périodique/événementiel) et nature.

    Seuls les registres réels (non nuls) ressortent. `evenement_declencheur` n'est
    présent que pour un relevé d'origine événementielle (C15).
    """

    type_ligne: Literal["releve"]
    source: str | None = None
    releve_id: str | None = None
    nature_index: str | None = None
    origine_releve: str | None = None
    ordre_index: int | None = None
    evenement_declencheur: str | None = None

    # Registres réels (index_*_kwh) — entiers (ADR-0034), non-nuls uniquement.
    index_base_kwh: int | None = None
    index_hp_kwh: int | None = None
    index_hc_kwh: int | None = None
    index_hph_kwh: int | None = None
    index_hch_kwh: int | None = None
    index_hpb_kwh: int | None = None
    index_hcb_kwh: int | None = None


class LignePeriodeEnergie(_LigneBase):
    """Une période d'énergie dérivée : bornes + **verdicts** (qualité/communication) + énergie
    physique (kWh). **Aucun** montant tarifaire (ADR-0027)."""

    type_ligne: Literal["periode_energie"]
    debut: str | None = None
    fin: str | None = None
    nb_jours: int | None = None
    qualite: str | None = None
    statut_communication: str | None = None

    # Énergies (energie_*_kwh) — flottants (ADR-0034), non-nulles uniquement.
    energie_base_kwh: float | None = None
    energie_hp_kwh: float | None = None
    energie_hc_kwh: float | None = None
    energie_hph_kwh: float | None = None
    energie_hch_kwh: float | None = None
    energie_hpb_kwh: float | None = None
    energie_hcb_kwh: float | None = None


# Union discriminée sur `type_ligne` : pydantic résout chaque ligne au bon sous-type.
LigneChronologie = Annotated[
    LigneEvenement | LigneReleve | LignePeriodeEnergie,
    Field(discriminator="type_ligne"),
]

# Adaptateur réutilisable pour valider une ligne brute (dict) → sous-type concret.
_ADAPTER: TypeAdapter = TypeAdapter(LigneChronologie)


def valider_ligne_chronologie(ligne: dict) -> LigneEvenement | LigneReleve | LignePeriodeEnergie:
    """Valide une ligne brute en son sous-type concret via l'union discriminée."""
    return _ADAPTER.validate_python(ligne)
