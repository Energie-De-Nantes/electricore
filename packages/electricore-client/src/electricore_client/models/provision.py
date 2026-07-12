"""Modèles de contrat de l'estimation de provision (GET RPC, ADR-0048, #487/#630).

`GET /provision/estimation?pdl=…` dérive en cœur-pur, depuis `flux_r67` (cold-start),
la *provision d'énergie* d'un lissé en **kWh** (annuel par cadran + provision mensuelle
`/12` plate) + métadonnées de couverture / profondeur / qualité / signal alertable.
**Aucun €** (prix fournisseur = ERP, ADR-0016/0027).

Single-sourcés ici (ADR-0043) : le router FastAPI (`provision_service.serialiser_rapport`)
émet ce même gabarit, pas de redéfinition.
"""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, ConfigDict

# Version du contrat (cf. provision_service.CONTRAT_VERSION). Source unique.
CONTRAT_VERSION_PROVISION = 1


class EstimationProvision(BaseModel):
    """Estimation de provision d'un PDL : sortie WIDE en kWh + métadonnées (ADR-0048).

    `energie_<cadran>_kwh` = estimation **annuelle** ; `energie_<cadran>_mensuel_kwh` =
    la provision mensuelle `/12` plate. `base` porte toujours le total ; les cadrans fins
    sont `None` hors de la `profondeur_cadran` déclarée.
    """

    model_config = ConfigDict(extra="ignore")

    pdl: str

    # --- Estimation annuelle (kWh) — WIDE, base = total ---
    energie_base_kwh: float
    energie_hp_kwh: float | None = None
    energie_hc_kwh: float | None = None
    energie_hph_kwh: float | None = None
    energie_hpb_kwh: float | None = None
    energie_hch_kwh: float | None = None
    energie_hcb_kwh: float | None = None

    # --- Provision mensuelle (kWh) — `/12` plate ---
    energie_base_mensuel_kwh: float
    energie_hp_mensuel_kwh: float | None = None
    energie_hc_mensuel_kwh: float | None = None
    energie_hph_mensuel_kwh: float | None = None
    energie_hpb_mensuel_kwh: float | None = None
    energie_hch_mensuel_kwh: float | None = None
    energie_hcb_mensuel_kwh: float | None = None

    # --- Couverture (fenêtre de 12 mois glissants) ---
    couverture_debut: date | None = None
    couverture_fin: date | None = None
    couverture_mois: float
    couverture_suffisante: bool

    # --- Profondeur cadran déclarée ---
    profondeur_cadran: str

    # --- Qualité dérivée du mix R/E/C ---
    qualite: str
    presence_regularisation: bool

    # --- Signal alertable (la lib expose, l'aval alerte — ADR-0037) ---
    signal_alertable: bool


class RapportProvision(BaseModel):
    """Enveloppe JSON de `GET /provision/estimation` : `{pdl, as_of, trouve, estimation}`.

    `estimation` est `None` quand `trouve` est `False` (aucune période R67 dans la
    fenêtre de 12 mois pour ce PDL).
    """

    model_config = ConfigDict(extra="ignore")

    pdl: str
    as_of: date
    trouve: bool
    estimation: EstimationProvision | None = None
