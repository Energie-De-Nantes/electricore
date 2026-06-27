"""Service d'estimation de provision : `RapportProvision` → enveloppe JSON (#487, ADR-0048).

Sérialise le build cœur-pur `estimation_provision(pdl)` (ou `estimer(...)` côté test) en une
enveloppe JSON `{contract_version, pdl, as_of, trouve, estimation}` — mêmes conventions
facturiste que `POST /facturation/rsc` / méta-périodes (`contract_version` + en-tête
`X-Contract-Version`).

L'`estimation` est une projection plate, en **kWh** uniquement (annuel par cadran + provision
mensuelle `/12` plate) + métadonnées de couverture / profondeur / qualité / signal alertable.
**Aucun €** (prix fournisseur = ERP, ADR-0016/0027).
"""

from __future__ import annotations

import datetime as dt

from electricore.core.builds.rapport_provision import RapportProvision

# Version du contrat exposée dans l'enveloppe (cf. rsc / méta-périodes). Bump sur rupture ;
# l'ajout d'un champ optionnel reste additif et ne change pas la version.
CONTRAT_VERSION = 1


def _serialiser_date(valeur: object) -> object:
    """Rend une `date` sérialisable JSON (ISO) ; passe-plat sinon."""
    if isinstance(valeur, dt.date):
        return valeur.isoformat()
    return valeur


def serialiser_rapport(rapport: RapportProvision) -> dict:
    """Enveloppe JSON d'un `RapportProvision` : `{contract_version, pdl, as_of, trouve, estimation}`.

    `estimation` est `None` si le PDL n'a aucune période R67 dans la fenêtre de 12 mois
    (`trouve == False`), sinon le dict plat de l'unique ligne (dates en ISO).
    """
    estimation: dict | None = None
    if rapport.trouve:
        ligne = rapport.estimation.to_dicts()[0]
        estimation = {cle: _serialiser_date(val) for cle, val in ligne.items()}
    return {
        "contract_version": CONTRAT_VERSION,
        "pdl": rapport.pdl,
        "as_of": rapport.as_of.isoformat(),
        "trouve": rapport.trouve,
        "estimation": estimation,
    }
