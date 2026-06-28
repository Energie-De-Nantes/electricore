"""Router de l'estimation de provision : `GET /provision/estimation?pdl=…` (#487, ADR-0048).

Endpoint **ERP-agnostique** (zéro `integrations.odoo`, ADR-0016) : il dérive en cœur-pur,
depuis `flux_r67` (mesures facturantes M023), l'estimation de *provision d'énergie* d'un
lissé en **kWh** (annuel par cadran + provision mensuelle `/12` plate) + métadonnées de
couverture / profondeur / qualité / signal alertable. **Aucun €** (prix fournisseur = ERP,
ADR-0016/0027). Réponse JSON enveloppée avec `contract_version`, auth `X-API-Key` — mêmes
conventions que `POST /facturation/rsc`.
"""

import logging

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Query, Response

from electricore.api.security import get_current_api_key
from electricore.api.services.provision_service import CONTRAT_VERSION, serialiser_rapport
from electricore.core.builds.rapport_provision import RapportProvision
from electricore.core.loaders.duckdb import DuckDBLockError

logger = logging.getLogger(__name__)

router = APIRouter(tags=["provision"])


def _estimer(pdl: str) -> RapportProvision:
    """Estimation de provision d'un PDL (seam IO, monkeypatché en test).

    Pousse le filtre `pdl` dans DuckDB (loader `r67`) et lit l'horloge une seule fois au
    boundary du build (`estimation_provision`, décision 7 d'ADR-0048).
    """
    from electricore.core.builds.rapport_provision import estimation_provision

    return estimation_provision(pdl)


@router.get("/provision/estimation")
async def get_estimation_provision(
    response: Response,
    pdl: str = Query(..., description="Point de livraison (PDL) à estimer", examples=["12345678901234"]),
    api_key: str = Depends(get_current_api_key),
) -> dict:
    """Estime la *provision d'énergie* d'un lissé depuis R67 (cold-start), en kWh (JSON enveloppé).

    **Authentification requise** (`X-API-Key`).

    Renvoie `{contract_version, pdl, as_of, trouve, estimation}`. `estimation` porte les
    `energie_<cadran>_kwh` annuels + `energie_<cadran>_mensuel_kwh` (provision `/12` plate) +
    `couverture_*` + `profondeur_cadran` + `qualite` + `signal_alertable` (la lib expose le
    signal, l'aval alerte — ADR-0037). `trouve == False` (estimation `null`) quand le PDL n'a
    aucune période R67 dans la fenêtre de 12 mois. **Zéro €** (ADR-0016/0027).
    """
    response.headers["X-Contract-Version"] = str(CONTRAT_VERSION)
    try:
        rapport = _estimer(pdl)
    except (HTTPException, DuckDBLockError):
        # DuckDBLockError → handler d'app (503 « ingestion en cours », main.py).
        raise
    except duckdb.CatalogException as e:
        # flux_r67 pas (encore) matérialisé : aucune mesure facturante M023 (R67) ingérée.
        # État opérationnel attendu, pas un bug → 503 actionnable plutôt qu'un 500 brut.
        logger.warning("provision/estimation: flux_r67 absent — %s", e)
        raise HTTPException(
            503,
            "Données R67 indisponibles : le flux flux_r67 n'est pas matérialisé (aucune "
            "mesure facturante M023 ingérée). Déclencher une demande M023 sur le portail "
            "SGE, l'ingérer, puis réessayer.",
        ) from e
    except FileNotFoundError as e:
        logger.warning("provision/estimation: base DuckDB absente — %s", e)
        raise HTTPException(503, f"Base de données indisponible : {e}") from e
    except Exception as e:
        logger.exception("provision/estimation: erreur inattendue")
        raise HTTPException(503, f"Erreur lors de l'estimation : {e}") from e
    return serialiser_rapport(rapport)
