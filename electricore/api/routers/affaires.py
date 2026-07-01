"""Router du cockpit des affaires SGE (suivi opérationnel, #276).

Expose la vue read-only des affaires non soldées (flux X12/X13) sous le domaine
`/perimetre` — prolongement des entrées/sorties C15 (le passé) vers les affaires en
vol (l'en-cours). Le rollup « non soldées + ancienneté » se calcule à la lecture
(`affaires_ouvertes`), jamais matérialisé. Lecture seule : aucune écriture vers le SGE.
"""

from datetime import date, datetime
from zoneinfo import ZoneInfo

import polars as pl
from fastapi import APIRouter, Depends, Query

from electricore.api.decorators import binary_endpoint
from electricore.api.security import get_current_api_key

router = APIRouter(tags=["perimetre"])

PARIS = ZoneInfo("Europe/Paris")


@binary_endpoint(router, "/perimetre/pdls.csv", media_type="text/csv", filename="perimetre_pdls.csv", error_status=500)
def get_perimetre_pdls_csv(
    # noqa B008 : motif FastAPI standard du repo ; B008 ne dédouane que les types immuables connus (pas `date`).
    jour: date | None = Query(None, description="Périmètre actif à ce jour civil (défaut : aujourd'hui)"),  # noqa: B008
    limit: int = Query(100000, le=1000000, description="Nombre maximum de PDL"),
) -> bytes:
    """CSV (une colonne `pdl`) des PDL présents dans le périmètre à une date (ADR-0052).

    Liste à déposer sur le portail SGE pour une **demande M023** (collecte des relevés
    quotidiens → R64). `jour` optionnel : par défaut le périmètre du jour.
    """
    from electricore.core.loaders.duckdb import c15
    from electricore.core.pipelines.perimetre import pdls_actifs_a

    # L'aujourd'hui (impur) est injecté ici, au boundary ; le cœur reste pur (#179).
    df = pdls_actifs_a(c15().lazy(), jour or datetime.now(PARIS).date()).head(limit)
    return df.write_csv().encode("utf-8")


def _load_affaires_df(**kwargs) -> pl.DataFrame:
    """Charge `flux_affaires` via le loader core (seam IO, monkeypatché en test)."""
    from electricore.core.loaders.duckdb import affaires

    return affaires().collect()


@router.get("/perimetre/affaires")
def get_affaires_ouvertes(
    origine: str | None = Query(None, description="Filtre : initiee (X12) / recue (X13)"),
    inclure_ame: bool = Query(False, description="Inclure les souscriptions de flux de données (AME)"),
    api_key: str = Depends(get_current_api_key),
) -> dict:
    """Affaires SGE non soldées (statut COURS) avec leur dernier état et leur ancienneté.

    AME (souscription de flux de données ≈ 45 % du volume) est écarté par défaut ;
    `inclure_ame=true` le réintègre.
    """
    from electricore.core.pipelines.affaires import affaires_ouvertes

    jalons = _load_affaires_df()
    vue = affaires_ouvertes(
        jalons,
        maintenant=datetime.now(PARIS),
        exclure_prestations=() if inclure_ame else ("AME",),
    )
    if origine:
        vue = vue.filter(pl.col("origine") == origine)
    vue = vue.sort("anciennete_jours", descending=True)
    return {"affaires": vue.to_dicts()}
