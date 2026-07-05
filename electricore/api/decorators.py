"""Décorateurs pour routes binaires (XLSX / Arrow).

Encapsule le squelette répété autour de chaque endpoint binaire :
- dépendance d'authentification (`Depends(get_current_api_key)`)
- garde Odoo optionnelle (`requires_odoo=True` → 501 si non configuré)
- dispatch sync (run_in_executor) vs coroutine (await as-is)
- conversion d'exception métier en `HTTPException(503)` avec `logger.exception`
- emballage `Response` avec media type + `Content-Disposition` optionnel
  (placeholders du `filename` résolus depuis les arguments de la fonction décorée)

Voir issue #18.
"""

import asyncio
import inspect
import logging

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Response

from electricore.api.config import settings
from electricore.api.security import get_current_api_key
from electricore.core.loaders.duckdb import DuckDBLockError

logger = logging.getLogger(__name__)

XLSX_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
ARROW_MEDIA_TYPE = "application/vnd.apache.arrow.stream"


def _resoudre_filename(template: str, args: dict[str, object]) -> str:
    """Résout les placeholders `{nom_arg}` dans le template depuis les args.

    Un arg `None` produit une chaîne vide pour son placeholder
    (cas typique : filtre optionnel non fourni → `accise{trimestre}.xlsx`
    devient `accise.xlsx`).
    """
    valeurs = {k: ("" if v is None else str(v)) for k, v in args.items()}
    return template.format(**valeurs)


def binary_endpoint(
    target: FastAPI | APIRouter,
    path: str,
    *,
    media_type: str,
    filename: str | None = None,
    requires_odoo: bool = False,
    error_status: int = 503,
    tags: list[str] | None = None,
):
    """Enregistre un handler retournant `bytes` comme endpoint binaire générique.

    Args:
        target: cible d'enregistrement de la route — instance `FastAPI` ou
            `APIRouter`. Les deux exposent `.get(path, *, tags, dependencies)`
            avec la même signature ; le décorateur ne distingue pas.
        path: chemin HTTP de l'endpoint (`GET`).
        media_type: type MIME de la réponse (XLSX, Arrow stream, ZIP…).
        filename: template du `Content-Disposition` (placeholders `{nom_arg}`
            résolus depuis les arguments du handler). Si `None`, pas de
            header `Content-Disposition` (cas Arrow streaming).
        requires_odoo: si `True`, retourne `501` quand Odoo n'est pas configuré.
        tags: tags OpenAPI optionnels (s'ajoutent à ceux déclarés sur l'`APIRouter`
            si la cible est un router).
    """

    def decorator(handler):
        async def wrapper(**kwargs) -> Response:
            if requires_odoo and not settings.is_odoo_configured:
                raise HTTPException(
                    501,
                    "Odoo non configuré. Définissez ODOO__URL/DB/USERNAME/PASSWORD.",
                )
            try:
                if asyncio.iscoroutinefunction(handler):
                    content = await handler(**kwargs)
                else:
                    content = await asyncio.get_event_loop().run_in_executor(None, lambda: handler(**kwargs))
            except (HTTPException, DuckDBLockError):
                # DuckDBLockError remonte au handler d'app qui la convertit en
                # 503 « ingestion en cours » (cf. main.verrou_duckdb_en_503, #171).
                raise
            except Exception as e:
                logger.exception("Erreur %s", path)
                raise HTTPException(error_status, f"Erreur lors du traitement : {e}")

            headers: dict[str, str] = {}
            if filename is not None:
                resolved = _resoudre_filename(filename, kwargs)
                headers["Content-Disposition"] = f"attachment; filename={resolved}"
            return Response(content=content, media_type=media_type, headers=headers)

        wrapper.__signature__ = inspect.signature(handler)  # type: ignore[attr-defined]
        wrapper.__name__ = handler.__name__
        # Marqueur introspectable (pas juste comportemental) : le test d'invariant
        # tag `legacy` ⟺ dépendance au vieil Odoo (#584) le lit via `route.endpoint`.
        wrapper.requires_odoo = requires_odoo

        target.get(path, tags=tags, dependencies=[Depends(get_current_api_key)])(wrapper)
        return wrapper

    return decorator
