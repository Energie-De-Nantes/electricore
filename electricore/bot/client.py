"""
Client HTTP async pour l'API ElectriCore.
Utilisé par le bot Telegram pour appeler les endpoints REST.

Le squelette httpx (ouverture du client, clé API, `raise_for_status`) vit dans
les primitives `_get_json` / `_get_bytes` / `_post_json` (#174) ; les méthodes
publiques ne déclarent que le chemin et le budget de timeout.
"""

import httpx

from electricore.api.config import settings

# Budgets de timeout par profil d'endpoint (secondes)
TIMEOUT_COURT = 10  # JSON légers : statuts, listes, infos de table
TIMEOUT_EXPORT = 120  # exports XLSX de tables flux
TIMEOUT_LOURD = 300  # livrables calculés : taxes, facturation, check Odoo


class ElectriCoreClient:
    """Client async vers l'API ElectriCore."""

    def __init__(self, transport: httpx.AsyncBaseTransport | None = None):
        self._base = settings.api_base_url.rstrip("/")
        self._headers = {"X-API-Key": settings.get_valid_api_keys()[0]} if settings.get_valid_api_keys() else {}
        self._transport = transport

    def _http(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=self._transport)

    async def _get(self, path: str, *, timeout: float, params: dict | None = None) -> httpx.Response:
        async with self._http() as c:
            r = await c.get(f"{self._base}{path}", headers=self._headers, params=params, timeout=timeout)
            r.raise_for_status()
            return r

    async def _get_json(self, path: str, *, timeout: float = TIMEOUT_COURT, params: dict | None = None):
        return (await self._get(path, timeout=timeout, params=params)).json()

    async def _get_bytes(self, path: str, *, timeout: float = TIMEOUT_EXPORT, params: dict | None = None) -> bytes:
        return (await self._get(path, timeout=timeout, params=params)).content

    async def _post_json(self, path: str, *, json: dict, timeout: float = TIMEOUT_COURT):
        async with self._http() as c:
            r = await c.post(f"{self._base}{path}", json=json, headers=self._headers, timeout=timeout)
            r.raise_for_status()
            return r.json()

    async def list_tables(self) -> list[str]:
        data = await self._get_json("/")
        return data.get("available_tables", [])

    async def get_table_info(self, table: str) -> dict:
        return await self._get_json(f"/flux/{table}/info")

    async def run_etl(self, mode: str) -> dict:
        return await self._post_json("/etl/run", json={"mode": mode})

    async def get_job(self, job_id: str) -> dict:
        return await self._get_json(f"/etl/jobs/{job_id}")

    async def get_jobs(self, limit: int = 5) -> list[dict]:
        return await self._get_json("/etl/jobs", params={"limit": limit})

    async def get_entrees_xlsx(self) -> bytes:
        return await self._get_bytes("/flux/c15/entrees.xlsx")

    async def get_sorties_xlsx(self) -> bytes:
        return await self._get_bytes("/flux/c15/sorties.xlsx")

    async def get_xlsx(self, table: str) -> bytes:
        return await self._get_bytes(f"/flux/{table}.xlsx")

    async def get_accise_xlsx(self, trimestre: str | None = None) -> bytes:
        params = {"trimestre": trimestre} if trimestre else None
        return await self._get_bytes("/taxes/accise/rapport.xlsx", params=params, timeout=TIMEOUT_LOURD)

    async def get_cta_xlsx(self, trimestre: str | None = None) -> bytes:
        params = {"trimestre": trimestre} if trimestre else None
        return await self._get_bytes("/taxes/cta/rapport.xlsx", params=params, timeout=TIMEOUT_LOURD)

    async def get_facturation_xlsx(self, mois: str | None = None) -> bytes:
        params = {"mois": mois} if mois else None
        return await self._get_bytes("/facturation/rapport.xlsx", params=params, timeout=TIMEOUT_LOURD)

    async def get_facturation_documents_xlsx(self, mois: str | None = None) -> bytes:
        """Livrable XLSX multi-onglets des documents de campagne facturation (cf. #78)."""
        params = {"mois": mois} if mois else None
        return await self._get_bytes("/facturation/documents.xlsx", params=params, timeout=TIMEOUT_LOURD)

    async def check_facturation_odoo(self) -> dict:
        return await self._get_json("/facturation/check/odoo", timeout=TIMEOUT_LOURD)

    async def get_check_odoo_xlsx(self) -> bytes:
        """Détail complet du check Odoo en XLSX multi-onglets (issue #150)."""
        return await self._get_bytes("/facturation/check/odoo.xlsx", timeout=TIMEOUT_LOURD)
