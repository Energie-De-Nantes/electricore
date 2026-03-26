"""
Client HTTP async pour l'API ElectriCore.
Utilisé par le bot Telegram pour appeler les endpoints REST.
"""

import httpx
from electricore.api.config import settings


class ElectriCoreClient:
    """Client async vers l'API ElectriCore."""

    def __init__(self):
        self._base = settings.api_base_url.rstrip("/")
        self._headers = {"X-API-Key": settings.get_valid_api_keys()[0]} if settings.get_valid_api_keys() else {}

    async def list_tables(self) -> list[str]:
        async with httpx.AsyncClient() as c:
            r = await c.get(f"{self._base}/", timeout=10)
            r.raise_for_status()
            return r.json().get("available_tables", [])

    async def get_table_info(self, table: str) -> dict:
        async with httpx.AsyncClient() as c:
            r = await c.get(f"{self._base}/flux/{table}/info", headers=self._headers, timeout=10)
            r.raise_for_status()
            return r.json()

    async def run_etl(self, mode: str) -> dict:
        async with httpx.AsyncClient() as c:
            r = await c.post(
                f"{self._base}/etl/run",
                json={"mode": mode},
                headers=self._headers,
                timeout=10,
            )
            r.raise_for_status()
            return r.json()

    async def get_job(self, job_id: str) -> dict:
        async with httpx.AsyncClient() as c:
            r = await c.get(f"{self._base}/etl/jobs/{job_id}", headers=self._headers, timeout=10)
            r.raise_for_status()
            return r.json()

    async def get_jobs(self, limit: int = 5) -> list[dict]:
        async with httpx.AsyncClient() as c:
            r = await c.get(f"{self._base}/etl/jobs", params={"limit": limit}, headers=self._headers, timeout=10)
            r.raise_for_status()
            return r.json()

    async def get_entrees_xlsx(self) -> bytes:
        async with httpx.AsyncClient() as c:
            r = await c.get(f"{self._base}/flux/c15/entrees/xlsx", headers=self._headers, timeout=120)
            r.raise_for_status()
            return r.content

    async def get_sorties_xlsx(self) -> bytes:
        async with httpx.AsyncClient() as c:
            r = await c.get(f"{self._base}/flux/c15/sorties/xlsx", headers=self._headers, timeout=120)
            r.raise_for_status()
            return r.content

    async def get_xlsx(self, table: str) -> bytes:
        async with httpx.AsyncClient() as c:
            r = await c.get(f"{self._base}/flux/{table}/xlsx", headers=self._headers, timeout=120)
            r.raise_for_status()
            return r.content
