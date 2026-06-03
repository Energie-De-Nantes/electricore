"""Client Python pour l'API electricore.

Permet à un notebook distant de consommer les endpoints structurés (Arrow IPC).
Voir `docs/adr/0012-api-read-only-odoo.md` pour la politique read-only.
"""

import io

import httpx
import polars as pl

DEFAULT_TIMEOUT = httpx.Timeout(30.0, read=120.0)


class ElectricoreClient:
    """Client HTTP synchrone vers une instance de l'API electricore."""

    def __init__(
        self,
        url: str,
        api_key: str,
        *,
        http_client: httpx.Client | None = None,
    ) -> None:
        self.url = url.rstrip("/")
        self.api_key = api_key
        self._http = http_client or httpx.Client(timeout=DEFAULT_TIMEOUT)

    def _get_arrow(self, path: str, params: dict) -> pl.DataFrame:
        response = self._http.get(
            f"{self.url}{path}",
            params=params,
            headers={"X-API-Key": self.api_key},
        )
        response.raise_for_status()
        return pl.read_ipc_stream(io.BytesIO(response.content))

    def facturation(self, mois: str | None = None) -> pl.DataFrame:
        """Récupère `lignes_facture_rapprochees` pour le mois donné.

        Args:
            mois: "YYYY-MM-DD" — défaut : dernier mois disponible côté serveur.
        """
        return self._get_arrow("/facturation/arrow", {"mois": mois} if mois else {})

    def accise(self, trimestre: str | None = None) -> pl.DataFrame:
        """Récupère le détail Accise TICFE pour le trimestre donné.

        Args:
            trimestre: "YYYY-TX" (ex: "2025-T1") — défaut : tous les trimestres.
        """
        return self._get_arrow("/taxes/accise/arrow", {"trimestre": trimestre} if trimestre else {})

    def cta(self, trimestre: str | None = None) -> pl.DataFrame:
        """Récupère le détail CTA mensuel pour le trimestre donné.

        Args:
            trimestre: "YYYY-TX" (ex: "2025-T1") — défaut : tous les trimestres.
        """
        return self._get_arrow("/taxes/cta/arrow", {"trimestre": trimestre} if trimestre else {})


__all__ = ["ElectricoreClient"]
