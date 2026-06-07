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

    def flux(
        self,
        table_name: str,
        *,
        prm: str | None = None,
        limit: int = 1_000_000,
    ) -> pl.DataFrame:
        """Récupère le contenu brut d'un flux Enedis (c15, r151, f15, etc.) en Polars.

        Sert d'équivalent HTTP des fonctions `c15()`, `r151()`, `f15()` du
        `DuckDBQuery` — pour les notebooks distants qui n'ont pas accès local
        à la base DuckDB (cf. ADR-0009).

        Args:
            table_name: Nom de la table flux (`c15`, `r151`, `r15`, `f15_detail`, etc.).
            prm: Filtre optionnel sur la colonne `pdl`.
            limit: Nombre maximum de lignes (défaut 1 000 000, max serveur 10 000 000).

        Returns:
            `pl.DataFrame` typé (timezones préservées).
        """
        params: dict[str, str | int] = {"limit": limit}
        if prm:
            params["prm"] = prm
        return self._get_arrow(f"/flux/{table_name}.arrow", params)

    def facturation(self, mois: str | None = None) -> pl.DataFrame:
        """Récupère `lignes_facture_rapprochees` pour le mois donné.

        Args:
            mois: "YYYY-MM-DD" — défaut : dernier mois disponible côté serveur.
        """
        return self._get_arrow("/facturation/detail.arrow", {"mois": mois} if mois else {})

    def accise(self, trimestre: str | None = None) -> pl.DataFrame:
        """Récupère le détail Accise TICFE pour le trimestre donné.

        Args:
            trimestre: "YYYY-TX" (ex: "2025-T1") — défaut : tous les trimestres.
        """
        return self._get_arrow("/taxes/accise/detail.arrow", {"trimestre": trimestre} if trimestre else {})

    def cta(self, trimestre: str | None = None) -> pl.DataFrame:
        """Récupère le détail CTA mensuel pour le trimestre donné.

        Args:
            trimestre: "YYYY-TX" (ex: "2025-T1") — défaut : tous les trimestres.
        """
        return self._get_arrow("/taxes/cta/detail.arrow", {"trimestre": trimestre} if trimestre else {})


__all__ = ["ElectricoreClient"]
