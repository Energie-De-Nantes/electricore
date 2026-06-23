"""Client Arrow historique : endpoints structurés en `pl.DataFrame` (extra `[arrow]`).

Ce sous-module **n'est jamais importé au top-level** de `electricore_client` :
il porte le seul code qui dépend de polars, et l'importe **paresseusement** (à
l'appel d'une méthode). La base du paquet reste donc polars-free — l'invariant
prouvé par le test de pureté tient même quand `[arrow]` n'est pas installé.

Endpoints Arrow IPC (`/flux/{table}.arrow`, `/releves.arrow`, `/facturation/
detail.arrow`, `/taxes/{accise,cta}/detail.arrow`) — pour les notebooks distants
qui n'ont pas la base DuckDB en local (ADR-0009). Lecture seule (ADR-0012).

Installer avec l'extra : `pip install "electricore-client[arrow]"`.
"""

from __future__ import annotations

import io
from typing import TYPE_CHECKING, Any

from .transport import _BaseClient

if TYPE_CHECKING:  # pragma: no cover - annotations only, jamais exécuté
    import polars as pl

_HINT_INSTALL = (
    'Le client Arrow nécessite polars : installez l\'extra `arrow` (`pip install "electricore-client[arrow]"`).'
)


def _polars():
    """Importe polars paresseusement, avec un message d'install clair s'il manque."""
    try:
        import polars as pl
    except ModuleNotFoundError as exc:  # pragma: no cover - testé via monkeypatch
        raise ModuleNotFoundError(_HINT_INSTALL) from exc
    return pl


class ElectricoreArrowClient(_BaseClient):
    """Client Arrow synchrone : endpoints structurés rendus en `pl.DataFrame`.

    Hérite du substrat de transport partagé (URL, `X-API-Key`, timeout, 503 →
    `IngestionEnCours`). polars n'est tiré qu'au premier appel de méthode.
    """

    def _get_arrow(self, path: str, params: dict) -> pl.DataFrame:
        pl = _polars()
        response = self._http.get(f"{self.url}{path}", params=params, headers=self._headers)
        self._raise_for_status(response)
        return pl.read_ipc_stream(io.BytesIO(response.content))

    def flux(
        self,
        table_name: str,
        *,
        prm: str | None = None,
        limit: int = 1_000_000,
    ) -> pl.DataFrame:
        """Contenu brut d'un flux Enedis (c15, r151, f15, etc.) en Polars.

        Équivalent HTTP des `c15()`/`r151()`/`f15()` du `DuckDBQuery` — pour les
        notebooks distants sans accès local à la base (ADR-0009).

        Args:
            table_name: nom de la table flux (`c15`, `r151`, `r15`, `f15_detail`, …).
            prm: filtre optionnel sur la colonne `pdl`.
            limit: nombre maximum de lignes (défaut 1 000 000, max serveur 10 000 000).
        """
        params: dict[str, Any] = {"limit": limit}
        if prm:
            params["prm"] = prm
        return self._get_arrow(f"/flux/{table_name}.arrow", params)

    def releves(
        self,
        *,
        prm: str | None = None,
        source: str | None = None,
        debut: str | None = None,
        fin: str | None = None,
        limit: int = 1_000_000,
    ) -> pl.DataFrame:
        """Mart de relevés canonique `releves` (ADR-0029) en Polars.

        Args:
            prm: filtre optionnel sur la colonne `pdl`.
            source: filtre optionnel (`flux_R151` / `flux_R64` / `flux_C15`).
            debut: borne basse incluse sur `date_releve` (ex. `"2025-01-01"`).
            fin: borne haute incluse sur `date_releve` (ex. `"2025-12-31"`).
            limit: nombre maximum de lignes (défaut 1 000 000, max serveur 10 000 000).
        """
        params: dict[str, Any] = {"limit": limit}
        if prm:
            params["prm"] = prm
        if source:
            params["source"] = source
        if debut:
            params["debut"] = debut
        if fin:
            params["fin"] = fin
        return self._get_arrow("/releves.arrow", params)

    def facturation(self, mois: str | None = None) -> pl.DataFrame:
        """`lignes_facture_rapprochees` pour le mois donné.

        Args:
            mois: "YYYY-MM-DD" — défaut : dernier mois disponible côté serveur.
        """
        return self._get_arrow("/facturation/detail.arrow", {"mois": mois} if mois else {})

    def accise(self, trimestre: str | None = None) -> pl.DataFrame:
        """Détail Accise TICFE pour le trimestre donné.

        Args:
            trimestre: "YYYY-TX" (ex. "2025-T1") — défaut : tous les trimestres.
        """
        return self._get_arrow("/taxes/accise/detail.arrow", {"trimestre": trimestre} if trimestre else {})

    def cta(self, trimestre: str | None = None) -> pl.DataFrame:
        """Détail CTA mensuel pour le trimestre donné.

        Args:
            trimestre: "YYYY-TX" (ex. "2025-T1") — défaut : tous les trimestres.
        """
        return self._get_arrow("/taxes/cta/detail.arrow", {"trimestre": trimestre} if trimestre else {})


__all__ = ["ElectricoreArrowClient"]
