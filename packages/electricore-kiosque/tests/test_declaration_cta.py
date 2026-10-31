"""Tests de l'app Kiosque « déclaration CTA » (#708, ADR-0058).

Helpers sur transport mocké (`httpx.MockTransport`, aucun réseau), notebook exécuté en
process via `app.run(defs=...)` (même patron que `test_exports.py`), helpers stubbés.
"""

from __future__ import annotations

import io

import httpx
import polars as pl
import pytest
from electricore_kiosque import declaration_cta, helpers

_LIGNES = [
    {
        "ref_situation_contractuelle": rsc,
        "pdl": pdl,
        "mois_annee": mois,
        "trimestre": trimestre,
        "turpe_fixe_eur": 10.0,
        "taux_cta_pct": 15.0,
        "cta_eur": 1.5,
        "source_hash": f"hash-{rsc}-{mois}",
        "empreinte_trimestre": f"empreinte-{trimestre}",
        "version_empreinte": 1,
    }
    for trimestre, mois in (("2026-T2", "2026-04"), ("2026-T3", "2026-07"))
    for rsc, pdl in (("RSC-1", "PDL-1"), ("RSC-2", "PDL-2"))
]


@pytest.fixture(autouse=True)
def _api_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KIOSQUE__API_URL", "https://kiosque.test.invalide")


def _http(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


# =============================================================================
# Helpers
# =============================================================================


def test_recuperer_declaration_cta_lit_l_arrow_de_l_api() -> None:
    vus: list[httpx.URL] = []

    def handler(request: httpx.Request) -> httpx.Response:
        vus.append(request.url)
        buf = io.BytesIO()
        pl.DataFrame(_LIGNES).write_ipc_stream(buf)
        return httpx.Response(200, content=buf.getvalue())

    lignes = helpers.recuperer_declaration_cta("cle", http_client=_http(handler))

    assert vus[0].path == "/taxes/cta/declaration.arrow"
    assert lignes == _LIGNES


def test_telecharger_declaration_cta_rend_les_octets_de_l_api() -> None:
    vus: list[httpx.URL] = []

    def handler(request: httpx.Request) -> httpx.Response:
        vus.append(request.url)
        return httpx.Response(200, content=b"PK\x03\x04xlsx")

    assert helpers.telecharger_declaration_cta("cle", "2026-T3", http_client=_http(handler)) == b"PK\x03\x04xlsx"
    assert vus[0].path == "/taxes/cta/declaration.xlsx"
    assert vus[0].params.get("trimestre") == "2026-T3"


def test_trimestre_non_termine_devient_un_message_affichable() -> None:
    motif = "Le trimestre 2026-T4 n'est pas terminé : seul un trimestre terminé se déclare."

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(422, json={"detail": motif}, headers={"X-Error-Kind": "precondition"})

    with pytest.raises(helpers.ServiceIndisponible, match="seul un trimestre terminé"):
        helpers.telecharger_declaration_cta("cle", "2026-T4", http_client=_http(handler))


def test_cle_refusee_sur_la_declaration() -> None:
    with pytest.raises(helpers.CleApiRefusee):
        helpers.recuperer_declaration_cta("cle", http_client=_http(lambda request: httpx.Response(401)))


# =============================================================================
# Notebook
# =============================================================================


class _FakeCle:
    def __init__(self, value: str) -> None:
        self.value = value


def _rendu(monkeypatch: pytest.MonkeyPatch, *, lignes=_LIGNES, xlsx=b"PK\x03\x04", erreur=None) -> str:
    def _recuperer(cle: str, **kwargs: object) -> list[dict]:
        if erreur is not None:
            raise erreur
        return lignes

    monkeypatch.setattr(helpers, "recuperer_declaration_cta", _recuperer)
    monkeypatch.setattr(helpers, "telecharger_declaration_cta", lambda cle, trimestre, **kw: xlsx)
    sorties, _ = declaration_cta.app.run(defs={"cle": _FakeCle("une-cle")})
    return sorties[-1].text


def test_notebook_affiche_le_dernier_trimestre_son_total_et_son_empreinte(monkeypatch: pytest.MonkeyPatch) -> None:
    texte = _rendu(monkeypatch)

    assert "2026-T3" in texte
    assert "empreinte-2026-T3" in texte
    assert "3.00 €" in texte  # 2 lignes × 1,50 € — somme d'affichage du seul trimestre choisi
    assert "empreinte-2026-T2" not in texte
    assert "declaration_cta_2026-T3.xlsx" in texte


def test_notebook_aucun_trimestre_termine(monkeypatch: pytest.MonkeyPatch) -> None:
    assert "Aucun trimestre terminé" in _rendu(monkeypatch, lignes=[])


@pytest.mark.parametrize(
    "erreur",
    [helpers.CleApiRefusee(), helpers.ServiceIndisponible("Ingestion en cours côté serveur — réessaie.")],
)
def test_notebook_erreurs_affichees_sans_traceback(monkeypatch: pytest.MonkeyPatch, erreur: Exception) -> None:
    texte = _rendu(monkeypatch, erreur=erreur)
    assert str(erreur) in texte
    assert "Traceback" not in texte


def test_notebook_sans_cle_attend() -> None:
    sorties, _ = declaration_cta.app.run(defs={"cle": _FakeCle("")})
    assert "En attente d'une clé API" in sorties[-1].text
