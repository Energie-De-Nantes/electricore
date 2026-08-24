"""Tests des helpers clé API → client `electricore-client` (#705, #720).

Transport HTTP mocké (`httpx.MockTransport`) : aucun réseau, même patron que
`electricore_client/tests/test_transport.py` et `electricore_client/tests/test_arrow.py`.
"""

from __future__ import annotations

import io
import json
from datetime import date

import httpx
import polars as pl
import pytest
from electricore_kiosque.helpers import (
    LIMITE_LIGNES,
    CleApiRefusee,
    ServiceIndisponible,
    TableFluxAbsente,
    construire_client,
    construire_client_arrow,
    fenetre_dernier_mois,
    recuperer_flux,
    recuperer_meta_periodes,
    recuperer_releves,
)


@pytest.fixture(autouse=True)
def _api_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """`KIOSQUE__API_URL` requise (fail-fast, pas de défaut) — posée pour les tests
    qui ne portent pas spécifiquement sur cette configuration."""
    monkeypatch.setenv("KIOSQUE__API_URL", "https://kiosque.test.invalide")


_LIGNE = {
    "ref_situation_contractuelle": "RSC123",
    "pdl": "PDL456",
    "mois_annee": "2026-05",
    "debut": "2026-05-01",
    "fin": "2026-06-01",
    "nb_jours": 31,
    "turpe_fixe_eur": 12.3,
    "source_hash": "abc123",
}


def _http(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def _reponse_arrow(df: pl.DataFrame, *, status_code: int = 200) -> httpx.Response:
    buf = io.BytesIO()
    df.write_ipc_stream(buf)
    return httpx.Response(status_code, content=buf.getvalue())


def test_construire_client_positionne_url_et_cle(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KIOSQUE__API_URL", "https://kiosque.exemple.fr")
    client = construire_client("ma-cle")
    assert client.url == "https://kiosque.exemple.fr"
    assert client.api_key == "ma-cle"


def test_recuperer_meta_periodes_retourne_les_lignes_et_ferme_le_client() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"X-Contract-Version": "3"},
            content=(json.dumps(_LIGNE) + "\n").encode(),
        )

    http = _http(handler)
    lignes = recuperer_meta_periodes("une-cle", http_client=http)

    assert len(lignes) == 1
    assert lignes[0]["ref_situation_contractuelle"] == "RSC123"
    assert lignes[0]["turpe_fixe_eur"] == 12.3
    assert "releves_utilises" not in lignes[0]
    assert http.is_closed  # chaque appel referme sa connexion (kiosque multi-visiteurs)


def test_recuperer_meta_periodes_cle_refusee_ferme_quand_meme_le_client() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401)

    http = _http(handler)
    with pytest.raises(CleApiRefusee, match="admin"):
        recuperer_meta_periodes("une-cle", http_client=http)
    assert http.is_closed


def test_recuperer_meta_periodes_propage_les_autres_erreurs_et_ferme_le_client() -> None:
    """Une erreur qui n'est pas une clé refusée reste une erreur HTTP normale."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    http = _http(handler)
    with pytest.raises(httpx.HTTPStatusError):
        recuperer_meta_periodes("une-cle", http_client=http)
    assert http.is_closed


# -- erreurs opérationnelles (#722) --------------------------------------------


def test_recuperer_meta_periodes_ingestion_en_cours_ferme_quand_meme_le_client() -> None:
    """503 + X-Error-Kind réel (`electricore_client._raise_for_status`) → message clair."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, headers={"X-Error-Kind": "ingestion-lock"})

    http = _http(handler)
    with pytest.raises(ServiceIndisponible, match="Ingestion en cours"):
        recuperer_meta_periodes("une-cle", http_client=http)
    assert http.is_closed


def test_recuperer_meta_periodes_api_injoignable_ferme_quand_meme_le_client() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connexion refusée", request=request)

    http = _http(handler)
    with pytest.raises(ServiceIndisponible, match="injoignable"):
        recuperer_meta_periodes("une-cle", http_client=http)
    assert http.is_closed


def test_recuperer_meta_periodes_version_desynchronisee_ferme_quand_meme_le_client() -> None:
    """Serveur en retard (`X-Contract-Version` < attendue) → message « préviens ton admin »."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers={"X-Contract-Version": "1"}, content=b"")

    http = _http(handler)
    with pytest.raises(ServiceIndisponible, match="admin"):
        recuperer_meta_periodes("une-cle", http_client=http)
    assert http.is_closed


# -- client Arrow (#720) -------------------------------------------------------


def test_construire_client_arrow_positionne_url_et_cle(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KIOSQUE__API_URL", "https://kiosque.exemple.fr")
    client = construire_client_arrow("ma-cle")
    assert client.url == "https://kiosque.exemple.fr"
    assert client.api_key == "ma-cle"


# -- recuperer_releves ----------------------------------------------------------


def test_recuperer_releves_retourne_les_lignes_transmet_les_bornes_et_ferme_le_client() -> None:
    captures: list[httpx.URL] = []
    df = pl.DataFrame({"pdl": ["PDL456"], "date_releve": ["2026-05-01"]})

    def handler(request: httpx.Request) -> httpx.Response:
        captures.append(request.url)
        return _reponse_arrow(df)

    http = _http(handler)
    lignes, tronque = recuperer_releves("une-cle", prm="PDL456", debut="2026-05-01", fin="2026-06-01", http_client=http)

    assert lignes == df.to_dicts()
    assert tronque is False
    assert http.is_closed
    params = captures[0].params
    assert params.get("prm") == "PDL456"
    assert params.get("debut") == "2026-05-01"
    assert params.get("fin") == "2026-06-01"
    assert params.get("limit") == str(LIMITE_LIGNES)


def test_recuperer_releves_signale_la_troncature_quand_la_limite_est_atteinte() -> None:
    """La limite dure côté kiosque (`LIMITE_LIGNES`) déclenche le bandeau « vue tronquée »."""
    df = pl.DataFrame({"pdl": ["PDL456"] * LIMITE_LIGNES})

    def handler(request: httpx.Request) -> httpx.Response:
        return _reponse_arrow(df)

    http = _http(handler)
    lignes, tronque = recuperer_releves("une-cle", http_client=http)

    assert len(lignes) == LIMITE_LIGNES
    assert tronque is True
    assert http.is_closed


def test_recuperer_releves_cle_refusee_ferme_quand_meme_le_client() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401)

    http = _http(handler)
    with pytest.raises(CleApiRefusee, match="admin"):
        recuperer_releves("une-cle", http_client=http)
    assert http.is_closed


def test_recuperer_releves_propage_les_autres_erreurs_et_ferme_le_client() -> None:
    """Une erreur qui n'est pas une clé refusée reste une erreur HTTP normale."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    http = _http(handler)
    with pytest.raises(httpx.HTTPStatusError):
        recuperer_releves("une-cle", http_client=http)
    assert http.is_closed


def test_recuperer_releves_ingestion_en_cours_ferme_quand_meme_le_client() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, headers={"X-Error-Kind": "ingestion-lock"})

    http = _http(handler)
    with pytest.raises(ServiceIndisponible, match="Ingestion en cours"):
        recuperer_releves("une-cle", http_client=http)
    assert http.is_closed


def test_recuperer_releves_api_injoignable_ferme_quand_meme_le_client() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connexion refusée", request=request)

    http = _http(handler)
    with pytest.raises(ServiceIndisponible, match="injoignable"):
        recuperer_releves("une-cle", http_client=http)
    assert http.is_closed


# -- recuperer_flux ---------------------------------------------------------------


def test_recuperer_flux_retourne_les_lignes_transmet_la_limite_et_ferme_le_client() -> None:
    captures: list[httpx.URL] = []
    df = pl.DataFrame({"pdl": ["PDL456"], "evenement_declencheur": ["MES"]})

    def handler(request: httpx.Request) -> httpx.Response:
        captures.append(request.url)
        return _reponse_arrow(df)

    http = _http(handler)
    lignes, tronque = recuperer_flux("une-cle", "c15", prm="PDL456", http_client=http)

    assert lignes == df.to_dicts()
    assert tronque is False
    assert http.is_closed
    assert captures[0].params.get("prm") == "PDL456"
    assert captures[0].params.get("limit") == str(LIMITE_LIGNES)


def test_recuperer_flux_signale_la_troncature_quand_la_limite_est_atteinte() -> None:
    """Même plafond dur que `recuperer_releves` (#721) : une table de flux brute
    peut être aussi volumineuse que le mart de relevés."""
    df = pl.DataFrame({"pdl": ["PDL456"] * LIMITE_LIGNES})

    def handler(request: httpx.Request) -> httpx.Response:
        return _reponse_arrow(df)

    http = _http(handler)
    lignes, tronque = recuperer_flux("une-cle", "c15", http_client=http)

    assert len(lignes) == LIMITE_LIGNES
    assert tronque is True
    assert http.is_closed


def test_recuperer_flux_table_absente_ferme_quand_meme_le_client() -> None:
    """Table hors registre côté box → message propre, pas de stacktrace (404 API)."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    http = _http(handler)
    with pytest.raises(TableFluxAbsente, match="inconnue"):
        recuperer_flux("une-cle", "inconnue", http_client=http)
    assert http.is_closed


def test_recuperer_flux_cle_refusee_ferme_quand_meme_le_client() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401)

    http = _http(handler)
    with pytest.raises(CleApiRefusee, match="admin"):
        recuperer_flux("une-cle", "c15", http_client=http)
    assert http.is_closed


def test_recuperer_flux_propage_les_autres_erreurs_et_ferme_le_client() -> None:
    """Une erreur qui n'est pas une clé refusée ni un 404 reste une erreur HTTP normale."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    http = _http(handler)
    with pytest.raises(httpx.HTTPStatusError):
        recuperer_flux("une-cle", "c15", http_client=http)
    assert http.is_closed


def test_recuperer_flux_ingestion_en_cours_ferme_quand_meme_le_client() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, headers={"X-Error-Kind": "ingestion-lock"})

    http = _http(handler)
    with pytest.raises(ServiceIndisponible, match="Ingestion en cours"):
        recuperer_flux("une-cle", "c15", http_client=http)
    assert http.is_closed


def test_recuperer_flux_api_injoignable_ferme_quand_meme_le_client() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connexion refusée", request=request)

    http = _http(handler)
    with pytest.raises(ServiceIndisponible, match="injoignable"):
        recuperer_flux("une-cle", "c15", http_client=http)
    assert http.is_closed


# -- fenetre_dernier_mois -----------------------------------------------------


def test_fenetre_dernier_mois_renvoie_le_mois_calendaire_precedent() -> None:
    debut, fin = fenetre_dernier_mois(date(2026, 8, 24))
    assert (debut, fin) == ("2026-07-01", "2026-07-31")


def test_fenetre_dernier_mois_traverse_le_changement_d_annee() -> None:
    debut, fin = fenetre_dernier_mois(date(2026, 1, 15))
    assert (debut, fin) == ("2025-12-01", "2025-12-31")
