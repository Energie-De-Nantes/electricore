"""Tests d'intégration de `GET /facturation/chronologie` (vue facturiste, #367).

Endpoint de lecture (ADR-0027/0012) : frise complète d'un point/contrat + verdicts, **sans
montant tarifaire**. Le seam de test est la fonction `chronologie_point_ou_contrat` référencée
par le router (même patron que `tests/integration/test_meta_periodes_endpoint.py`) — on
court-circuite la reconstruction DuckDB, le chemin transport + enveloppe reste réel.

Couvre aussi (acceptance #367) un **point à plusieurs RSC** (changement de main) et un **point
récemment activé** (MDPRM → verdict communication correct, post-#365) en faisant tisser le vrai
service depuis un `ContexteMensuel` synthétique.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl
import pytest
from fastapi.testclient import TestClient

from electricore.api.main import app
from electricore.api.security import get_current_api_key
from electricore.core.builds.contexte_mensuel import ContexteMensuel

PARIS = ZoneInfo("Europe/Paris")


@pytest.fixture
def frise_synthetique() -> pl.DataFrame:
    """Mime la sortie de `chronologie_point_ou_contrat` : frise hétérogène ordonnée."""
    return pl.DataFrame(
        [
            {
                "type_ligne": "evenement",
                "date": datetime(2024, 1, 5, tzinfo=PARIS),
                "pdl": "PDL_X",
                "ref_situation_contractuelle": "REF_1",
                "evenement_declencheur": "MES",
                "impacte_abonnement": True,
            },
            {
                "type_ligne": "periode_energie",
                "date": datetime(2024, 1, 5, tzinfo=PARIS),
                "pdl": "PDL_X",
                "ref_situation_contractuelle": "REF_1",
                "qualite": "réelle",
                "statut_communication": "communicante",
                "energie_base_kwh": 50.0,
            },
        ],
        infer_schema_length=None,
    )


def test_chronologie_retourne_enveloppe_json(monkeypatch, frise_synthetique):
    """Tracer bullet : GET par PDL sert la frise en JSON enveloppé, grain `point`."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    monkeypatch.setattr(
        "electricore.api.routers.chronologie.chronologie_point_ou_contrat",
        lambda pdl=None, rsc=None: frise_synthetique,
    )
    try:
        response = TestClient(app).get("/facturation/chronologie", params={"pdl": "PDL_X"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()
    assert body["grain"] == "point"
    assert body["filters"] == {"pdl": "PDL_X"}
    assert body["pagination"]["total"] == 2
    types = {r["type_ligne"] for r in body["data"]}
    assert types == {"evenement", "periode_energie"}


def test_chronologie_par_rsc_grain_contrat(monkeypatch, frise_synthetique):
    """Le grain contrat (`rsc`) est servi avec `grain=contrat`."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    appels: list[tuple] = []

    def _capture(pdl=None, rsc=None):
        appels.append((pdl, rsc))
        return frise_synthetique

    monkeypatch.setattr("electricore.api.routers.chronologie.chronologie_point_ou_contrat", _capture)
    try:
        response = TestClient(app).get("/facturation/chronologie", params={"rsc": "REF_1"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert appels == [(None, "REF_1")]
    assert response.json()["grain"] == "contrat"


def test_chronologie_pas_de_montant_tarifaire(monkeypatch, frise_synthetique):
    """Différenciateur vs /meta-periodes : aucun montant tarifaire dans le payload."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    monkeypatch.setattr(
        "electricore.api.routers.chronologie.chronologie_point_ou_contrat",
        lambda pdl=None, rsc=None: frise_synthetique,
    )
    try:
        response = TestClient(app).get("/facturation/chronologie", params={"pdl": "PDL_X"})
    finally:
        app.dependency_overrides.clear()

    payload = response.text
    assert "turpe" not in payload
    assert "cta_eur" not in payload
    assert "accise" not in payload


def test_chronologie_refuse_sans_api_key():
    """Endpoint sécurisé : 401 sans clé."""
    response = TestClient(app).get("/facturation/chronologie", params={"pdl": "PDL_X"})
    assert response.status_code == 401


def test_chronologie_exige_un_grain(monkeypatch):
    """Sans `pdl` ni `rsc` → 422 (un grain est requis)."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    try:
        response = TestClient(app).get("/facturation/chronologie")
    finally:
        app.dependency_overrides.clear()
    assert response.status_code == 422


def test_chronologie_pagine(monkeypatch, frise_synthetique):
    """`limit`/`offset` tranchent ; `total` reste le total non paginé."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    monkeypatch.setattr(
        "electricore.api.routers.chronologie.chronologie_point_ou_contrat",
        lambda pdl=None, rsc=None: frise_synthetique,
    )
    try:
        response = TestClient(app).get("/facturation/chronologie", params={"pdl": "PDL_X", "limit": 1, "offset": 1})
    finally:
        app.dependency_overrides.clear()
    body = response.json()
    assert body["pagination"] == {"limit": 1, "offset": 1, "returned": 1, "total": 2}
    assert len(body["data"]) == 1


# =============================================================================
# Acceptance #367 : tissage réel depuis un ContexteMensuel synthétique
# =============================================================================


def _ctx_multi_rsc() -> ContexteMensuel:
    """Point PDL_X passé de REF_1 (RES) à REF_2 (CFNE) — changement de main."""
    historique = pl.DataFrame(
        [
            {
                "date_evenement": datetime(2024, 1, 5, tzinfo=PARIS),
                "pdl": "PDL_X",
                "ref_situation_contractuelle": "REF_1",
                "source": "flux_C15",
                "type_fait": "evenement",
                "evenement_declencheur": "MES",
                "niveau_ouverture_services": "2",
                "impacte_abonnement": True,
            },
            {
                "date_evenement": datetime(2024, 6, 1, tzinfo=PARIS),
                "pdl": "PDL_X",
                "ref_situation_contractuelle": "REF_1",
                "source": "flux_C15",
                "type_fait": "evenement",
                "evenement_declencheur": "RES",
                "niveau_ouverture_services": "2",
                "impacte_abonnement": True,
            },
            {
                "date_evenement": datetime(2024, 6, 2, tzinfo=PARIS),
                "pdl": "PDL_X",
                "ref_situation_contractuelle": "REF_2",
                "source": "flux_C15",
                "type_fait": "evenement",
                "evenement_declencheur": "CFNE",
                "niveau_ouverture_services": "2",
                "impacte_abonnement": True,
            },
        ],
        schema_overrides={"date_evenement": pl.Datetime("us", "Europe/Paris")},
    )
    return ContexteMensuel(
        mois="2024-06-01",
        historique_enrichi=historique.lazy(),
        abonnements=pl.LazyFrame({}),
        energie=pl.LazyFrame({}),
        releves_utilises=pl.LazyFrame({}),
        facturation_mensuelle=pl.DataFrame({}),
    )


def _ctx_recemment_active() -> ContexteMensuel:
    """PDL_Y récemment activé : MES (niveau 2) puis MDPRM hors-comptage (toujours niveau 2),
    une période d'énergie réelle ET communicante. Post-#365 : le verdict communication ne doit
    PAS retomber à `non_communicante` faute de niveau forward-fillé depuis un relevé indexé.
    """
    historique = pl.DataFrame(
        [
            {
                "date_evenement": datetime(2024, 3, 11, tzinfo=PARIS),
                "pdl": "PDL_Y",
                "ref_situation_contractuelle": "REF_Y",
                "source": "flux_C15",
                "type_fait": "evenement",
                "evenement_declencheur": "MES",
                "niveau_ouverture_services": "2",
                "impacte_abonnement": True,
            },
            {
                "date_evenement": datetime(2024, 3, 16, tzinfo=PARIS),
                "pdl": "PDL_Y",
                "ref_situation_contractuelle": "REF_Y",
                "source": "flux_C15",
                "type_fait": "evenement",
                "evenement_declencheur": "MDPRM",  # hors-comptage
                "niveau_ouverture_services": "2",
                "impacte_abonnement": False,
            },
        ],
        schema_overrides={"date_evenement": pl.Datetime("us", "Europe/Paris")},
    )
    energie = pl.DataFrame(
        [
            {
                "pdl": "PDL_Y",
                "ref_situation_contractuelle": "REF_Y",
                "debut": datetime(2024, 4, 1, tzinfo=PARIS),
                "fin": datetime(2024, 5, 1, tzinfo=PARIS),
                "nb_jours": 30,
                "qualite": "réelle",
                "statut_communication": "communicante",
                "energie_base_kwh": 120.0,
            }
        ],
        schema_overrides={
            "debut": pl.Datetime("us", "Europe/Paris"),
            "fin": pl.Datetime("us", "Europe/Paris"),
        },
    )
    return ContexteMensuel(
        mois="2024-04-01",
        historique_enrichi=historique.lazy(),
        abonnements=pl.LazyFrame({}),
        energie=energie.lazy(),
        releves_utilises=pl.LazyFrame({}),
        facturation_mensuelle=pl.DataFrame({}),
    )


def test_chronologie_point_plusieurs_rsc_via_service(monkeypatch):
    """Acceptance : un point à plusieurs RSC trace ses deux situations (changement de main)."""
    import electricore.api.services.chronologie_service as svc

    monkeypatch.setattr(svc, "contexte_du_mois_filtre", lambda pdl=None, rsc=None, horizon=None: _ctx_multi_rsc())
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    try:
        response = TestClient(app).get("/facturation/chronologie", params={"pdl": "PDL_X"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    data = response.json()["data"]
    rsc = {r["ref_situation_contractuelle"] for r in data if r["type_ligne"] == "evenement"}
    assert rsc == {"REF_1", "REF_2"}
    # Les charnières (RES sortie / CFNE entrée) sont visibles comme faits.
    evts = {r["evenement_declencheur"] for r in data if r["type_ligne"] == "evenement"}
    assert {"RES", "CFNE"} <= evts


def test_chronologie_point_recemment_active_communication_correcte(monkeypatch):
    """Acceptance post-#365 : MDPRM présent comme fait, et la période réelle reste
    `communicante` (le verdict n'est pas faussé par un niveau périmé)."""
    import electricore.api.services.chronologie_service as svc

    monkeypatch.setattr(
        svc, "contexte_du_mois_filtre", lambda pdl=None, rsc=None, horizon=None: _ctx_recemment_active()
    )
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    try:
        response = TestClient(app).get("/facturation/chronologie", params={"pdl": "PDL_Y"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    data = response.json()["data"]
    # MDPRM (hors-comptage) figure dans la frise.
    evts = {r["evenement_declencheur"] for r in data if r["type_ligne"] == "evenement"}
    assert "MDPRM" in evts
    # La période d'énergie porte le verdict communication CORRECT (communicante).
    periode = next(r for r in data if r["type_ligne"] == "periode_energie")
    assert periode["statut_communication"] == "communicante"
    assert periode["qualite"] == "réelle"
