"""Tests d'intégration de `GET /facturation/chronologie` (vue facturiste, #367/#408).

Endpoint de lecture (ADR-0027/0012) : frise complète d'un point/contrat + verdicts, **sans
montant tarifaire**. Depuis #408, la réponse est un **flux JSONL** (`application/jsonl`, une
ligne = un `LigneChronologie` — union discriminée sur `type_ligne`) avec métadonnées en
en-têtes (`X-Contract-Version`, `X-Grain`). Le seam de test est la fonction
`chronologie_point_ou_contrat` référencée par le router — on court-circuite la reconstruction
DuckDB, le chemin transport + sérialisation reste réel.

Couvre aussi (acceptance #367) un **point à plusieurs RSC** (changement de main) et un **point
récemment activé** (MDPRM → verdict communication correct, post-#365) en faisant tisser le vrai
service depuis un `ContexteMensuel` synthétique.
"""

import json
from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl
import pytest
from electricore_client.models import (
    LigneEvenement,
    LignePeriodeEnergie,
    valider_ligne_chronologie,
)
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


def _lignes(response) -> list[dict]:
    return [json.loads(ligne) for ligne in response.text.splitlines() if ligne.strip()]


def test_chronologie_streame_du_jsonl(monkeypatch, frise_synthetique):
    """Tracer bullet : GET par PDL sert la frise en JSONL, grain `point` en en-tête."""
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
    assert response.headers["content-type"].startswith("application/jsonl")
    assert response.headers["X-Grain"] == "point"
    assert response.headers["X-Contract-Version"] == "1"

    lignes = _lignes(response)
    assert len(lignes) == 2
    assert {ligne["type_ligne"] for ligne in lignes} == {"evenement", "periode_energie"}


def test_chronologie_lignes_resolvent_lunion(monkeypatch, frise_synthetique):
    """Chaque ligne se résout en son sous-type via l'union discriminée client."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    monkeypatch.setattr(
        "electricore.api.routers.chronologie.chronologie_point_ou_contrat",
        lambda pdl=None, rsc=None: frise_synthetique,
    )
    try:
        response = TestClient(app).get("/facturation/chronologie", params={"pdl": "PDL_X"})
    finally:
        app.dependency_overrides.clear()

    typees = [valider_ligne_chronologie(ligne) for ligne in _lignes(response)]
    assert isinstance(typees[0], LigneEvenement)
    assert isinstance(typees[1], LignePeriodeEnergie)
    assert typees[1].energie_base_kwh == 50.0


def test_chronologie_par_rsc_grain_contrat(monkeypatch, frise_synthetique):
    """Le grain contrat (`rsc`) est servi avec `X-Grain: contrat`."""
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
    assert response.headers["X-Grain"] == "contrat"


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


def test_chronologie_ligne_hors_contrat_500_atomique(monkeypatch):
    """Une ligne hors-contrat (`type_ligne` hors de l'union discriminée) fait un **500 propre**
    *avant* tout octet — pas un 200 tronqué en cours de flux (validate-then-stream, #427)."""
    frise_cassee = pl.DataFrame(
        [
            {
                "type_ligne": "bidon",  # hors union evenement|releve|periode_energie
                "date": datetime(2024, 1, 5, tzinfo=PARIS),
                "pdl": "PDL_X",
                "ref_situation_contractuelle": "REF_1",
            }
        ],
        infer_schema_length=None,
    )
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    monkeypatch.setattr(
        "electricore.api.routers.chronologie.chronologie_point_ou_contrat",
        lambda pdl=None, rsc=None: frise_cassee,
    )
    try:
        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/facturation/chronologie", params={"pdl": "PDL_X"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 500
    # Le flux n'a pas commencé : pas de données de frise dans le corps.
    assert not response.headers["content-type"].startswith("application/jsonl")
    assert "type_ligne" not in response.text


def test_chronologie_openapi_annonce_le_flux_jsonl():
    """Découvrabilité (#455) : OpenAPI documente la 200 en `application/jsonl` (NDJSON), pas
    en `application/json` implicite — sinon Swagger tente un parse JSON unique (« [object Blob] »).
    """
    reponse_200 = app.openapi()["paths"]["/facturation/chronologie"]["get"]["responses"]["200"]
    assert list(reponse_200["content"].keys()) == ["application/jsonl"]
    assert "ligne par ligne" in reponse_200["description"]


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
    typees = [valider_ligne_chronologie(ligne) for ligne in _lignes(response)]
    evenements = [ligne for ligne in typees if isinstance(ligne, LigneEvenement)]
    rsc = {ligne.ref_situation_contractuelle for ligne in evenements}
    assert rsc == {"REF_1", "REF_2"}
    # Les charnières (RES sortie / CFNE entrée) sont visibles comme faits.
    evts = {ligne.evenement_declencheur for ligne in evenements}
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
    typees = [valider_ligne_chronologie(ligne) for ligne in _lignes(response)]
    # MDPRM (hors-comptage) figure dans la frise.
    evts = {ligne.evenement_declencheur for ligne in typees if isinstance(ligne, LigneEvenement)}
    assert "MDPRM" in evts
    # La période d'énergie porte le verdict communication CORRECT (communicante).
    periode = next(ligne for ligne in typees if isinstance(ligne, LignePeriodeEnergie))
    assert periode.statut_communication == "communicante"
    assert periode.qualite == "réelle"
