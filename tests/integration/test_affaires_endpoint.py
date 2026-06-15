"""Endpoint cockpit des affaires SGE ouvertes (`GET /perimetre/affaires`, #276).

Suivi opérationnel read-only : la vue « affaires non soldées + ancienneté » se calcule
à la lecture (rollup `affaires_ouvertes`). Comme les autres endpoints data, on
monkeypatche le seam de chargement `_load_affaires_df` pour court-circuiter l'IO DuckDB.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl
import pytest
from fastapi.testclient import TestClient

from electricore.api.main import app
from electricore.api.security import get_current_api_key

PARIS = ZoneInfo("Europe/Paris")


@pytest.fixture
def client():
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()


def _jalons() -> pl.DataFrame:
    """Jalons bruts (grain flux_affaires) : une affaire CFN en cours, une AME en cours,
    une CFN terminée."""

    def row(aid, num, etat, statut, prestation):
        return {
            "affaire_id": aid,
            "origine": "initiee",
            "prestation": prestation,
            "prestation_libelle": prestation,
            "statut": statut,
            "pdl": "99000000000017",
            "segment": "C5",
            "jalon_num": num,
            "affaire_jalon_id": f"{aid}#{num}",
            "jalon_date_heure": datetime(2024, 11, 20 + num, 9, tzinfo=PARIS),
            "affaire_etat": etat,
            "affaire_etat_libelle": etat,
        }

    return pl.DataFrame(
        [
            row("CFN1", 1, "DMTR", "COURS", "CFN"),
            row("CFN1", 2, "INPL", "COURS", "CFN"),
            row("AME1", 1, "DMTR", "COURS", "AME"),
            row("OLD1", 1, "CPRE", "TERMN", "MES"),
        ]
    )


def test_perimetre_affaires_liste_les_non_soldees_hors_ame(client, monkeypatch):
    """`GET /perimetre/affaires` rend les affaires en cours (hors AME) avec dernier état."""
    monkeypatch.setattr("electricore.api.routers.affaires._load_affaires_df", lambda **kw: _jalons())

    response = client.get("/perimetre/affaires")

    assert response.status_code == 200
    affaires = response.json()["affaires"]
    assert [a["affaire_id"] for a in affaires] == ["CFN1"]  # AME exclu, TERMN exclu
    (cfn,) = affaires
    assert cfn["dernier_etat"] == "INPL"
    assert isinstance(cfn["anciennete_jours"], int)


def test_perimetre_affaires_inclure_ame(client, monkeypatch):
    """`?inclure_ame=true` réintègre les souscriptions de flux (AME)."""
    monkeypatch.setattr("electricore.api.routers.affaires._load_affaires_df", lambda **kw: _jalons())

    response = client.get("/perimetre/affaires", params={"inclure_ame": "true"})

    assert response.status_code == 200
    ids = {a["affaire_id"] for a in response.json()["affaires"]}
    assert ids == {"CFN1", "AME1"}


def test_perimetre_affaires_exige_la_cle_api():
    """Sans `X-API-Key`, l'endpoint est refusé (401)."""
    assert TestClient(app).get("/perimetre/affaires").status_code == 401
