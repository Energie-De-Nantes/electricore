"""Tests d'intégration de `GET /facturation/prestations` (souscriptions_odoo#37, ADR-0043).

File « à refacturer » : Odoo tire toutes les lignes F15 `unite='UNITE'` en JSONL
(une ligne = une `PrestationF15`). Le seam est la fonction de service
`prestations` référencée dans le router — l'I/O DuckDB est court-circuitée, le
chemin transport + validation-then-stream reste réel.
"""

import json

import polars as pl
from electricore_client.models import PrestationF15
from fastapi.testclient import TestClient

from electricore.api.main import app
from electricore.api.security import get_current_api_key


def _prestations_synthetiques() -> pl.DataFrame:
    """Une indemnité (`NS`, montant négatif) façonnée sur le golden `flux_f15_detail`."""
    return pl.DataFrame(
        {
            "pdl": ["99510061232830"],
            "ref_situation_contractuelle": ["833195558"],
            "id_ev": ["DCOUP_PEN"],
            "nature_ev": ["03"],
            "libelle_ev": ["Pénalité pour coupure réseau"],
            "taux_tva_applicable": ["NS"],
            "prix_unitaire": [-24.0],
            "quantite": [2.0],
            "montant_ht": [-48.0],
            "date_debut": ["2025-01-17"],
            "date_fin": ["2025-01-17"],
            "num_facture": ["3210619182009"],
            "date_facture": ["2025-02-05"],
            "reference": ["a1b2c3d4e5f60718"],
        }
    )


def _lignes(response) -> list[dict]:
    """Décode le corps JSONL en liste de dicts (une ligne = un objet)."""
    return [json.loads(ligne) for ligne in response.text.splitlines() if ligne.strip()]


def test_prestations_streame_du_jsonl(monkeypatch):
    """Tracer bullet : GET sert les prestations en JSONL (`application/x-ndjson`)."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    monkeypatch.setattr(
        "electricore.api.routers.prestations.prestations",
        lambda rsc=None: _prestations_synthetiques(),
    )
    try:
        response = TestClient(app).get("/facturation/prestations")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/x-ndjson")
    assert response.headers["X-Contract-Version"] == "1"

    lignes = _lignes(response)
    assert len(lignes) == 1
    assert lignes[0]["reference"] == "a1b2c3d4e5f60718"
    assert lignes[0]["taux_tva_applicable"] == "NS"
    assert lignes[0]["montant_ht"] == -48.0


def test_prestations_lignes_valident_le_modele(monkeypatch):
    """Chaque ligne est une `PrestationF15` valide (`model_validate` passe)."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    monkeypatch.setattr(
        "electricore.api.routers.prestations.prestations",
        lambda rsc=None: _prestations_synthetiques(),
    )
    try:
        response = TestClient(app).get("/facturation/prestations")
    finally:
        app.dependency_overrides.clear()

    prestas = [PrestationF15.model_validate(ligne) for ligne in _lignes(response)]
    assert prestas[0].reference == "a1b2c3d4e5f60718"
    assert prestas[0].date_debut == "2025-01-17"  # jour civil ISO, pas un instant
