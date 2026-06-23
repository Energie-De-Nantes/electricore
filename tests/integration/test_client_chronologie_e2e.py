"""Bout-en-bout `ElectricoreClient.chronologie` ↔ endpoint JSONL (#408).

Vérifie la boucle complète client → endpoint → client : le client (polars-free)
streame une frise dont chaque ligne se résout au bon sous-type via l'union
discriminée, depuis la vraie app FastAPI (via `TestClient`). Exerce les **trois**
sous-types et les deux grains (`pdl`, `rsc`), plus le rejet XOR côté client.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl
import pytest
from electricore_client import ElectricoreClient
from electricore_client.models import LigneEvenement, LignePeriodeEnergie, LigneReleve
from fastapi.testclient import TestClient

from electricore.api.main import app
from electricore.api.security import get_current_api_key

PARIS = ZoneInfo("Europe/Paris")


@pytest.fixture
def frise() -> pl.DataFrame:
    """Frise avec les trois sous-types (événement, relevé, période d'énergie)."""
    return pl.DataFrame(
        [
            {
                "type_ligne": "evenement",
                "date": datetime(2024, 1, 5, tzinfo=PARIS),
                "pdl": "PDL_X",
                "ref_situation_contractuelle": "REF_1",
                "source": "flux_C15",
                "evenement_declencheur": "MES",
                "impacte_abonnement": True,
            },
            {
                "type_ligne": "releve",
                "date": datetime(2024, 1, 5, tzinfo=PARIS),
                "pdl": "PDL_X",
                "ref_situation_contractuelle": "REF_1",
                "source": "flux_R151",
                "releve_id": "a1b2c3d4e5f60718",
                "nature_index": "réel",
                "origine_releve": "périodique",
                "ordre_index": 0,
                "index_hp_kwh": 1000,
                "index_hc_kwh": 500,
            },
            {
                "type_ligne": "periode_energie",
                "date": datetime(2024, 1, 5, tzinfo=PARIS),
                "pdl": "PDL_X",
                "ref_situation_contractuelle": "REF_1",
                "debut": datetime(2024, 1, 5, tzinfo=PARIS),
                "fin": datetime(2024, 2, 1, tzinfo=PARIS),
                "nb_jours": 27,
                "qualite": "réelle",
                "statut_communication": "communicante",
                "energie_base_kwh": 50.0,
            },
        ],
        infer_schema_length=None,
    )


def test_client_streame_les_trois_sous_types(monkeypatch, frise):
    """Le client résout chaque ligne au bon sous-type via l'union discriminée."""
    monkeypatch.setattr(
        "electricore.api.routers.chronologie.chronologie_point_ou_contrat",
        lambda pdl=None, rsc=None: frise,
    )
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    try:
        client = ElectricoreClient(url="http://testserver", api_key="key", http_client=TestClient(app))
        with client.chronologie(pdl="PDL_X") as stream:
            assert stream.grain == "point"
            lignes = stream.collect()
    finally:
        app.dependency_overrides.clear()

    assert isinstance(lignes[0], LigneEvenement)
    assert isinstance(lignes[1], LigneReleve)
    assert isinstance(lignes[2], LignePeriodeEnergie)
    assert lignes[1].index_hp_kwh == 1000
    assert lignes[2].energie_base_kwh == 50.0


def test_client_grain_contrat(monkeypatch, frise):
    """Le grain contrat (`rsc`) round-trip avec `grain=contrat`."""
    monkeypatch.setattr(
        "electricore.api.routers.chronologie.chronologie_point_ou_contrat",
        lambda pdl=None, rsc=None: frise,
    )
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    try:
        client = ElectricoreClient(url="http://testserver", api_key="key", http_client=TestClient(app))
        with client.chronologie(rsc="REF_1") as stream:
            assert stream.grain == "contrat"
            lignes = stream.collect()
    finally:
        app.dependency_overrides.clear()
    assert len(lignes) == 3


def test_client_rejette_les_deux_grains_avant_requete():
    """pdl ET rsc → ValueError côté client, sans toucher l'app."""
    client = ElectricoreClient(url="http://testserver", api_key="key", http_client=TestClient(app))
    with pytest.raises(ValueError):
        client.chronologie(pdl="PDL_X", rsc="REF_1")
