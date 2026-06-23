"""Bout-en-bout `ElectricoreClient.meta_periodes` ↔ endpoint JSONL (#407).

Vérifie la boucle complète client → endpoint → client : le client (polars-free)
streame des `PeriodeMeta` typés depuis la vraie app FastAPI (via `TestClient`),
relevés imbriqués compris, en lisant la version de contrat dans les en-têtes.
Le service `meta_periodes` est court-circuité par un DataFrame déterministe.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl
import pytest
from electricore_client import ElectricoreClient, PeriodeMeta
from fastapi.testclient import TestClient

from electricore.api.main import app
from electricore.api.security import get_current_api_key

PARIS = ZoneInfo("Europe/Paris")


@pytest.fixture
def df_meta() -> pl.DataFrame:
    df = pl.DataFrame(
        {
            "ref_situation_contractuelle": ["RSC-1", "RSC-2"],
            "pdl": ["12345678901234", "12345678905678"],
            "mois_annee": ["2026-05", "2026-05"],
            "debut": [datetime(2026, 5, 1, tzinfo=PARIS), datetime(2026, 5, 1, tzinfo=PARIS)],
            "fin": [datetime(2026, 6, 1, tzinfo=PARIS), datetime(2026, 6, 1, tzinfo=PARIS)],
            "nb_jours": [31, 31],
            "puissance_moyenne_kva": [6.0, 9.0],
            "formule_tarifaire_acheminement": ["BTINFCUST", "BTINFCUST"],
            "energie_base_kwh": [None, 420.0],
            "energie_hp_kwh": [312.4, None],
            "energie_hc_kwh": [145.2, None],
            "turpe_fixe_eur": [9.13, 12.0],
            "turpe_variable_eur": [18.4, 22.0],
            "cta_eur": [1.97, 2.59],
            "taux_accise_eur_mwh": [22.5, 22.5],
            "has_changement": [False, False],
            "qualite": ["réelle", "incalculable"],
            "statut_communication": ["communicante", "non_communicante"],
            "source_hash": ["deadbeefcafe0001", "deadbeefcafe0002"],
        }
    )
    releves = [
        [
            {
                "releve_id": "a1b2c3d4e5f60718",
                "date_releve": "2026-05-01T00:00:00+02:00",
                "nature_index": "réel",
                "origine_releve": "périodique",
                "index_hp_kwh": 1000,
                "index_hc_kwh": 500,
            }
        ],
        [],
    ]
    return df.with_columns(pl.Series("releves_utilises", releves, dtype=pl.Object))


def test_client_streame_des_periode_meta(monkeypatch, df_meta):
    """Le client streame des `PeriodeMeta` typés depuis l'endpoint JSONL réel."""
    monkeypatch.setattr(
        "electricore.api.routers.meta_periodes.meta_periodes",
        lambda mois=None, rsc=None: ("2026-05-01", df_meta),
    )
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    try:
        client = ElectricoreClient(url="http://testserver", api_key="key", http_client=TestClient(app))
        with client.meta_periodes(mois="2026-05-01") as stream:
            assert stream.contract_version == 3
            assert stream.mois == "2026-05-01"
            periodes = stream.collect()
    finally:
        app.dependency_overrides.clear()

    assert all(isinstance(p, PeriodeMeta) for p in periodes)
    assert [p.ref_situation_contractuelle for p in periodes] == ["RSC-1", "RSC-2"]
    assert periodes[0].releves_utilises[0].index_hp_kwh == 1000
    assert periodes[1].releves_utilises == []
