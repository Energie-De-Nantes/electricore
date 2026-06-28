"""Tests d'intégration de `GET /provision/estimation?pdl=…` (estimation de provision, #487).

Estimation **cœur-pure** depuis flux_r67 (cold-start, ADR-0048) : provision d'énergie d'un
lissé en kWh (annuel par cadran + provision mensuelle `/12` plate) + couverture / profondeur /
qualité / signal alertable. Le seam de build (`_estimer`) est monkeypatché : on vérifie le
câblage transport + enveloppe (`contract_version`, en-tête, sérialisation), pas le cœur
(couvert par `tests/unit/test_pipeline_provision.py`). Auth surchargée comme pour rsc.
"""

import datetime as dt

import duckdb
import polars as pl
import pytest
from fastapi.testclient import TestClient

from electricore.api.main import app
from electricore.api.security import get_current_api_key
from electricore.core.builds.rapport_provision import RapportProvision


@pytest.fixture
def client():
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()


def _rapport(pdl: str, *, trouve: bool) -> RapportProvision:
    if not trouve:
        return RapportProvision(pdl=pdl, as_of=dt.date(2026, 6, 27), estimation=pl.DataFrame())
    estimation = pl.DataFrame(
        {
            "pdl": [pdl],
            "energie_base_kwh": [1200.0],
            "energie_hp_kwh": [None],
            "energie_hc_kwh": [None],
            "energie_base_mensuel_kwh": [100.0],
            "energie_hp_mensuel_kwh": [None],
            "energie_hc_mensuel_kwh": [None],
            "couverture_debut": [dt.date(2025, 6, 1)],
            "couverture_fin": [dt.date(2026, 6, 1)],
            "couverture_mois": [12.0],
            "couverture_suffisante": [True],
            "profondeur_cadran": ["base"],
            "qualite": ["réelle"],
            "presence_regularisation": [False],
            "signal_alertable": [False],
        }
    )
    return RapportProvision(pdl=pdl, as_of=dt.date(2026, 6, 27), estimation=estimation)


def test_estimation_renvoie_enveloppe_kwh(client, monkeypatch):
    """Tracer : un PDL trouvé → enveloppe `{contract_version, …, estimation}` en kWh."""
    monkeypatch.setattr(
        "electricore.api.routers.provision._estimer",
        lambda pdl: _rapport(pdl, trouve=True),
    )
    response = client.get("/provision/estimation", params={"pdl": "12345678901234"})
    assert response.status_code == 200
    assert response.headers["X-Contract-Version"] == "1"
    body = response.json()
    assert body["contract_version"] == 1
    assert body["pdl"] == "12345678901234"
    assert body["trouve"] is True
    est = body["estimation"]
    # kWh annuel + provision mensuelle /12 plate + couverture.
    assert est["energie_base_kwh"] == 1200.0
    assert est["energie_base_mensuel_kwh"] == 100.0
    assert est["couverture_debut"] == "2025-06-01"
    assert est["couverture_mois"] == 12.0
    assert est["couverture_suffisante"] is True
    # Zéro € exposé (convention de colonne € = suffixe `_eur`, ADR-0016/0027).
    assert not any(cle.endswith("_eur") or "_eur_" in cle for cle in est)


def test_flux_r67_absent_renvoie_503_actionnable(client, monkeypatch):
    """Régression : flux_r67 non matérialisé (R67 ponctuel jamais ingéré / aucune M023) → 503
    ACTIONNABLE, pas un 500 brut. Le chemin loader réel n'était pas couvert (estimateur
    monkeypatché), d'où la CatalogException non gérée remontée en prod."""

    def _table_absente(pdl):
        raise duckdb.CatalogException("Catalog Error: Table with name flux_r67 does not exist!")

    monkeypatch.setattr("electricore.api.routers.provision._estimer", _table_absente)
    response = client.get("/provision/estimation", params={"pdl": "12345678901234"})
    assert response.status_code == 503
    detail = response.json()["detail"]
    assert "flux_r67" in detail and "M023" in detail  # message qui dit quoi faire


def test_erreur_inattendue_renvoie_503_pas_500(client, monkeypatch):
    """Garde : toute erreur inattendue du seam est emballée en 503 — l'endpoint n'émet jamais
    de 500 brut (convention `binary_endpoint`/decorators.py)."""

    def _boom(pdl):
        raise RuntimeError("imprévu")

    monkeypatch.setattr("electricore.api.routers.provision._estimer", _boom)
    response = client.get("/provision/estimation", params={"pdl": "12345678901234"})
    assert response.status_code == 503


def _rapport_4_cadrans(pdl: str) -> RapportProvision:
    estimation = pl.DataFrame(
        {
            "pdl": [pdl],
            "energie_base_kwh": [600.0],
            "energie_hp_kwh": [360.0],
            "energie_hc_kwh": [240.0],
            "energie_hph_kwh": [120.0],
            "energie_hpb_kwh": [240.0],
            "energie_hch_kwh": [60.0],
            "energie_hcb_kwh": [180.0],
            "energie_base_mensuel_kwh": [50.0],
            "energie_hp_mensuel_kwh": [30.0],
            "energie_hc_mensuel_kwh": [20.0],
            "energie_hph_mensuel_kwh": [10.0],
            "energie_hpb_mensuel_kwh": [20.0],
            "energie_hch_mensuel_kwh": [5.0],
            "energie_hcb_mensuel_kwh": [15.0],
            "couverture_debut": [dt.date(2025, 1, 1)],
            "couverture_fin": [dt.date(2026, 1, 1)],
            "couverture_mois": [12.0],
            "couverture_suffisante": [True],
            "profondeur_cadran": ["4_cadrans"],
            "qualite": ["réelle"],
            "presence_regularisation": [False],
            "signal_alertable": [False],
        }
    )
    return RapportProvision(pdl=pdl, as_of=dt.date(2026, 6, 27), estimation=estimation)


def test_estimation_propage_la_profondeur_wide(client, monkeypatch):
    """#488 : la profondeur WIDE (hp/hc + 4 cadrans + déclaration) traverse jusqu'à l'API."""
    monkeypatch.setattr(
        "electricore.api.routers.provision._estimer",
        _rapport_4_cadrans,
    )
    response = client.get("/provision/estimation", params={"pdl": "P4"})
    assert response.status_code == 200
    est = response.json()["estimation"]
    assert est["profondeur_cadran"] == "4_cadrans"
    # Invariant base = hp+hc = Σ4 visible dans la sortie WIDE annuelle.
    assert est["energie_base_kwh"] == 600.0
    assert est["energie_hp_kwh"] + est["energie_hc_kwh"] == 600.0
    sigma4 = est["energie_hph_kwh"] + est["energie_hpb_kwh"] + est["energie_hch_kwh"] + est["energie_hcb_kwh"]
    assert sigma4 == 600.0
    # Provision mensuelle /12 plate par cadran.
    assert est["energie_hp_mensuel_kwh"] == 30.0


def test_estimation_propage_le_signal_alertable(client, monkeypatch):
    """#489 : qualité + couverture + signal alertable traversent jusqu'à l'API (la lib expose,
    l'aval alerte — pas de logique de notification ici)."""
    rapport = _rapport("PSIG", trouve=True)
    rapport = RapportProvision(
        pdl="PSIG",
        as_of=rapport.as_of,
        estimation=rapport.estimation.with_columns(
            pl.lit("estimée").alias("qualite"),
            pl.lit(False).alias("couverture_suffisante"),
            pl.lit(6.0).alias("couverture_mois"),
            pl.lit(True).alias("presence_regularisation"),
            pl.lit(True).alias("signal_alertable"),
        ),
    )
    monkeypatch.setattr("electricore.api.routers.provision._estimer", lambda pdl: rapport)
    est = client.get("/provision/estimation", params={"pdl": "PSIG"}).json()["estimation"]
    assert est["qualite"] == "estimée"
    assert est["couverture_suffisante"] is False
    assert est["presence_regularisation"] is True
    assert est["signal_alertable"] is True


def test_estimation_pdl_sans_r67_renvoie_trouve_false(client, monkeypatch):
    monkeypatch.setattr(
        "electricore.api.routers.provision._estimer",
        lambda pdl: _rapport(pdl, trouve=False),
    )
    response = client.get("/provision/estimation", params={"pdl": "00000000000000"})
    assert response.status_code == 200
    body = response.json()
    assert body["trouve"] is False
    assert body["estimation"] is None


def test_estimation_exige_le_parametre_pdl(client):
    """Sans `pdl` → 422 (validation FastAPI)."""
    response = client.get("/provision/estimation")
    assert response.status_code == 422


def test_estimation_refuse_sans_api_key():
    """Endpoint sécurisé : 401 sans clé (pas d'override d'auth ici)."""
    response = TestClient(app).get("/provision/estimation", params={"pdl": "1"})
    assert response.status_code == 401
