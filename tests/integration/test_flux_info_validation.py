"""Tests d'intégration : `GET /flux/{table_name}/info` valide le nom **en amont** (#428).

Le nom de table est validé contre les tables physiquement présentes (`list_tables()` — le
même ensemble que les routes data servent et que le menu bot affiche) *avant* toute requête
de métadonnées. Un nom inconnu (ou porteur de quote) renvoie un 404 propre qui nomme les
tables disponibles — levé **en amont**, pas en exécutant du SQL puis en attrapant l'erreur.
Ferme le seul chemin SQL f-string piloté par l'utilisateur.
"""

import duckdb
import pytest
from fastapi.testclient import TestClient

from electricore.api.main import app
from electricore.api.security import get_current_api_key
from electricore.api.services import duckdb_service


@pytest.fixture
def client():
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()


@pytest.fixture
def base_flux(tmp_path, monkeypatch):
    """Base DuckDB réelle avec un flux `c15` — `list_tables()` renvoie réellement `['c15']`."""
    base = tmp_path / "flux.duckdb"
    conn = duckdb.connect(str(base))
    conn.execute("CREATE SCHEMA flux_enedis")
    conn.execute("CREATE TABLE flux_enedis.flux_c15 (pdl VARCHAR, date_evenement TIMESTAMP)")
    conn.execute("INSERT INTO flux_enedis.flux_c15 VALUES ('pdl1', '2026-06-01 00:00:00')")
    conn.close()
    monkeypatch.setenv("DUCKDB_PATH", str(base))
    return base


@pytest.fixture
def espion_get_table_info(monkeypatch):
    """Enregistre les appels à `get_table_info` pour prouver le court-circuit en amont."""
    appels: list = []
    reel = duckdb_service.get_table_info

    def _espion(*args, **kwargs):
        appels.append((args, kwargs))
        return reel(*args, **kwargs)

    monkeypatch.setattr(duckdb_service, "get_table_info", _espion)
    return appels


def test_nom_inconnu_valide_en_amont_sans_lancer_de_sql(client, base_flux, espion_get_table_info):
    """Tracer bullet : un flux inconnu → 404 nommant les tables, **sans** lire les métadonnées
    (validé en amont, pas en exécutant du SQL puis en attrapant l'erreur)."""
    response = client.get("/flux/totalement_inexistant/info")

    assert response.status_code == 404
    assert "c15" in response.json()["detail"]
    # Aucune requête de métadonnées n'a été lancée : la validation a court-circuité.
    assert espion_get_table_info == []


def test_nom_porteur_de_quote_404_sans_toucher_le_sql(client, base_flux, espion_get_table_info):
    """Un nom porteur de quote (`c15'--`) → 404 propre **avant** tout SQL : il n'atteint
    jamais le moteur (injection close au bord), pas un 500/erreur SQL attrapée."""
    response = client.get("/flux/c15'--/info")

    assert response.status_code == 404
    assert espion_get_table_info == []


def test_nom_valide_200_avec_metadonnees(client, base_flux):
    """Un nom présent (`c15`) → 200 avec les métadonnées de la table."""
    response = client.get("/flux/c15/info")

    assert response.status_code == 200
    payload = response.json()
    assert payload["table"] == "flux_c15"
    assert payload["count"] == 1
