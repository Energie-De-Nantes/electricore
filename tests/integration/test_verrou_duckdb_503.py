"""Tests d'intégration : verrou DuckDB pendant l'ingestion → 503 explicite (issue #171).

Pendant qu'un job d'ingestion écrit dans DuckDB (mono-writer), les lectures échouent
après le retry court. L'API doit alors répondre 503 avec un détail actionnable
(« ingestion en cours, réessaie ») plutôt que l'exception brute — et surtout ne
pas maquiller le verrou en 404 ou en erreur générique.
"""

import pytest
from fastapi.testclient import TestClient

from electricore.api.main import app
from electricore.api.security import get_current_api_key
from electricore.core.loaders.duckdb import DuckDBLockError


@pytest.fixture
def client():
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()


@pytest.fixture
def base_verrouillee(monkeypatch):
    """Simule le verrou writer : toute lecture flux lève DuckDBLockError."""

    def _verrou(*args, **kwargs):
        raise DuckDBLockError("IO Error: Conflicting lock is held")

    monkeypatch.setattr("electricore.api.routers.flux._load_flux_df", _verrou)


class TestVerrouPendantIngestion:
    """Le verrou DuckDB se présente comme une attente, pas comme une panne."""

    def test_export_xlsx_repond_503_avec_detail_ingestion(self, client, base_verrouillee):
        response = client.get("/flux/r151.xlsx")

        assert response.status_code == 503
        assert "ingestion en cours" in response.json()["detail"].lower()

    def test_verrou_503_porte_header_x_error_kind_ingestion_lock(self, client, base_verrouillee):
        """Le 503 verrou porte `X-Error-Kind: ingestion-lock` (#424) : signal
        distinctif qui permet au client de mapper ce seul cas en IngestionEnCours,
        sans confondre avec un 503 générique."""
        response = client.get("/flux/r151.xlsx")

        assert response.status_code == 503
        assert response.headers.get("x-error-kind") == "ingestion-lock"

    def test_flux_json_repond_503_avec_detail_ingestion(self, client, base_verrouillee):
        response = client.get("/flux/r151")

        assert response.status_code == 503
        assert "ingestion en cours" in response.json()["detail"].lower()

    def test_info_repond_503_pas_un_faux_404(self, client, monkeypatch):
        """Le verrou ne doit pas être maquillé en « Table non trouvée ».

        Depuis #428, la 1re lecture du chemin `/info` est `list_tables` (validation du nom
        en amont) ; sous verrou, c'est elle qui lève `DuckDBLockError` → 503 via le handler
        d'app, sans dégénérer en faux 404.
        """

        def _verrou(*args, **kwargs):
            raise DuckDBLockError("IO Error: Conflicting lock is held")

        monkeypatch.setattr("electricore.api.services.duckdb_service.list_tables", _verrou)
        response = client.get("/flux/r151/info")

        assert response.status_code == 503
        assert "ingestion en cours" in response.json()["detail"].lower()


class TestAutresErreursRestentDistinctes:
    """Pas de faux « ingestion en cours » : seules les erreurs de verrou y ont droit."""

    def test_erreur_generique_garde_son_message(self, client, monkeypatch):
        def _panne(*args, **kwargs):
            raise RuntimeError("colonne inattendue dans le parquet")

        monkeypatch.setattr("electricore.api.routers.flux._load_flux_df", _panne)
        response = client.get("/flux/r151.xlsx")

        detail = response.json()["detail"]
        assert "ingestion en cours" not in detail.lower()
        assert "colonne inattendue" in detail
