"""Tests d'intégration de l'endpoint `GET /health` — identité d'instance (ADR-0015)."""

from fastapi.testclient import TestClient

from electricore.api.main import app


def test_health_expose_instance_slug_when_set(monkeypatch):
    monkeypatch.setenv("INSTANCE_SLUG", "edn")
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["instance"] == "edn"


def test_health_expose_empty_instance_when_unset(monkeypatch):
    monkeypatch.setenv("INSTANCE_SLUG", "")
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["instance"] == ""
