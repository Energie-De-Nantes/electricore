"""Attribution des clés API au consommateur (security.py, ADR-0046 §4)."""

import pytest

from electricore.config import runtime


@pytest.fixture(autouse=True)
def _env_isole(monkeypatch):
    """Neutralise le .env du dépôt + vide le cache des accessors runtime."""
    monkeypatch.setattr(runtime, "FICHIER_ENV", None)
    runtime.vider_cache()
    yield
    runtime.vider_cache()
