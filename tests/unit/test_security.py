"""Attribution des clés API au consommateur (security.py, ADR-0046 §4)."""

import pytest

from electricore.api import security
from electricore.config import runtime


@pytest.fixture(autouse=True)
def _env_isole(monkeypatch):
    """Neutralise le .env du dépôt + vide le cache des accessors runtime."""
    monkeypatch.setattr(runtime, "FICHIER_ENV", None)
    runtime.vider_cache()
    yield
    runtime.vider_cache()


def test_api_key_info_attribue_le_consommateur(monkeypatch):
    """APIKeyInfo expose le consommateur (label) d'une clé valide du trousseau."""
    monkeypatch.setenv("API__TROUSSEAU__librewatt__KEY", "k-librewatt-xxxxxxxxxxxxxxxxxxxxxxx")
    info = security.APIKeyInfo.from_key("k-librewatt-xxxxxxxxxxxxxxxxxxxxxxx")
    assert info.is_valid
    assert info.consumer == "librewatt"


def test_api_key_info_consommateur_none_si_invalide(monkeypatch):
    """Clé inconnue : invalide et sans consommateur attribué."""
    monkeypatch.setenv("API__TROUSSEAU__librewatt__KEY", "k-librewatt-xxxxxxxxxxxxxxxxxxxxxxx")
    info = security.APIKeyInfo.from_key("cle-inconnue")
    assert not info.is_valid
    assert info.consumer is None
