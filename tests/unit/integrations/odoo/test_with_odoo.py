"""Tests pour le décorateur `@with_odoo` (issue #67).

Le décorateur encapsule le pattern récurrent :

    with OdooReader(config=settings.get_odoo_config()) as odoo:
        return f(odoo, *args, **kwargs)

…dans `electricore/api/services/{taxes,facturation}_service.py`. Préserve la
signature publique de `f` (sans le premier arg `odoo`) pour FastAPI.
"""

from contextlib import contextmanager
from unittest.mock import MagicMock


def test_with_odoo_injects_odoo_as_first_arg(monkeypatch):
    """`@with_odoo` ouvre OdooReader et le passe en 1er à la fonction décorée."""
    from electricore.integrations.odoo.decorators import with_odoo

    fake_odoo = MagicMock(name="fake_odoo")

    @contextmanager
    def _fake_reader(config):
        yield fake_odoo

    monkeypatch.setattr("electricore.integrations.odoo.decorators.OdooReader", _fake_reader)
    # pydantic Settings interdit setattr — on patche la méthode sur la classe.
    from electricore.integrations.odoo.decorators import settings as decorator_settings

    monkeypatch.setattr(type(decorator_settings), "get_odoo_config", lambda self: {"url": "fake://"})

    received: dict = {}

    @with_odoo
    def calc(odoo, value: int) -> int:
        received["odoo"] = odoo
        received["value"] = value
        return value * 2

    result = calc(7)

    assert result == 14
    assert received["odoo"] is fake_odoo
    assert received["value"] == 7


def test_with_odoo_closes_reader_on_exception(monkeypatch):
    """`@with_odoo` ferme l'OdooReader même si la fonction décorée lève."""
    from electricore.integrations.odoo.decorators import with_odoo

    closed = {"yes": False}

    @contextmanager
    def _fake_reader(config):
        try:
            yield MagicMock(name="odoo")
        finally:
            closed["yes"] = True

    monkeypatch.setattr("electricore.integrations.odoo.decorators.OdooReader", _fake_reader)
    from electricore.integrations.odoo.decorators import settings as decorator_settings

    monkeypatch.setattr(type(decorator_settings), "get_odoo_config", lambda self: {})

    @with_odoo
    def boom(odoo):
        raise RuntimeError("planned failure")

    import pytest

    with pytest.raises(RuntimeError, match="planned failure"):
        boom()

    assert closed["yes"], "OdooReader doit être fermé même quand la fonction décorée lève"


def test_with_odoo_preserves_name_and_doc():
    """`@functools.wraps` préserve __name__ et __doc__ — FastAPI introspection."""
    from electricore.integrations.odoo.decorators import with_odoo

    @with_odoo
    def calculer_accise_detail(odoo, trimestre=None):
        """Docstring originale."""
        return None

    assert calculer_accise_detail.__name__ == "calculer_accise_detail"
    assert calculer_accise_detail.__doc__ == "Docstring originale."


def test_with_odoo_uses_settings_get_odoo_config(monkeypatch):
    """Le décorateur appelle `settings.get_odoo_config()` à chaque invocation."""
    from electricore.integrations.odoo.decorators import with_odoo

    captured_configs = []

    @contextmanager
    def _fake_reader(config):
        captured_configs.append(config)
        yield MagicMock()

    monkeypatch.setattr("electricore.integrations.odoo.decorators.OdooReader", _fake_reader)
    from electricore.integrations.odoo.decorators import settings as decorator_settings

    monkeypatch.setattr(type(decorator_settings), "get_odoo_config", lambda self: {"url": "specific://here"})

    @with_odoo
    def noop(odoo):
        return None

    noop()

    assert captured_configs == [{"url": "specific://here"}]
