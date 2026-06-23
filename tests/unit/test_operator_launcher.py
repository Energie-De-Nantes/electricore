"""Tests du lanceur opérateur `electricore-notebooks` (pont transitoire, #414).

Couvre deux seams :
- validation de l'environnement (creds Odoo + ELECTRICORE_API_URL/KEY) ;
- construction de l'app ASGI marimo montant le dossier de notebooks embarqué.

Le smoke navigateur/serveur est une vérif MANUELLE (non automatisée).
"""

from pathlib import Path

import pytest

from electricore.config import runtime
from electricore.operator_launcher import (
    construire_app,
    dossier_notebooks,
    valider_environnement,
)


@pytest.fixture(autouse=True)
def _isoler_cache_config():
    """Le registre runtime cache les domaines : repartir propre à chaque test."""
    runtime.vider_cache()
    yield
    runtime.vider_cache()


# Jeu d'env minimal complet : creds Odoo TEST + clés API opérateur.
_ENV_COMPLET = {
    "ODOO_ENV": "test",
    "ODOO_TEST_URL": "https://test.odoo.example",
    "ODOO_TEST_DB": "base_test",
    "ODOO_TEST_USERNAME": "operateur@example.com",
    "ODOO_TEST_PASSWORD": "secret",
    "ELECTRICORE_API_URL": "https://electricore.localhost",
    "ELECTRICORE_API_KEY": "cle-api-operateur",
}


def _poser_env(monkeypatch, env: dict):
    """Pose exactement `env`, efface toute autre var pertinente du process."""
    for var in (*_ENV_COMPLET, "API_KEY", "API_KEYS"):
        monkeypatch.delenv(var, raising=False)
    for cle, valeur in env.items():
        monkeypatch.setenv(cle, valeur)
    # Couper le fichier .env du dépôt pour que seul l'env du test compte.
    monkeypatch.setattr(runtime, "FICHIER_ENV", None)


def test_environnement_complet_ne_leve_pas(monkeypatch):
    _poser_env(monkeypatch, _ENV_COMPLET)
    # Ne doit rien lever quand tout est présent.
    valider_environnement()


@pytest.mark.parametrize(
    "var_manquante",
    [
        "ODOO_TEST_URL",
        "ODOO_TEST_PASSWORD",
        "ELECTRICORE_API_URL",
        "ELECTRICORE_API_KEY",
    ],
)
def test_var_manquante_leve_message_actionnable(monkeypatch, var_manquante):
    env = {k: v for k, v in _ENV_COMPLET.items() if k != var_manquante}
    _poser_env(monkeypatch, env)

    with pytest.raises(SystemExit) as exc:
        valider_environnement()

    # Sortie non-nulle (exigence #414).
    assert exc.value.code != 0


def test_message_nomme_la_variable_et_son_role(monkeypatch, capsys):
    """Le message doit nommer la variable ET expliquer à quoi elle sert."""
    env = {k: v for k, v in _ENV_COMPLET.items() if k != "ELECTRICORE_API_KEY"}
    _poser_env(monkeypatch, env)

    with pytest.raises(SystemExit):
        valider_environnement()

    captured = capsys.readouterr()
    sortie = captured.out + captured.err
    assert "ELECTRICORE_API_KEY" in sortie


# --- Seam : résolution du dossier embarqué + construction de l'app ASGI ---


def test_dossier_notebooks_contient_les_deux_notebooks():
    """Le dossier résolu (embarqué ou repli dev) sert les 2 notebooks opérateur."""
    dossier = dossier_notebooks()
    assert dossier.is_dir()
    noms = {p.name for p in dossier.glob("*.py")}
    assert "facturation.py" in noms
    assert "injection_rsc.py" in noms


def test_construire_app_monte_le_dossier_embarque(tmp_path: Path):
    """L'app ASGI se construit et monte le dossier fourni (sans démarrer de serveur)."""
    pytest.importorskip("marimo")
    from marimo._server.asgi import DynamicDirectoryMiddleware

    # Dossier factice : on vérifie le montage, pas l'exécution d'un notebook.
    (tmp_path / "facturation.py").write_text("import marimo\napp = marimo.App()\n")

    app = construire_app(tmp_path, prefixe="/apps")

    # with_dynamic_directory → l'app finale est le middleware de dossier dynamique,
    # monté sur le dossier passé sous /apps. On ne démarre aucun uvicorn.
    assert isinstance(app, DynamicDirectoryMiddleware)
    assert Path(app.directory) == tmp_path
    assert app.base_path == "/apps"
    assert callable(app)
