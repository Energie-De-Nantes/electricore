"""Tests du lanceur opérateur `electricore-notebooks` (pont transitoire, #414).

Couvre deux seams :
- validation de l'environnement (creds Odoo + ELECTRICORE_API_URL/KEY) ;
- construction de l'app ASGI marimo montant le dossier de notebooks embarqué.

Le smoke navigateur/serveur est une vérif MANUELLE (non automatisée).
"""

import os
from pathlib import Path

import pytest

from electricore.config import runtime
from electricore.operator_launcher import (
    charger_env,
    construire_app,
    dossier_notebooks,
    url_navigateur,
    valider_environnement,
)


@pytest.fixture(autouse=True)
def _isoler_cache_config():
    """Le registre runtime cache les domaines : repartir propre à chaque test."""
    runtime.vider_cache()
    yield
    runtime.vider_cache()


# Jeu d'env minimal complet : creds Odoo (bloc unique ODOO__*) + clés API opérateur.
_ENV_COMPLET = {
    "ODOO__URL": "https://odoo.example",
    "ODOO__DB": "edn",
    "ODOO__USERNAME": "operateur@example.com",
    "ODOO__PASSWORD": "secret",
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
        "ODOO__URL",
        "ODOO__PASSWORD",
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


# --- Seam : chargement du .env dans os.environ (précédence env-système > .env) ---


def test_charger_env_charge_le_fichier_dans_environ(monkeypatch, tmp_path):
    """`charger_env()` rend les vars du .env visibles dans os.environ.

    Les notebooks et la validation lisent ELECTRICORE_API_URL/KEY via os.getenv :
    sans ce chargement, un opérateur qui remplit .env reste « manquant ».
    """
    monkeypatch.delenv("ELECTRICORE_API_URL", raising=False)
    fichier = tmp_path / ".env"
    fichier.write_text("ELECTRICORE_API_URL=https://depuis-le-fichier.example\n")
    monkeypatch.setattr(runtime, "FICHIER_ENV", fichier)

    charger_env()

    assert os.environ.get("ELECTRICORE_API_URL") == "https://depuis-le-fichier.example"


def test_charger_env_ne_surcharge_pas_l_env_systeme(monkeypatch, tmp_path):
    """L'env-système l'emporte sur le .env (précédence documentée, override=False)."""
    monkeypatch.setenv("ELECTRICORE_API_URL", "https://depuis-le-systeme.example")
    fichier = tmp_path / ".env"
    fichier.write_text("ELECTRICORE_API_URL=https://depuis-le-fichier.example\n")
    monkeypatch.setattr(runtime, "FICHIER_ENV", fichier)

    charger_env()

    assert os.environ.get("ELECTRICORE_API_URL") == "https://depuis-le-systeme.example"


def test_charger_env_sans_fichier_ne_fait_rien(monkeypatch):
    """Seam de test : FICHIER_ENV None → aucun chargement, aucune erreur."""
    monkeypatch.delenv("ELECTRICORE_API_URL", raising=False)
    monkeypatch.setattr(runtime, "FICHIER_ENV", None)

    charger_env()  # ne doit pas lever

    assert os.environ.get("ELECTRICORE_API_URL") is None


def test_charger_env_charge_aussi_le_env_du_cwd(monkeypatch, tmp_path):
    """Après `uv tool install`, le `.env` paquet tombe dans site-packages (absent) :
    charger_env() doit aussi lire le `./.env` du répertoire courant, là où l'opérateur
    dépose le `.env` livré (cf. docs/operateur-notebook.md).
    """
    monkeypatch.delenv("ELECTRICORE_API_URL", raising=False)
    # Simule la distribution : FICHIER_ENV pointe sur un .env paquet ABSENT (non None
    # → la branche de chargement s'exécute), donc seul le ./.env du CWD peut peupler.
    monkeypatch.setattr(runtime, "FICHIER_ENV", tmp_path / "site-packages" / ".env")
    (tmp_path / ".env").write_text("ELECTRICORE_API_URL=https://depuis-le-cwd.example\n")
    monkeypatch.chdir(tmp_path)

    charger_env()

    assert os.environ.get("ELECTRICORE_API_URL") == "https://depuis-le-cwd.example"


def test_charger_env_paquet_l_emporte_sur_le_cwd(monkeypatch, tmp_path):
    """Précédence : en checkout dev, le `.env` paquet l'emporte sur le `./.env` du CWD."""
    monkeypatch.delenv("ELECTRICORE_API_URL", raising=False)
    paquet = tmp_path / "depot"
    paquet.mkdir()
    (paquet / ".env").write_text("ELECTRICORE_API_URL=https://depuis-le-paquet.example\n")
    monkeypatch.setattr(runtime, "FICHIER_ENV", paquet / ".env")
    (tmp_path / ".env").write_text("ELECTRICORE_API_URL=https://depuis-le-cwd.example\n")
    monkeypatch.chdir(tmp_path)

    charger_env()

    assert os.environ.get("ELECTRICORE_API_URL") == "https://depuis-le-paquet.example"


# --- Seam : résolution du dossier embarqué + construction de l'app ASGI ---


def test_dossier_notebooks_contient_les_deux_notebooks():
    """Le dossier résolu (embarqué ou repli dev) sert les 2 notebooks opérateur."""
    dossier = dossier_notebooks()
    assert dossier.is_dir()
    noms = {p.name for p in dossier.glob("*.py")}
    assert "facturation.py" in noms
    assert "injection_rsc.py" in noms


def test_dossier_notebooks_contient_l_accueil():
    """L'accueil (page de liens) est servi comme les autres notebooks."""
    dossier = dossier_notebooks()
    noms = {p.name for p in dossier.glob("*.py")}
    assert "accueil.py" in noms


# --- Seam : URL d'ouverture du navigateur (sans démarrer de serveur) ---

_BASE = "http://127.0.0.1:2718/apps"


def test_url_navigateur_cible_l_accueil():
    """Le navigateur s'ouvre sur /apps/accueil quand l'accueil est servi.

    `DynamicDirectoryMiddleware` n'expose aucun index sous /apps : ouvrir l'accueil
    (et pas le 1er notebook trié) est le seul moyen de lister tous les notebooks.
    """
    noms = ["accueil", "facturation", "injection_rsc"]
    assert url_navigateur(_BASE, noms) == f"{_BASE}/accueil"


def test_url_navigateur_repli_sur_le_premier_sans_accueil():
    """Sans accueil (config dégradée), on retombe sur le 1er notebook trié."""
    assert url_navigateur(_BASE, ["facturation", "injection_rsc"]) == f"{_BASE}/facturation"


def test_url_navigateur_aucun_notebook():
    """Dossier vide → aucune URL à ouvrir."""
    assert url_navigateur(_BASE, []) is None


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
