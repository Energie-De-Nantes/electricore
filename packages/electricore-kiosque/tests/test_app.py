"""Tests de la couture ASGI du Kiosque.

`assembler()` prend catalogue + sélection en paramètres (couture testable, pas de lecture
d'env ici) : les notebooks sont des fixtures factices, il n'y a ni navigateur ni vrai
serveur — juste l'app FastAPI assemblée, exercée par `TestClient`.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
from electricore_kiosque.app import NomAppInconnu, assembler
from starlette.testclient import TestClient

_NOTEBOOK_FACTICE = textwrap.dedent(
    """
    import marimo

    __generated_with = "0.23.0"
    app = marimo.App()

    with app.setup:
        import marimo as mo

    @app.cell
    def _():
        mo.md("bonjour")
        return

    if __name__ == "__main__":
        app.run()
    """
)


def _ecrire_notebook(chemin: Path) -> str:
    chemin.write_text(_NOTEBOOK_FACTICE)
    return str(chemin)


@pytest.fixture
def catalogue(tmp_path: Path) -> dict[str, str]:
    return {
        "exports": _ecrire_notebook(tmp_path / "exports.py"),
        "facturation": _ecrire_notebook(tmp_path / "facturation.py"),
    }


@pytest.fixture
def accueil(tmp_path: Path) -> str:
    return _ecrire_notebook(tmp_path / "accueil.py")


def test_route_active_presente(catalogue: dict[str, str], accueil: str) -> None:
    app = assembler(catalogue, ["exports"], accueil=accueil)
    with TestClient(app) as client:
        reponse = client.get("/exports")
    assert reponse.status_code == 200


def test_route_non_listee_absente(catalogue: dict[str, str], accueil: str) -> None:
    """Un notebook du catalogue non listé dans la sélection n'est pas monté."""
    app = assembler(catalogue, ["exports"], accueil=accueil)
    with TestClient(app) as client:
        reponse = client.get("/facturation")
    assert reponse.status_code == 404


def test_accueil_monte_a_la_racine(catalogue: dict[str, str], accueil: str) -> None:
    app = assembler(catalogue, [], accueil=accueil)
    with TestClient(app) as client:
        reponse = client.get("/")
    assert reponse.status_code == 200


def test_fail_fast_nom_hors_catalogue(catalogue: dict[str, str], accueil: str) -> None:
    """Une faute de frappe dans KIOSQUE__APPS est détectée au démarrage, pas en prod."""
    with pytest.raises(NomAppInconnu, match="typo_exports"):
        assembler(catalogue, ["typo_exports"], accueil=accueil)


def test_fail_fast_message_liste_le_catalogue_disponible(catalogue: dict[str, str], accueil: str) -> None:
    with pytest.raises(NomAppInconnu, match="exports"):
        assembler(catalogue, ["inconnu"], accueil=accueil)
