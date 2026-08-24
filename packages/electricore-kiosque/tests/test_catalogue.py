"""Le catalogue réel entre en jeu (#705) : `exports` existe et se monte.

Complète `test_app.py` (couture ASGI générique, catalogue factice) : ici on
exerce le VRAI catalogue du paquet, pour prouver que `KIOSQUE__APPS=exports`
donne effectivement un Kiosque avec une app montée — et rien de plus si
`exports` n'est pas listé.
"""

from __future__ import annotations

from importlib import resources
from pathlib import Path

from electricore_kiosque.app import assembler
from electricore_kiosque.catalogue import CATALOGUE
from starlette.testclient import TestClient


def _accueil() -> str:
    return str(resources.files("electricore_kiosque").joinpath("accueil.py"))


def test_exports_est_au_catalogue() -> None:
    assert "exports" in CATALOGUE
    assert Path(CATALOGUE["exports"]).is_file()


def test_exports_se_monte_quand_liste() -> None:
    app = assembler(CATALOGUE, ["exports"], accueil=_accueil())
    with TestClient(app) as client:
        reponse = client.get("/exports")
    assert reponse.status_code == 200


def test_exports_absent_quand_non_liste() -> None:
    app = assembler(CATALOGUE, [], accueil=_accueil())
    with TestClient(app) as client:
        reponse = client.get("/exports")
    assert reponse.status_code == 404
