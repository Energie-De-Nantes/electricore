"""Lancement local du Kiosque : `python -m electricore_kiosque` / `electricore-kiosque`.

Config par variables d'environnement (`KIOSQUE__APPS`, `KIOSQUE__TITRE`, voir `config.py`) —
une instance par entité, même image (ADR-0057).
"""

from __future__ import annotations

import sys
from importlib import resources

from fastapi import FastAPI

from electricore_kiosque.app import NomAppInconnu, assembler
from electricore_kiosque.catalogue import CATALOGUE
from electricore_kiosque.config import apps_actives

_HOTE = "127.0.0.1"
_PORT = 8765


def _accueil() -> str:
    with resources.as_file(resources.files("electricore_kiosque").joinpath("accueil.py")) as chemin:
        return str(chemin)


def construire_app_ou_sortir(*, catalogue: dict[str, str], actifs: list[str], accueil: str) -> FastAPI:
    """Assemble l'app, ou sort en erreur (message explicite) si `actifs` est mal renseigné.

    Fail-fast voulu (ADR-0057) : une faute de frappe dans `KIOSQUE__APPS` doit planter le
    démarrage du service, pas silencieusement servir un Kiosque incomplet en production.
    """
    try:
        return assembler(catalogue, actifs, accueil=accueil)
    except NomAppInconnu as exc:
        print(f"Kiosque : configuration invalide — {exc}", file=sys.stderr)
        sys.exit(1)


def main() -> None:
    app = construire_app_ou_sortir(catalogue=CATALOGUE, actifs=apps_actives(), accueil=_accueil())

    import uvicorn

    uvicorn.run(app, host=_HOTE, port=_PORT)


if __name__ == "__main__":
    main()
