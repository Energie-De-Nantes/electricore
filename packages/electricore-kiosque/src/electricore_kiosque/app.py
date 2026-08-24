"""Assemblage ASGI du Kiosque : catalogue de notebooks marimo → app ASGI unique.

Le Kiosque sert plusieurs notebooks marimo en mode `run` (lecture seule, ADR-0057) sous un
seul process : un accueil monté à la racine (`path=""`), chaque app active du catalogue
montée sous son propre chemin (`path=f"/{nom}"`). `assembler()` prend catalogue + sélection
en paramètres — c'est la couture testable, sans lecture d'env (voir `config.py` pour ça).
"""

from __future__ import annotations

import marimo
from starlette.types import ASGIApp


class NomAppInconnu(ValueError):
    """Un nom de la sélection active (`KIOSQUE__APPS`) n'existe pas dans le catalogue.

    Fail-fast voulu (ADR-0057) : une faute de frappe se détecte au démarrage du service,
    pas en production face à l'utilisateur·ice.
    """


def assembler(catalogue: dict[str, str], actifs: list[str], *, accueil: str) -> ASGIApp:
    """Construit l'app ASGI du Kiosque à partir d'un catalogue et d'une sélection active.

    `catalogue` : nom → chemin de notebook marimo disponible.
    `actifs` : sélection à monter (valeur de `KIOSQUE__APPS`) — chaque nom DOIT exister
    dans `catalogue`, sinon `NomAppInconnu`.
    `accueil` : chemin du notebook marimo monté à la racine (`/`).
    """
    inconnus = [nom for nom in actifs if nom not in catalogue]
    if inconnus:
        disponibles = ", ".join(sorted(catalogue)) or "aucune"
        raise NomAppInconnu(
            f"KIOSQUE__APPS référence des apps inconnues du catalogue : "
            f"{', '.join(inconnus)} (disponibles : {disponibles})"
        )

    server = marimo.create_asgi_app()
    for nom in actifs:
        server = server.with_app(path=f"/{nom}", root=catalogue[nom])
    server = server.with_app(path="", root=accueil)

    return server.build()
