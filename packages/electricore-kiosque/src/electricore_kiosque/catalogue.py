"""Catalogue des notebooks marimo disponibles au Kiosque.

Premier peuplement (#705) : `exports` — consultation + téléchargement CSV de la
facturation mensuelle via `electricore-client` (voir `helpers.py`). `assembler()`
(`app.py`) est la couture qui monte la sélection active (`KIOSQUE__APPS`) parmi
ces entrées ; ajouter une app future = ajouter une entrée ici.
"""

from __future__ import annotations

from importlib import resources


def _chemin(nom_fichier: str) -> str:
    return str(resources.files("electricore_kiosque").joinpath(nom_fichier))


CATALOGUE: dict[str, str] = {
    "exports": _chemin("exports.py"),
}
