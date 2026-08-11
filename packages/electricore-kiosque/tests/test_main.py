"""Fail-fast au démarrage : nom hors catalogue → message explicite + sortie non-nulle."""

from __future__ import annotations

import pytest
from electricore_kiosque.__main__ import construire_app_ou_sortir


def test_construire_app_ou_sortir_leve_sysexit_sur_nom_inconnu(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exc_info:
        construire_app_ou_sortir(catalogue={"exports": "quelque_part.py"}, actifs=["typo"], accueil="accueil.py")

    assert exc_info.value.code == 1
    erreur = capsys.readouterr().err
    assert "typo" in erreur
    assert "exports" in erreur
