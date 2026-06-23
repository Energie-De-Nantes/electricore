"""Vérifie que le wheel force-inclut les notebooks opérateur (pont transitoire, #414).

`uv build` doit produire un wheel `electricore` qui CONTIENT `accueil.py`,
`facturation.py` et `injection_rsc.py` sous `electricore/_operator_notebooks/`,
tandis que les sources restent dans `notebooks/` (aucun déplacement).

Test lent : construit réellement le wheel via `uv build` en sous-process.
"""

import shutil
import subprocess
import zipfile
from pathlib import Path

import pytest

# Racine du dépôt (ce fichier est tests/integration/…).
_RACINE = Path(__file__).resolve().parents[2]

# Sources attendues, et leur emplacement embarqué dans le wheel.
# `accueil.py` est la page de liens : sans elle, l'opérateur ne peut atteindre
# qu'un seul notebook (le middleware de dossier dynamique n'expose pas d'index).
_NOTEBOOKS = ("accueil.py", "facturation.py", "injection_rsc.py")
_PREFIXE_EMBARQUE = "electricore/_operator_notebooks"

# Notebooks Odoo gardés par la garde de sécurité (simulation + mo.stop). L'accueil
# n'écrit rien et n'a donc pas cette garde — il est exclu de ce sous-ensemble.
_NOTEBOOKS_GARDES = ("facturation.py", "injection_rsc.py")


def _est_erreur_reseau(stderr: str) -> bool:
    """Vrai si l'échec de build vient d'une indisponibilité réseau (hors-ligne)."""
    indices = ("dns error", "name resolution", "failed to lookup address", "Temporary failure in name")
    bas = stderr.lower()
    return any(indice.lower() in bas for indice in indices)


def test_sources_restent_dans_notebooks():
    """Les notebooks ne sont PAS déplacés : ils restent dans notebooks/."""
    for nom in _NOTEBOOKS:
        assert (_RACINE / "notebooks" / nom).is_file()


@pytest.mark.parametrize("nom", _NOTEBOOKS_GARDES)
def test_garde_de_securite_des_notebooks_intacte(nom):
    """Mode simulation `value=True` par défaut + écriture gardée par `mo.stop(run_button)`.

    Le lanceur ne touche pas aux notebooks : cette garde doit rester en place
    (sinon une écriture Odoo pourrait partir sans validation explicite).
    """
    source = (_RACINE / "notebooks" / nom).read_text()
    # Mode simulation activé par défaut (aucune écriture réelle tant que l'opérateur
    # ne le décoche pas explicitement).
    assert 'mo.ui.checkbox(label="Mode simulation' in source
    assert "value=True" in source
    # L'écriture OdooWriter est gardée derrière le bouton (mo.stop court-circuite
    # tant que le bouton n'est pas cliqué).
    assert "mo.stop(not run_button.value" in source
    assert "OdooWriter(config=config" in source


@pytest.mark.slow
def test_wheel_contient_les_notebooks_operateur(tmp_path: Path):
    """`uv build --wheel` produit un wheel electricore contenant les 3 notebooks."""
    if shutil.which("uv") is None:
        pytest.skip("uv indisponible : build du wheel non testable")

    def _build(*extra: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["uv", "build", "--wheel", "--out-dir", str(tmp_path), *extra],
            cwd=_RACINE,
            capture_output=True,
            text=True,
        )

    # CI a le réseau ; en bac-à-sable hors-ligne, le cache uv est déjà chaud →
    # repli --offline. Un échec qui reste réseau/DNS est environnemental → skip,
    # pas un faux négatif sur le force-include.
    resultat = _build()
    if resultat.returncode != 0 and _est_erreur_reseau(resultat.stderr):
        resultat = _build("--offline")
    if resultat.returncode != 0:
        if _est_erreur_reseau(resultat.stderr):
            pytest.skip(f"uv build indisponible hors-ligne (réseau requis) :\n{resultat.stderr}")
        pytest.fail(f"uv build a échoué :\n{resultat.stderr}")

    wheels = list(tmp_path.glob("electricore-*.whl"))
    assert len(wheels) == 1, f"attendu 1 wheel electricore, trouvé : {wheels}"

    noms = zipfile.ZipFile(wheels[0]).namelist()
    for nom in _NOTEBOOKS:
        attendu = f"{_PREFIXE_EMBARQUE}/{nom}"
        assert attendu in noms, f"{attendu} absent du wheel (présents : {noms[:20]})"

    # Le wheel ne livre QUE le package electricore (pas le client en workspace).
    assert not [n for n in noms if n.startswith("packages/")]
