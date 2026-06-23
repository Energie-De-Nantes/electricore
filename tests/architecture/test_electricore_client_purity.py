"""Garde-fou (côté moteur) : `electricore_client` reste polars-free au top-level.

Le moteur dépend de `electricore-client` (source unique des modèles de contrat,
ADR-0043). Cet invariant — `import electricore_client` ne tire que httpx +
pydantic — doit donc rester vrai dans l'environnement du moteur (où polars EST
installé). On le prouve dans un **sous-processus** en inspectant `sys.modules`,
comme `test_core_purity.py`. Le test-client CI le revérifie en isolation
(httpx + pydantic seuls).
"""

import subprocess
import sys

INTERDITS = ("polars", "duckdb", "fastapi")


def test_import_electricore_client_ne_charge_pas_polars() -> None:
    """`import electricore_client` ne charge ni polars, ni duckdb, ni fastapi."""
    code = (
        "import importlib, sys\n"
        "importlib.import_module('electricore_client')\n"
        f"interdits = {INTERDITS!r}\n"
        "charges = [m for m in interdits if m in sys.modules]\n"
        "assert not charges, f'modules interdits chargés: {charges}'\n"
        "print('OK')\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0 and "OK" in result.stdout, (
        f"electricore_client a chargé un module interdit ou a échoué.\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )
