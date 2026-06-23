"""Garde-fou : `electricore_client` est polars-free au top-level.

L'invariant central du paquet (cf. ADR-0043) : un `import electricore_client`
ne tire que httpx + pydantic — jamais polars, duckdb ni fastapi. Le client
Arrow (extra `[arrow]`) importe polars *paresseusement*, dans un sous-module
qui n'est pas chargé au top-level ; cet invariant doit donc tenir même quand
polars est installé.

Mirroir de `tests/architecture/test_core_purity.py` (moteur) : on importe dans
un **sous-processus** et on inspecte `sys.modules` — la seule façon fiable de
prouver qu'un module lourd n'a pas été chargé transitivement.
"""

import subprocess
import sys

INTERDITS = ("polars", "duckdb", "fastapi")


def test_import_electricore_client_est_polars_free() -> None:
    """`import electricore_client` ne charge aucun module lourd interdit."""
    code = (
        "import importlib, sys\n"
        "importlib.import_module('electricore_client')\n"
        f"interdits = {INTERDITS!r}\n"
        "charges = [m for m in interdits if m in sys.modules]\n"
        "assert not charges, f'modules interdits chargés au top-level: {charges}'\n"
        "print('OK')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        "L'import de electricore_client a chargé un module interdit (polars/duckdb/fastapi) "
        f"ou a échoué.\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )
    assert "OK" in result.stdout
