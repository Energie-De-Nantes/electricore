"""Garde-fou : le calculateur turpe-variable est ERP-agnostique (ADR-0030, ADR-0016).

Le router et le service exposent un calculateur **electricore-natif** (assiette par
cadran → montant €) et n'importent **rien** de `electricore.integrations` : l'`id`
opaque de l'appelant n'est jamais interprété. Analyse statique via `ast` (couvre aussi
les imports `TYPE_CHECKING`).
"""

import ast
from pathlib import Path

import pytest

API_ROOT = Path(__file__).resolve().parents[2] / "electricore" / "api"

MODULES = [
    API_ROOT / "routers" / "turpe_variable.py",
    API_ROOT / "services" / "turpe_variable_service.py",
]


def _absolute_imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                modules.add(alias.name)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            modules.add(node.module)
    return modules


@pytest.mark.parametrize("module", MODULES, ids=lambda p: p.name)
def test_turpe_variable_n_importe_pas_integrations(module: Path) -> None:
    """ADR-0030 : ni le router ni le service n'importent `electricore.integrations`."""
    forbidden = sorted(
        m
        for m in _absolute_imports(module)
        if m == "electricore.integrations" or m.startswith("electricore.integrations.")
    )
    assert not forbidden, (
        f"{module.name} importe {forbidden} — le calculateur turpe-variable doit rester "
        "ERP-agnostique (ADR-0030 / ADR-0016)."
    )
