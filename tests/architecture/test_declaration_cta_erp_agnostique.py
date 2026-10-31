"""Garde-fou : la déclaration CTA est ERP-agnostique (#651, ADR-0016/0058).

Successeur des `/taxes/cta/*` legacy (jointure Odoo `pdl_mapping`), le router et le
service de la déclaration n'importent **rien** de `electricore.integrations` : parc
Enedis entier, assiette recalculée par electricore. Analyse statique via `ast` (couvre
aussi les imports `TYPE_CHECKING`). Miroir de `test_sorties_erp_agnostique`.
"""

import ast
from pathlib import Path

import pytest

API_ROOT = Path(__file__).resolve().parents[2] / "electricore" / "api"

MODULES = [
    API_ROOT / "routers" / "declaration_cta.py",
    API_ROOT / "services" / "declaration_cta_service.py",
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
def test_declaration_cta_n_importe_pas_integrations(module: Path) -> None:
    """ADR-0016 : ni le router ni le service n'importent `electricore.integrations`."""
    forbidden = sorted(
        m
        for m in _absolute_imports(module)
        if m == "electricore.integrations" or m.startswith("electricore.integrations.")
    )
    assert not forbidden, f"{module.name} importe {forbidden} — la déclaration CTA doit rester ERP-agnostique."
