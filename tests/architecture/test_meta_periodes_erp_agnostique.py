"""Garde-fou : les endpoints « Odoo tire » sont ERP-agnostiques (ADR-0027, ADR-0016).

Contrairement aux autres endpoints `/facturation/*` (qui lisent Odoo via `@with_odoo`),
le router et le service des **méta-périodes** et de la **chronologie facturiste** (#367)
exposent un modèle **electricore-natif** et n'importent **rien** de
`electricore.integrations`. Analyse statique via `ast` (couvre aussi les imports
`TYPE_CHECKING`).
"""

import ast
from pathlib import Path

import pytest

API_ROOT = Path(__file__).resolve().parents[2] / "electricore" / "api"

MODULES = [
    API_ROOT / "routers" / "meta_periodes.py",
    API_ROOT / "services" / "meta_periodes_service.py",
    API_ROOT / "routers" / "chronologie.py",
    API_ROOT / "services" / "chronologie_service.py",
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
def test_endpoint_odoo_tire_n_importe_pas_integrations(module: Path) -> None:
    """ADR-0027 : ni le router ni le service n'importent `electricore.integrations`."""
    forbidden = sorted(
        m
        for m in _absolute_imports(module)
        if m == "electricore.integrations" or m.startswith("electricore.integrations.")
    )
    assert not forbidden, (
        f"{module.name} importe {forbidden} — un endpoint « Odoo tire » doit rester "
        "ERP-agnostique (ADR-0027 / ADR-0016)."
    )
