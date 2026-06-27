"""Garde-fou : la résolution RSC est ERP-agnostique (#282, ADR-0016/0043).

Le router et le service recoupent deux flux Enedis (X12 ⨝ C15) et n'importent **rien** de
`electricore.integrations` : l'`id_affaire` opaque de l'appelant n'est jamais interprété.
Analyse statique via `ast` (couvre aussi les imports `TYPE_CHECKING`). Miroir de
`test_turpe_variable_erp_agnostique` — même barre pour l'endpoint frère.
"""

import ast
from pathlib import Path

import pytest

API_ROOT = Path(__file__).resolve().parents[2] / "electricore" / "api"

MODULES = [
    API_ROOT / "routers" / "rsc.py",
    API_ROOT / "services" / "rsc_service.py",
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
def test_rsc_n_importe_pas_integrations(module: Path) -> None:
    """ADR-0016 : ni le router ni le service n'importent `electricore.integrations`."""
    forbidden = sorted(
        m
        for m in _absolute_imports(module)
        if m == "electricore.integrations" or m.startswith("electricore.integrations.")
    )
    assert not forbidden, (
        f"{module.name} importe {forbidden} — la résolution RSC doit rester ERP-agnostique (ADR-0016)."
    )


def test_router_rsc_single_source_les_modeles() -> None:
    """ADR-0043 : le router importe son modèle de `electricore_client`, plus aucune
    définition inline (`class ResolutionRscRequest` dans le router)."""
    router = API_ROOT / "routers" / "rsc.py"
    imports = _absolute_imports(router)
    assert any(m == "electricore_client.models" or m.startswith("electricore_client") for m in imports), (
        "Le router rsc doit importer ses modèles depuis electricore_client (single-source)."
    )

    tree = ast.parse(router.read_text(encoding="utf-8"))
    classes = {node.name for node in ast.walk(tree) if isinstance(node, ast.ClassDef)}
    inline = classes & {"ResolutionRscRequest", "ResultatResolutionRsc"}
    assert not inline, (
        f"Modèles encore définis inline dans le router : {sorted(inline)} (déménager vers electricore_client)."
    )
