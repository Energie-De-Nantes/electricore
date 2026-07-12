"""Garde-fou : les sorties du périmètre sont ERP-agnostiques (#632, ADR-0016/0052).

Le router et le service exposent une projection **electricore-native** du flux C15
(événements de sortie RES/CFNS) et n'importent **rien** de `electricore.integrations` :
le RSC de l'appelant est un identifiant opaque, jamais interprété. Analyse statique via
`ast` (couvre aussi les imports `TYPE_CHECKING`). Miroir de `test_rsc_erp_agnostique` /
`test_turpe_variable_erp_agnostique` — même barre pour l'endpoint frère.
"""

import ast
from pathlib import Path

import pytest

API_ROOT = Path(__file__).resolve().parents[2] / "electricore" / "api"

MODULES = [
    API_ROOT / "routers" / "sorties.py",
    API_ROOT / "services" / "sorties_service.py",
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
def test_sorties_n_importe_pas_integrations(module: Path) -> None:
    """ADR-0016 : ni le router ni le service n'importent `electricore.integrations`."""
    forbidden = sorted(
        m
        for m in _absolute_imports(module)
        if m == "electricore.integrations" or m.startswith("electricore.integrations.")
    )
    assert not forbidden, (
        f"{module.name} importe {forbidden} — les sorties du périmètre doivent rester ERP-agnostiques (ADR-0016)."
    )


def test_router_sorties_single_source_les_modeles() -> None:
    """ADR-0043 : le router importe son modèle de `electricore_client`, plus aucune
    définition inline (`class SortiesRequest` dans le router)."""
    router = API_ROOT / "routers" / "sorties.py"
    imports = _absolute_imports(router)
    assert any(m == "electricore_client.models" or m.startswith("electricore_client") for m in imports), (
        "Le router sorties doit importer ses modèles depuis electricore_client (single-source)."
    )

    tree = ast.parse(router.read_text(encoding="utf-8"))
    classes = {node.name for node in ast.walk(tree) if isinstance(node, ast.ClassDef)}
    inline = classes & {"SortiesRequest", "LigneSortie"}
    assert not inline, (
        f"Modèles encore définis inline dans le router : {sorted(inline)} (déménager vers electricore_client)."
    )
