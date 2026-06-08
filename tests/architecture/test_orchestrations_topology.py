"""Garde-fou : `electricore/core/orchestrations/` ne dépend d'aucun loader.

La règle est que les orchestrations sont des compositions pures de pipelines.
L'I/O (lecture DuckDB via `c15()`, `releves_harmonises()`, `f15()`, …) reste
à la charge de l'appelant (adapter ERP, router API). Cela garde les
orchestrations testables avec des LazyFrames synthétiques et facilement
réutilisables côté instance sans ERP (cf. CONTEXT.md, ADR-0016).

Analyse statique via `ast` : couvre aussi les imports `TYPE_CHECKING` (un type
emprunté à un loader resterait un couplage, même sans dépendance runtime).
Les imports relatifs (`from .X import Y`) sont exclus par construction — ils
restent dans le sous-package qui les déclare.
"""

import ast
from pathlib import Path

FORBIDDEN_PREFIXES: tuple[str, ...] = ("electricore.core.loaders",)

ORCHESTRATIONS_ROOT = Path(__file__).resolve().parents[2] / "electricore" / "core" / "orchestrations"


def _collect_absolute_imports(path: Path) -> set[str]:
    """Retourne les noms de modules importés *absolument* par un fichier .py."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                modules.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module:
                modules.add(node.module)
    return modules


def _is_forbidden(module: str) -> bool:
    return any(module == p or module.startswith(p + ".") for p in FORBIDDEN_PREFIXES)


def test_orchestrations_has_no_loader_imports() -> None:
    """Topologie : aucun fichier sous `electricore/core/orchestrations/` n'importe `core/loaders/`."""
    violations: list[tuple[str, str]] = []
    repo_root = ORCHESTRATIONS_ROOT.parents[2]
    for py_file in sorted(ORCHESTRATIONS_ROOT.rglob("*.py")):
        for module in _collect_absolute_imports(py_file):
            if _is_forbidden(module):
                violations.append((str(py_file.relative_to(repo_root)), module))

    assert not violations, "Imports interdits dans electricore/core/orchestrations/ :\n" + "\n".join(
        f"  {path}: {module}" for path, module in violations
    )
