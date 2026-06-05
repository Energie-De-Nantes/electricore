"""Garde-fou : `electricore/core/` ne dépend d'aucun ERP ni couche supérieure.

Enforce [ADR-0016](../../docs/adr/0016-core-erp-agnostique.md) — `core/` ne
dépend que de Polars, DuckDB, Pandera et la stdlib. Aucun import vers
`electricore.integrations`, `electricore.api`, `electricore.bot`, ou une lib
ERP (`odoorpc`, …) n'est toléré.

Analyse statique via `ast` : couvre aussi les imports `TYPE_CHECKING` (un type
emprunté à une couche supérieure reste un couplage, même sans dépendance runtime).
Les imports relatifs (`from .X import Y`) sont exclus par construction — ils
restent dans le sous-package qui les déclare.
"""

import ast
from pathlib import Path

FORBIDDEN_PREFIXES: tuple[str, ...] = (
    "electricore.integrations",
    "electricore.api",
    "electricore.bot",
    "odoorpc",
)

CORE_ROOT = Path(__file__).resolve().parents[2] / "electricore" / "core"


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


def test_core_has_no_forbidden_imports() -> None:
    """ADR-0016 : aucun fichier sous `electricore/core/` n'importe d'ERP ou de couche supérieure."""
    violations: list[tuple[str, str]] = []
    repo_root = CORE_ROOT.parent.parent
    for py_file in sorted(CORE_ROOT.rglob("*.py")):
        for module in _collect_absolute_imports(py_file):
            if _is_forbidden(module):
                violations.append((str(py_file.relative_to(repo_root)), module))

    assert not violations, "Imports interdits dans electricore/core/ (cf. ADR-0016) :\n" + "\n".join(
        f"  {path}: {module}" for path, module in violations
    )
