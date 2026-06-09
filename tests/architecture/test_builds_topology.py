"""Garde-fou : `electricore/core/builds/` ne dépend d'aucune integration ERP.

La règle (ADR-0019) est que les builds sont des compositions de pipelines et loaders,
ERP-agnostiques. L'I/O ERP reste à la charge de l'appelant (adapter ERP, router API).
Cela garantit que `core/` reste testable sans instance ERP (cf. ADR-0016).

Analyse statique via `ast` : couvre aussi les imports `TYPE_CHECKING`.
Les imports relatifs sont exclus par construction.
"""

import ast
from pathlib import Path

FORBIDDEN_PREFIXES: tuple[str, ...] = ("electricore.integrations",)

BUILDS_ROOT = Path(__file__).resolve().parents[2] / "electricore" / "core" / "builds"


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


def test_builds_has_no_erp_imports() -> None:
    """Topologie ADR-0019 : aucun fichier sous `electricore/core/builds/` n'importe `integrations/`."""
    violations: list[tuple[str, str]] = []
    repo_root = BUILDS_ROOT.parents[2]
    for py_file in sorted(BUILDS_ROOT.rglob("*.py")):
        for module in _collect_absolute_imports(py_file):
            if _is_forbidden(module):
                violations.append((str(py_file.relative_to(repo_root)), module))

    assert not violations, "Imports interdits dans electricore/core/builds/ :\n" + "\n".join(
        f"  {path}: {module}" for path, module in violations
    )


def test_builds_dir_exists() -> None:
    """Garde-fou de migration : `core/builds/` existe (ADR-0019, ex `core/orchestrations/`)."""
    assert BUILDS_ROOT.is_dir(), f"{BUILDS_ROOT} n'existe pas — renommage core/orchestrations/ → core/builds/ requis"


def test_orchestrations_dir_removed() -> None:
    """Garde-fou de migration : `core/orchestrations/` n'existe plus (ADR-0019)."""
    old_path = BUILDS_ROOT.parent / "orchestrations"
    assert not old_path.exists(), f"{old_path} existe encore — supprimer après migration vers core/builds/"
