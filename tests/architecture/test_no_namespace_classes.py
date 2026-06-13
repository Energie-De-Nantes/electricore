"""
Guard architectural contre les classes-namespace (ADR-0018).

Toute classe dans `electricore/` doit être justifiée par son état interne.
Sont acceptées :
- Classes avec `__init__` custom (état géré manuellement)
- Classes décorées `@dataclass` (avec ou sans paramètres)
- Sous-classes des parents de l'allow-list ci-dessous

Allow-list (types qui apportent leur propre machinerie de déclaration) :
- `pa.DataFrameModel` / `DataFrameModel` (Pandera)
- `BaseModel` (Pydantic)
- `BaseSettings` (pydantic-settings — registre runtime, #141/ADR-0025)
- `StrEnum`, `Enum`, `IntEnum` (stdlib)
- `NamedTuple` (cas marginal autorisé explicitement)
- `dict`, `list`, `tuple`, `set` (sous-classes utilitaires)
- Sous-classes d'exceptions — base dont le nom finit par `Error`, `Exception`
  ou `Warning` (convention PEP 8) : la machinerie d'état vient de
  `BaseException`, le corps vide est l'idiome (ex : `DuckDBLockError`).

Toute nouvelle lib apportant son propre type de base doit être ajoutée ici.
"""

import ast
from pathlib import Path

_ELECTRICORE_ROOT = Path(__file__).parents[2] / "electricore"

_ALLOW_LIST_SIMPLE = {
    "DataFrameModel",
    "BaseModel",
    "BaseSettings",
    "StrEnum",
    "Enum",
    "IntEnum",
    "NamedTuple",
    "dict",
    "list",
    "tuple",
    "set",
}

_ALLOW_LIST_ATTRIBUTE = {
    ("pa", "DataFrameModel"),
}


def _has_custom_init(class_node: ast.ClassDef) -> bool:
    return any(isinstance(node, ast.FunctionDef) and node.name == "__init__" for node in class_node.body)


def _has_dataclass_decorator(class_node: ast.ClassDef) -> bool:
    for dec in class_node.decorator_list:
        if isinstance(dec, ast.Name) and dec.id == "dataclass":
            return True
        if isinstance(dec, ast.Call):
            func = dec.func
            if isinstance(func, ast.Name) and func.id == "dataclass":
                return True
            if isinstance(func, ast.Attribute) and func.attr == "dataclass":
                return True
    return False


_EXCEPTION_SUFFIXES = ("Error", "Exception", "Warning")


def _base_in_allowlist(base: ast.expr) -> bool:
    if isinstance(base, ast.Name):
        if base.id in _ALLOW_LIST_SIMPLE:
            return True
        # Sous-classe d'exception : convention PEP 8 sur le nom de la base.
        if base.id.endswith(_EXCEPTION_SUFFIXES):
            return True
    if isinstance(base, ast.Attribute):
        if isinstance(base.value, ast.Name):
            if (base.value.id, base.attr) in _ALLOW_LIST_ATTRIBUTE:
                return True
        # Idem pour les bases qualifiées (ex : duckdb.IOException).
        if base.attr.endswith(_EXCEPTION_SUFFIXES):
            return True
    return False


def _has_allowlisted_parent(class_node: ast.ClassDef) -> bool:
    return any(_base_in_allowlist(base) for base in class_node.bases)


class _ViolationVisitor(ast.NodeVisitor):
    """Visite l'AST en maintenant la pile des classes pour ignorer les classes imbriquées
    dans un parent de l'allow-list (ex: class Config dans pa.DataFrameModel)."""

    def __init__(self, source_path: Path) -> None:
        self.violations: list[str] = []
        self._source_path = source_path
        self._class_stack: list[ast.ClassDef] = []

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        inside_allowlisted = any(_has_allowlisted_parent(cls) for cls in self._class_stack)
        if not inside_allowlisted:
            if not _has_custom_init(node) and not _has_dataclass_decorator(node) and not _has_allowlisted_parent(node):
                rel = self._source_path.relative_to(_ELECTRICORE_ROOT.parent)
                self.violations.append(
                    f"class {node.name} in {rel}:{node.lineno} violates ADR-0018: "
                    "no __init__, no @dataclass, no parent in allow-list. "
                    "Either justify the state (lifecycle/builder/cache/job), "
                    "use @dataclass(frozen=True, slots=True), or convert to module functions."
                )
        self._class_stack.append(node)
        self.generic_visit(node)
        self._class_stack.pop()


def _collect_violations(source_path: Path) -> list[str]:
    try:
        tree = ast.parse(source_path.read_text(encoding="utf-8"))
    except SyntaxError:
        return []
    visitor = _ViolationVisitor(source_path)
    visitor.visit(tree)
    return visitor.violations


def test_no_namespace_classes():
    """Aucune classe-namespace dans electricore/ (ADR-0018)."""
    py_files = [p for p in _ELECTRICORE_ROOT.rglob("*.py") if "__pycache__" not in p.parts]
    assert py_files, "Aucun fichier .py trouvé dans electricore/"

    all_violations: list[str] = []
    for path in sorted(py_files):
        all_violations.extend(_collect_violations(path))

    assert not all_violations, "\n" + "\n".join(all_violations)
