"""Garde-fou : `electricore/bot/` est un pur client HTTP de l'API.

Le bot traduit des commandes Telegram en appels HTTP et formate les réponses
([bot/CONTEXT.md](../../electricore/bot/CONTEXT.md), ADR-0009/0010). Il n'importe
ni la logique métier (`core/`, `integrations/`, `ingestion/`) ni l'intérieur de l'API
(`api.services`, `api.routers`) — seule la config partagée (`api.config`) est
tolérée.

Analyse statique via `ast` : couvre aussi les imports locaux aux fonctions
(c'est précisément ainsi que la violation #150 s'était glissée).
"""

import ast
from pathlib import Path

FORBIDDEN_PREFIXES: tuple[str, ...] = (
    "electricore.api.services",
    "electricore.api.routers",
    "electricore.core",
    "electricore.integrations",
    "electricore.ingestion",
)

BOT_ROOT = Path(__file__).resolve().parents[2] / "electricore" / "bot"


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


def test_bot_est_pur_client_http() -> None:
    """Aucun fichier sous `electricore/bot/` n'importe métier ou intérieur d'API."""
    violations: list[tuple[str, str]] = []
    repo_root = BOT_ROOT.parents[1]
    for py_file in sorted(BOT_ROOT.rglob("*.py")):
        for module in _collect_absolute_imports(py_file):
            if _is_forbidden(module):
                violations.append((str(py_file.relative_to(repo_root)), module))

    assert not violations, "Imports interdits dans electricore/bot/ (pur client HTTP) :\n" + "\n".join(
        f"  {path}: {module}" for path, module in violations
    )
