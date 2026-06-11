"""Garde-fous des règles d'import par rôle (ADR-0019, issues #109, #144 ; ADR-0023, issue #178).

Cinq règles enforced par analyse statique (`ast`) :

1. `core/pipelines/**` : n'importe pas `core.loaders`, `core.builds`,
   `integrations.*`, `api.*`. (Transformations pures.)

2. `core/builds/**` : n'importe pas `integrations.*`. (ERP-agnostique.)
   *(Doublon explicite de `test_builds_topology.py` — ici pour cohérence du fichier.)*

3. `integrations/<erp>/**` : ne déclare pas de fonctions dont l'annotation de
   retour référence `core.builds.*`. (Heuristique : si une intégration annote un
   retour `RapportTaxe`, elle assemble un livrable — c'est une violation d'ADR-0019.)

4. `integrations/<erp>/**` : whitelist — n'importe de `electricore.core` que
   `core.models` et `core.loaders` (calque exact de la table ADR-0019, pendant
   inverse de `test_core_purity`). Un import `core.builds` / `core.pipelines` /
   `core.writers` depuis une intégration échoue ici (#144).

5. `core/pipelines/abonnements.py` ↔ `core/pipelines/energie.py` : aucun import
   dans aucun sens (ADR-0023, #178). Les périodisations sont indépendantes ;
   les formules partagées vivent dans `core/pipelines/periodes.py`.

Pour chaque violation : fichier, ligne, import/annotation fautif + message ADR.

Voir [ADR-0019](../../docs/adr/0019-roles-loaders-pipelines-builds-integrations.md)
et [ADR-0023](../../docs/adr/0023-periodisations-separees-abonnement-energie.md).
"""

import ast
from pathlib import Path

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[2]
ELECTRICORE = REPO_ROOT / "electricore"

PIPELINES_ROOT = ELECTRICORE / "core" / "pipelines"
BUILDS_ROOT = ELECTRICORE / "core" / "builds"
INTEGRATIONS_ROOT = ELECTRICORE / "integrations"


def _py_files(root: Path) -> list[Path]:
    return sorted(p for p in root.rglob("*.py") if "__pycache__" not in str(p))


def _absolute_imports(path: Path) -> list[tuple[int, str]]:
    """Retourne `[(ligne, module), ...]` pour les imports absolus du fichier."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    result: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                result.append((node.lineno, alias.name))
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module:
                result.append((node.lineno, node.module))
    return result


def _return_annotation_names(path: Path) -> list[tuple[int, str]]:
    """Retourne `[(ligne, nom_type), ...]` des annotations de retour de fonctions."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    result: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.returns is None:
            continue
        # Extraire le texte brut de l'annotation
        try:
            name = ast.unparse(node.returns)
        except Exception:
            continue
        result.append((node.lineno, name))
    return result


def _fmt(path: Path, line: int, detail: str) -> str:
    return f"  {path.relative_to(REPO_ROOT)}:{line}: {detail}"


# ---------------------------------------------------------------------------
# Règle 1 : core/pipelines/** n'importe pas loaders / builds / integrations / api
# ---------------------------------------------------------------------------

PIPELINES_FORBIDDEN = (
    "electricore.core.loaders",
    "electricore.core.builds",
    "electricore.integrations",
    "electricore.api",
)


def test_pipelines_no_forbidden_imports() -> None:
    """Règle 1 ADR-0019 : `core/pipelines/` est une zone de transformations pures.

    Imports interdits : loaders, builds, integrations, api.
    """
    violations: list[str] = []
    for py_file in _py_files(PIPELINES_ROOT):
        for lineno, module in _absolute_imports(py_file):
            if any(module == p or module.startswith(p + ".") for p in PIPELINES_FORBIDDEN):
                violations.append(_fmt(py_file, lineno, f"import interdit: {module}"))

    assert not violations, (
        "Violations règle 1 ADR-0019 (core/pipelines/ import interdit) :\n"
        + "\n".join(violations)
        + "\nVoir docs/adr/0019-roles-loaders-pipelines-builds-integrations.md"
    )


# ---------------------------------------------------------------------------
# Règle 2 : core/builds/** n'importe pas integrations/
# ---------------------------------------------------------------------------

BUILDS_FORBIDDEN = ("electricore.integrations",)


def test_builds_no_erp_imports() -> None:
    """Règle 2 ADR-0019 : `core/builds/` est ERP-agnostique.

    Import interdit : integrations.* (sources ERP injectées par le caller).
    """
    violations: list[str] = []
    for py_file in _py_files(BUILDS_ROOT):
        for lineno, module in _absolute_imports(py_file):
            if any(module == p or module.startswith(p + ".") for p in BUILDS_FORBIDDEN):
                violations.append(_fmt(py_file, lineno, f"import interdit: {module}"))

    assert not violations, (
        "Violations règle 2 ADR-0019 (core/builds/ import integrations interdit) :\n"
        + "\n".join(violations)
        + "\nVoir docs/adr/0019-roles-loaders-pipelines-builds-integrations.md"
    )


# ---------------------------------------------------------------------------
# Règle 3 : integrations/<erp>/** n'annote pas de retour core.builds.*
# ---------------------------------------------------------------------------

BUILDS_MODULE_PREFIX = "electricore.core.builds"


def _builds_public_names() -> set[str]:
    """Noms publics définis dans core/builds/ (classes, fonctions) — sans préfixe."""
    names: set[str] = set()
    for py_file in _py_files(BUILDS_ROOT):
        tree = ast.parse(py_file.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, (ast.ClassDef, ast.FunctionDef)):
                if not node.name.startswith("_"):
                    names.add(node.name)
    return names


def test_integrations_no_builds_return_type() -> None:
    """Règle 3 ADR-0019 : `integrations/<erp>/` n'annote pas de retour `core.builds.*`.

    Heuristique : si une annotation de retour contient un nom défini dans
    `core/builds/`, c'est un assemblage de livrable dans une intégration — violation.
    Les noms sans qualification (ex: `RapportTaxe`) sont aussi détectés si le module
    importe depuis `core.builds`.
    """
    builds_names = _builds_public_names()
    violations: list[str] = []

    for erp_dir in INTEGRATIONS_ROOT.iterdir():
        if not erp_dir.is_dir():
            continue
        for py_file in _py_files(erp_dir):
            # Collecter les imports core.builds dans ce fichier
            imported_builds = {
                module.split(".")[-1]
                for _, module in _absolute_imports(py_file)
                if module.startswith(BUILDS_MODULE_PREFIX)
            }
            # Vérifier les annotations de retour
            for lineno, annotation in _return_annotation_names(py_file):
                for name in builds_names:
                    if name in annotation and name in imported_builds:
                        violations.append(
                            _fmt(py_file, lineno, f"retour `{annotation}` référence `core.builds.{name}`")
                        )

    assert not violations, (
        "Violations règle 3 ADR-0019 (integrations/ retour core.builds interdit) :\n"
        + "\n".join(violations)
        + "\nDéplacer l'assemblage vers core/builds/, laisser seul l'I/O Odoo dans integrations/.\n"
        + "Voir docs/adr/0019-roles-loaders-pipelines-builds-integrations.md"
    )


# ---------------------------------------------------------------------------
# Règle 4 : integrations/<erp>/** — whitelist des imports core (#144)
# ---------------------------------------------------------------------------

INTEGRATIONS_CORE_WHITELIST = (
    "electricore.core.models",
    "electricore.core.loaders",
)


def _imports_resolus_pipelines(path: Path) -> list[tuple[int, str]]:
    """Imports du fichier, relatifs résolus contre le package `core.pipelines`."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    result: list[tuple[int, str]] = list(_absolute_imports(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.level == 1 and node.module:
            result.append((node.lineno, f"electricore.core.pipelines.{node.module}"))
    return result


def test_periodisations_independantes() -> None:
    """Règle 5 ADR-0023 (#178) : abonnements et energie ne s'importent pas l'un l'autre.

    Les deux périodisations opèrent sur des lignes de temps distinctes (Enedis ne
    fournit pas de relevé aux événements hors comptage) ; les formules partagées
    (nb_jours, mois_annee, libellés) vivent dans `core/pipelines/periodes.py`.
    """
    paire = {
        PIPELINES_ROOT / "abonnements.py": "electricore.core.pipelines.energie",
        PIPELINES_ROOT / "energie.py": "electricore.core.pipelines.abonnements",
    }
    violations: list[str] = []
    for py_file, interdit in paire.items():
        for lineno, module in _imports_resolus_pipelines(py_file):
            if module == interdit or module.startswith(interdit + "."):
                violations.append(_fmt(py_file, lineno, f"import interdit: {module}"))

    assert not violations, (
        "Violations règle 5 ADR-0023 (périodisations indépendantes) :\n"
        + "\n".join(violations)
        + "\nLes formules partagées vivent dans core/pipelines/periodes.py.\n"
        + "Voir docs/adr/0023-periodisations-separees-abonnement-energie.md"
    )


def test_integrations_core_imports_whitelist() -> None:
    """Règle 4 ADR-0019 (#144) : `integrations/<erp>/` n'importe de `core` que
    `core.models` et `core.loaders`.

    Pendant inverse de `test_core_purity` : les sources/sinks ERP ne composent
    ni pipelines, ni builds, ni writers — l'assemblage vit en `core/builds/`,
    le wire-up en `api/services/`.
    """
    violations: list[str] = []
    for erp_dir in INTEGRATIONS_ROOT.iterdir():
        if not erp_dir.is_dir() or erp_dir.name == "__pycache__":
            continue
        for py_file in _py_files(erp_dir):
            for lineno, module in _absolute_imports(py_file):
                if module != "electricore.core" and not module.startswith("electricore.core."):
                    continue
                if any(module == w or module.startswith(w + ".") for w in INTEGRATIONS_CORE_WHITELIST):
                    continue
                violations.append(_fmt(py_file, lineno, f"import hors whitelist: {module}"))

    assert not violations, (
        "Violations règle 4 ADR-0019 (integrations/ importe core hors models/loaders) :\n"
        + "\n".join(violations)
        + "\nL'assemblage descend en core/builds/, le wire-up monte en api/services/.\n"
        + "Voir docs/adr/0019-roles-loaders-pipelines-builds-integrations.md"
    )
