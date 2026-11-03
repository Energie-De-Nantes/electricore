"""Anti-dérive de la cartographie des besoins (`docs/donnees/`).

La cartographie cite des noms exacts (endpoints, tables, fonctions, apps) pour qu'on
puisse s'y fier : ce test échoue dès qu'un nom cité n'existe plus dans le code.

- un endpoint cité (`GET /…`, `/flux/{table}.xlsx`…) doit correspondre à une route de l'app ;
- tout autre identifiant entre backticks doit apparaître comme mot dans le code.

ponytail: présence textuelle, pas sémantique — un nom resté dans un commentaire passe ;
suffit à attraper les renommages, qui sont la dérive courante.
Prior art : tests/unit/test_parite_docs.py.
"""

from __future__ import annotations

import re
from pathlib import Path

from electricore.api.main import app

REPO_ROOT = Path(__file__).resolve().parents[2]
PAGES = sorted((REPO_ROOT / "docs" / "donnees").rglob("*.md"))
CODE = "\n".join(
    f.read_text(encoding="utf-8")
    for racine in ("electricore", "packages")
    for f in (REPO_ROOT / racine).rglob("*")
    if f.suffix in {".py", ".sql", ".yml", ".yaml", ".csv"} and ".venv" not in f.parts
)

_BACKTICKS = re.compile(r"`([^`\n]+)`")
_IDENTIFIANT = re.compile(r"^[A-Za-z_][\w.-]*(\(\))?$")


def _jetons() -> list[str]:
    return [j for page in PAGES for j in _BACKTICKS.findall(page.read_text(encoding="utf-8"))]


def _route_existe(chemin: str) -> bool:
    chemin = re.sub(r"\{[^}]+\}", "x", chemin.split("?")[0])
    motifs = (re.sub(r"\{[^}]+\}", r"[^/]+", r.path) for r in app.routes)
    return any(re.fullmatch(m, chemin) for m in motifs)


def test_pages_presentes():
    assert PAGES, "docs/donnees/ vide"


def test_endpoints_cites_existent():
    # Endpoint = `GET /…`, `POST /…`, ou chemin à plusieurs segments ; `/ingestion` seul est une commande bot.
    chemins = [
        m.group(2)
        for j in _jetons()
        if (m := re.fullmatch(r"((?:GET|POST) )?(/\S+)", j)) and (m.group(1) or re.search(r"/.+[/.]", m.group(2)))
    ]
    absents = sorted({c for c in chemins if not _route_existe(c)})
    assert not absents, f"Endpoints cités dans docs/donnees/ sans route correspondante : {absents}"


def test_tables_flux_citees_sont_des_modeles_dbt():
    modeles = {f.stem for f in (REPO_ROOT / "electricore" / "ingestion" / "dbt" / "models").rglob("*.sql")}
    absentes = sorted({j for j in _jetons() if re.fullmatch(r"flux_\w+", j)} - modeles)
    assert not absentes, f"Tables citées dans docs/donnees/ sans modèle dbt : {absentes}"


def test_identifiants_cites_existent():
    def connu(nom: str) -> bool:
        return all(re.search(rf"(?<![\w-]){re.escape(p)}(?![\w-])", CODE) for p in nom.removesuffix("()").split("."))

    candidats = {j for j in _jetons() if _IDENTIFIANT.match(j) and not j.isupper()}
    absents = sorted(j for j in candidats if not connu(j))
    assert not absents, f"Identifiants cités dans docs/donnees/ introuvables dans le code : {absents}"
