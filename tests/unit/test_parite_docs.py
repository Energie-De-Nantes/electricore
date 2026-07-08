"""Garde-fous anti-pourrissement du site (#560) : parité nav + fraîcheur.

Deux garanties sur le même seam — le site publié (`mkdocs.yml` + `docs/**`) — sans
linter de prose ni seam par page (cf. #560) :

1. **Parité nav** : tout `.md` sous `docs/` est soit référencé dans la nav de
   `mkdocs.yml`, soit dans la liste d'exclusions EXPLICITE ci-dessous (ADRs, spikes,
   registres agent, docs techniques en attente d'affectation #557). Un doc ajouté
   hors nav sans y être ajouté ici fait échouer le test — pas d'orphelin silencieux.
2. **Fraîcheur** : toute page EN NAV porte un front-matter `fraicheur: YYYY-MM-DD`
   (convention du chantier #555/#560). Un check minimal liste (sans échouer) les
   pages de plus de 12 mois — pas de machinerie de rapport, juste un print.

Le parsing (nav YAML + front-matter) réutilise les mêmes outils que MkDocs lui-même
(`mkdocs.utils.yaml.yaml_load`, `mkdocs.utils.meta.get_data`) plutôt que de
réinventer un mini-parseur : même tolérance aux tags YAML mkdocs (`!ENV`…) et même
notion de « front-matter » que ce que le site construit réellement.

Prior art (style) : tests/unit/test_parite_typage.py, tests/unit/test_contexte_filtre_parite.py.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pytest

pytest.importorskip("mkdocs", reason="mkdocs absent — uv sync --group docs")

from mkdocs.utils.meta import get_data
from mkdocs.utils.yaml import yaml_load

REPO_ROOT = Path(__file__).resolve().parents[2]
DOCS_DIR = REPO_ROOT / "docs"
MKDOCS_YML = REPO_ROOT / "mkdocs.yml"

# ---------------------------------------------------------------------------
# Exclusions nav — versionnées et explicites (#560). Un doc ajouté hors nav SANS
# être ajouté ici fait échouer `test_tout_doc_est_en_nav_ou_exclu`.
# ---------------------------------------------------------------------------

# Répertoires entiers hors périmètre de la parité nav.
_PREFIXES_EXCLUS = (
    "adr/",  # registre immuable, publié hors nav — indexé par #559
    "spikes/",  # archives de spikes (répertoire vide pour l'instant)
    "agents/",  # registres agent (CLAUDE.md-adjacent), hors chemin doc-par-rôles
)

# Docs techniques à la racine de docs/, jugés périmés ou à fusionner par la passe
# de lecture #557 — chacun a son issue fille de mise à jour ; un doc remis à jour
# entre en nav et sort de cette liste (elle a vocation à fondre).
_FICHIERS_EN_ATTENTE_557 = {
    "configuration.md",  # périmé (§3 pré-secrets-as-code) — issue fille #605
    "conventions-dates-enedis.md",  # périmé (pointeurs impl. pré-ADR-0042) — issue fille #606
    "electricore-client-design.md",  # à fusionner (note de conception ADR-0043) — issue fille #611
    "ingestion.md",  # périmé (footer dates pré-ADR-0042) — issue fille #607
    "qualite-donnees-r151.md",  # rapport daté, candidat archives — issue fille #608
    "transmission.md",  # à fusionner (recouvre contribuer/developper.md) — issue fille #618
    "turpe-fixe-c4-btsup36kva.md",  # périmé (colonnes/coefficients) — issue fille #609
    "turpe-usage-standalone.md",  # périmé (colonnes _kva/_eur) — issue fille #610
}

_AGE_MAX_FRAICHEUR_JOURS = 365


def _est_exclu(chemin_relatif: str) -> bool:
    if chemin_relatif in _FICHIERS_EN_ATTENTE_557:
        return True
    return any(chemin_relatif.startswith(prefixe) for prefixe in _PREFIXES_EXCLUS)


def _config_mkdocs() -> dict:
    with MKDOCS_YML.open("rb") as f:
        return yaml_load(f)


def _chemins_nav(entree) -> list[str]:
    """Aplatit récursivement un bloc `nav:` de mkdocs.yml en chemins `.md` (relatifs à docs/)."""
    if isinstance(entree, str):
        return [entree] if entree.endswith(".md") else []
    if isinstance(entree, dict):
        return [c for valeur in entree.values() for c in _chemins_nav(valeur)]
    if isinstance(entree, list):
        return [c for item in entree for c in _chemins_nav(item)]
    return []


def _pages_en_nav() -> list[str]:
    return sorted(set(_chemins_nav(_config_mkdocs()["nav"])))


def _tous_les_docs() -> set[str]:
    return {str(p.relative_to(DOCS_DIR)) for p in DOCS_DIR.rglob("*.md")}


def _meta(chemin_relatif: str) -> dict:
    texte = (DOCS_DIR / chemin_relatif).read_text(encoding="utf-8")
    _, meta = get_data(texte)
    return meta


def test_tout_doc_est_en_nav_ou_exclu():
    en_nav = set(_pages_en_nav())
    orphelins = {d for d in _tous_les_docs() if d not in en_nav and not _est_exclu(d)}

    assert not orphelins, (
        "Docs hors nav sans exclusion explicite (#560) — ajoute-les à la nav de "
        "mkdocs.yml ou à la liste d'exclusions versionnée de ce module : "
        f"{sorted(orphelins)}"
    )


def test_toute_page_en_nav_porte_une_fraicheur():
    sans_fraicheur = [p for p in _pages_en_nav() if "fraicheur" not in _meta(p)]

    assert not sans_fraicheur, f"Pages en nav sans front-matter `fraicheur: YYYY-MM-DD` (#560) : {sans_fraicheur}"


def test_pages_perimees_signalees_sans_echouer():
    """Check minimal (#560) : liste — sans jamais échouer — les pages en nav dont la
    fraîcheur a plus de 12 mois. Pas de machinerie de rapport, juste un print."""
    aujourdhui = date.today()
    perimees = []
    for chemin_relatif in _pages_en_nav():
        brut = _meta(chemin_relatif).get("fraicheur")
        if not brut:
            continue
        fraicheur = brut if isinstance(brut, date) else datetime.strptime(str(brut), "%Y-%m-%d").date()
        if (aujourdhui - fraicheur).days > _AGE_MAX_FRAICHEUR_JOURS:
            perimees.append(f"{chemin_relatif} (fraicheur: {fraicheur})")

    if perimees:
        print(f"\n[fraîcheur #560] {len(perimees)} page(s) de plus de 12 mois :")
        for ligne in perimees:
            print(f"  - {ligne}")
