"""Garde d'indépendance du relais (#637, dernier critère d'acceptation).

Le relais est un outil « pose-et-oublie » : il ne doit importer AUCUNE surface
mouvante de l'ingestion (runner, curseur/état incrémental, sa DuckDB) — sinon
une évolution du runner (changement de schéma d'état, de curseur, de config
`flux.yaml`…) casserait le relais sans qu'on l'ait touché. Il ne réutilise que
les briques stables explicitement documentées : crypto, archive, sftp_enedis
(`create_sftp_resource`, générique et fsspec-agnostic — pas la
`sftp_enedis_brut` qui orchestre le curseur incrémental multi-flux).

Deux niveaux de garde : statique (grep des imports dans le code source, ne
dépend d'aucun import réel) et dynamique (sys.modules après import, prouve
qu'aucun import transitif ne charge le runner).
"""

import ast
import sys
from pathlib import Path

RACINE_RELAIS = Path(__file__).parents[2] / "electricore" / "ingestion" / "relais"

# Surface mouvante interdite (#637) : runner, curseur/état incrémental, orchestration
# multi-flux qui en dépend. `sources.sftp_enedis` (create_sftp_resource, brique stable
# générique) et `transformers.*` (crypto/archive) sont EXPLICITEMENT autorisés.
MODULES_INTERDITS = (
    "electricore.ingestion.runner",
    "electricore.ingestion.raw_landing",
    "electricore.ingestion.sources.sftp_enedis_brut",
)


def _imports_du_fichier(chemin: Path) -> set[str]:
    """Modules importés (absolus, `import x` / `from x import y`) d'un fichier source."""
    arbre = ast.parse(chemin.read_text(), filename=str(chemin))
    modules: set[str] = set()
    for noeud in ast.walk(arbre):
        if isinstance(noeud, ast.Import):
            modules.update(alias.name for alias in noeud.names)
        elif isinstance(noeud, ast.ImportFrom) and noeud.module and noeud.level == 0:
            modules.add(noeud.module)
    return modules


def test_le_code_source_du_relais_n_importe_aucune_surface_mouvante_de_l_ingestion():
    """Garde statique : lit chaque .py de electricore/ingestion/relais/, vérifie qu'aucun
    import (direct ou `from`) ne cible le runner/curseur/état/DuckDB de l'ingestion."""
    fichiers = sorted(RACINE_RELAIS.rglob("*.py"))
    assert fichiers, "electricore/ingestion/relais/ doit exister et contenir du code"

    for fichier in fichiers:
        modules = _imports_du_fichier(fichier)
        for interdit in MODULES_INTERDITS:
            fautifs = {m for m in modules if m == interdit or m.startswith(interdit + ".")}
            assert not fautifs, f"{fichier.name} importe {fautifs} — surface mouvante de l'ingestion interdite (#637)"


def test_importer_le_relais_n_ajoute_aucune_surface_mouvante_a_sys_modules():
    """Garde dynamique : importer le relais n'AJOUTE aucun module interdit à sys.modules.

    Comparaison avant/après (pas une absence absolue) : en suite complète, un module
    interdit peut déjà être chargé par un AUTRE test sans lien (ex. les tests du runner
    lui-même) — ce qui compte est que **le relais** ne le tire pas transitivement."""
    for nom in list(sys.modules):
        if nom.startswith("electricore.ingestion.relais"):
            del sys.modules[nom]

    avant = set(sys.modules)
    import electricore.ingestion.relais.__main__  # noqa: F401
    import electricore.ingestion.relais.pipeline  # noqa: F401

    ajoutes = set(sys.modules) - avant

    for interdit in MODULES_INTERDITS:
        fautifs = {m for m in ajoutes if m == interdit or m.startswith(interdit + ".")}
        assert not fautifs, f"importer le relais a chargé {fautifs} — surface mouvante interdite (#637)"
