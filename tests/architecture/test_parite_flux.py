"""Parité des sites de déclaration d'un flux connu (#538, ADR-0053).

Un *flux connu* (glossaire, `electricore/ingestion/CONTEXT.md`) se déclare dans plusieurs
sites répartis sur quatre modules : configuration de flux (`flux.yaml`), mapping raw→modèles
du runner, sources dbt, modèles dbt, registre des descripteurs loaders (+ factories/exports),
colonnes de date métier (stats), libellés bot. Un registre central est structurellement
impossible — trois murs (dbt parse son YAML avant tout runtime ; `core/` doit rester
installable sans l'extra `[ingestion]` ; le bot ne consomme que l'API) détaillés dans
[ADR-0053](../../docs/adr/0053-parite-sites-declaration-flux-connu.md). Ce test croise
tous les sites et porte ses exceptions en clair — c'est la protection qui remplace un
registre central impossible.

Skip si `dlt`/dbt absents (`uv sync --extra ingestion --extra dbt`).
"""

import ast
import re
from pathlib import Path

import pytest
import yaml

pytest.importorskip("dlt", reason="dlt absent — uv sync --extra ingestion")

from electricore.ingestion.config import flux_connus  # noqa: E402
from electricore.ingestion.runner import MODELES_PAR_RAW  # noqa: E402

RACINE = Path(__file__).resolve().parents[2]
DBT_MODELS = RACINE / "electricore" / "ingestion" / "dbt" / "models"
SOURCES_YML = DBT_MODELS / "sources.yml"
MODELES_FLUX_DIR = DBT_MODELS / "flux"

# ---------------------------------------------------------------------------
# Exceptions déclarées (ADR-0053) — pas des trous silencieux.
# ---------------------------------------------------------------------------

# `releves`/`spine_contrat`/`chronologie_releves` sont des MARTS dérivés (unions/CTE
# assemblées en dbt), pas des flux ingérés au sens de flux.yaml — hors périmètre du
# registre des *flux connus*. Ils ont leurs propres factories (`releves()`,
# `spine_contrat()`, `chronologie_releves()`), non couvertes ici par construction.
MARTS_HORS_PERIMETRE = {"releves", "spine_contrat", "chronologie_releves"}

# `flux_r15_acc` est un second modèle dbt issu de `raw_r15` (même flux ingéré, deux
# linéarisations) : le registre loaders est keyé par *flux ingéré* (une entrée `r15`),
# pas par *modèle dbt* — `flux_r15_acc` n'a pas de descripteur dédié.
MODELES_DBT_SANS_DESCRIPTOR_LOADER = {"flux_r15_acc"}


def _sources_yml_raw_tables() -> set[str]:
    sources = yaml.safe_load(SOURCES_YML.read_text())
    (source,) = sources["sources"]
    return {t["name"] for t in source["tables"]}


def _modeles_dbt_existants() -> set[str]:
    return {p.stem for p in MODELES_FLUX_DIR.glob("*.sql")}


def _flux_descriptors() -> dict:
    from electricore.core.loaders.duckdb.registry import FLUX_DESCRIPTORS

    return FLUX_DESCRIPTORS


def _factories_exportees() -> set[str]:
    """Noms des factories exportées par `electricore.core.loaders` (API fluide)."""
    from electricore.core import loaders

    return {nom for nom in loaders.__all__ if nom in _flux_descriptors()}


def _colonne_date_metier_keys() -> set[str]:
    from electricore.api.services.duckdb_service import COLONNE_DATE_METIER

    return set(COLONNE_DATE_METIER)


def _bot_descriptions_keys() -> set[str]:
    from electricore.bot.handlers.flux import DESCRIPTIONS

    return set(DESCRIPTIONS)


def _tables_exposees() -> set[str]:
    """Tables `flux_*` exposées côté API/bot : modèles dbt matérialisés, moins le
    préfixe `flux_` (convention `list_tables()`, `duckdb_service.py`)."""
    return {re.sub(r"^flux_", "", m) for m in _modeles_dbt_existants()}


# ---------------------------------------------------------------------------
# Invariants
# ---------------------------------------------------------------------------


def test_flux_yaml_correspond_au_mapping_raw_du_runner():
    """clés de flux.yaml ↔ mapping raw→modèles du runner, via la convention `raw_<flux>`."""
    attendus = {f"raw_{f}" for f in flux_connus()}
    assert attendus == set(MODELES_PAR_RAW)


def test_sources_yml_correspond_au_mapping_raw_du_runner():
    """tables de sources.yml ↔ mapping raw→modèles du runner."""
    assert _sources_yml_raw_tables() == set(MODELES_PAR_RAW)


def test_chaque_valeur_du_mapping_est_un_modele_dbt_existant():
    modeles_dbt = _modeles_dbt_existants()
    for raw, modeles in MODELES_PAR_RAW.items():
        for modele in modeles:
            assert modele in modeles_dbt, f"{raw} → {modele} : modèle dbt introuvable ({modele}.sql)"


def _assert_chaque_flux_a_un_descripteur(descriptors: dict) -> None:
    """Invariant partagé avec le test de mutation : l'affaiblir ici fait échouer
    `test_mutation_locale_retirer_un_flux_du_registre_loaders_fait_echouer`."""
    for f in flux_connus():
        assert f in descriptors, f"flux '{f}' ingéré (flux.yaml) mais absent de FLUX_DESCRIPTORS"


def test_chaque_flux_ingere_a_un_descripteur_loader():
    """Chaque flux connu doit être requêtable côté cœur (registre loaders)."""
    _assert_chaque_flux_a_un_descripteur(_flux_descriptors())


def test_chaque_descriptor_pointe_un_modele_dbt_existant():
    modeles_dbt = _modeles_dbt_existants()
    for nom, descriptor in _flux_descriptors().items():
        if descriptor.table is None:
            continue  # mart base_sql (hors périmètre flux ingérés)
        table_physique = descriptor.table.rsplit(".", 1)[-1]
        assert table_physique in modeles_dbt, f"descripteur '{nom}' pointe '{table_physique}' (modèle dbt introuvable)"


def test_modeles_dbt_orphelins_de_descriptor_sont_exactement_l_exception_declaree():
    """Tout modèle dbt sans descripteur loader doit être l'exception `r15_acc` connue
    (ADR-0053) — pas une nouvelle lacune de registre silencieuse."""
    modeles_pointes = {d.table.rsplit(".", 1)[-1] for d in _flux_descriptors().values() if d.table is not None}
    orphelins = _modeles_dbt_existants() - modeles_pointes
    assert orphelins == MODELES_DBT_SANS_DESCRIPTOR_LOADER


def test_factories_exportees_correspondent_au_registre_des_descriptors():
    """factories/exports ↔ registre des descripteurs — une factory nommée par flux connu."""
    descriptors = set(_flux_descriptors())
    factories = _factories_exportees()
    assert factories == descriptors, (
        f"écart factories/registre : manquantes={descriptors - factories}, en trop={factories - descriptors}"
    )


def test_marts_hors_perimetre_ont_leur_propre_factory_exportee():
    """Les marts dérivés (ADR-0053, hors du registre des *flux connus*) restent
    requêtables : chacun garde sa propre factory exportée par `core.loaders`."""
    from electricore.core import loaders

    for nom in MARTS_HORS_PERIMETRE:
        assert hasattr(loaders, nom), f"mart '{nom}' déclaré hors périmètre mais sans factory exportée"


def test_couverture_fraicheur_chaque_modele_flux_a_une_colonne_de_date():
    """chaque modèle flux_* a une colonne de date métier (fraîcheur, #537)."""
    colonnes = _colonne_date_metier_keys()
    tables = _tables_exposees()
    manquantes = tables - colonnes
    assert not manquantes, f"tables sans colonne de date métier (COLONNE_DATE_METIER) : {manquantes}"


def test_couverture_libelles_bot_chaque_table_exposee_a_une_description():
    """chaque table exposée dans le menu /flux a une description (#537)."""
    descriptions = _bot_descriptions_keys()
    tables = _tables_exposees()
    manquantes = tables - descriptions
    assert not manquantes, f"tables sans libellé bot (DESCRIPTIONS) : {manquantes}"


def test_mutation_locale_retirer_un_flux_du_registre_loaders_fait_echouer():
    """Sanity du test lui-même (#538, critère d'acceptation) : retirer un flux connu du
    registre loaders fait échouer le VRAI invariant (helper partagé, pas une copie —
    l'affaiblir se voit ici)."""
    descriptors = dict(_flux_descriptors())
    descriptors.pop("c12")
    with pytest.raises(AssertionError):
        _assert_chaque_flux_a_un_descripteur(descriptors)


def test_config_ne_dependend_pas_de_dlt_dans_le_module():
    """`electricore.ingestion.config` (source des flux connus) reste importable sans dlt —
    core doit pouvoir en dériver sans l'extra [ingestion] (mur #2, ADR-0053)."""
    source = Path(flux_connus.__globals__["__file__"]).read_text()
    tree = ast.parse(source)
    imports = {n.name.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.Import) for n in node.names}
    imports |= {
        node.module.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module
    }
    assert "dlt" not in imports
