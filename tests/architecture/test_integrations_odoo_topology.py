"""Tests de topologie pour le déplacement des adaptateurs Odoo (issue #35, ADR-0016).

Pinnent le contrat de l'arborescence cible :
- A : la nouvelle surface publique est importable depuis `electricore.integrations.odoo`
- B : les symboles vivent physiquement à `electricore.integrations.odoo.*`
- C : les anciens chemins `electricore.core.loaders.odoo` / `core.writers.odoo` /
      `core.models.odoo` ne sont plus exportés ni importables
"""

import importlib

import pytest

TYPES = ("OdooReader", "OdooQuery", "OdooWriter", "ResultatVerification")

HELPERS = (
    "query",
    "commandes_lignes",
    "lignes_factures_du_mois",
    "verifier",
    "url_pour_enregistrement",
    "enrichir_liens",
)


@pytest.mark.parametrize("name", TYPES)
def test_types_importable_from_integrations_odoo(name: str) -> None:
    """A : chaque type public (config + classes principales) est exposé par `integrations.odoo`."""
    module = importlib.import_module("electricore.integrations.odoo")
    assert hasattr(module, name), f"{name} absent de electricore.integrations.odoo"
    assert getattr(module, name) is not None


@pytest.mark.parametrize("name", HELPERS)
def test_helpers_importable_from_integrations_odoo(name: str) -> None:
    """A : chaque helper d'accès Odoo est exposé par `integrations.odoo`."""
    module = importlib.import_module("electricore.integrations.odoo")
    assert hasattr(module, name), f"{name} absent de electricore.integrations.odoo"
    assert callable(getattr(module, name))


@pytest.mark.parametrize("name", TYPES + HELPERS)
def test_surface_lives_physically_in_integrations_odoo(name: str) -> None:
    """B : chaque symbole exposé par `integrations.odoo` vit *physiquement* dans ce sous-package."""
    module = importlib.import_module("electricore.integrations.odoo")
    symbol = getattr(module, name)
    assert symbol.__module__.startswith("electricore.integrations.odoo"), (
        f"{name} vit toujours en {symbol.__module__} (devrait commencer par electricore.integrations.odoo)"
    )


OLD_ODOO_PATHS = (
    "electricore.core.loaders.odoo",
    "electricore.core.writers.odoo",
    "electricore.core.models.odoo",
    "electricore.integrations.odoo.models",  # supprimé en #508 : 3 schémas Pandera jamais .validate()-és
)


@pytest.mark.parametrize("module_path", OLD_ODOO_PATHS)
def test_old_odoo_path_removed(module_path: str) -> None:
    """C : les anciens chemins Odoo dans `core/` n'existent plus (ADR-0016)."""
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module(module_path)


@pytest.mark.parametrize("name", TYPES + HELPERS)
def test_loaders_root_no_longer_exposes_odoo(name: str) -> None:
    """C : `electricore.core.loaders` n'expose plus aucun symbole Odoo (ADR-0016)."""
    module = importlib.import_module("electricore.core.loaders")
    assert not hasattr(module, name), f"electricore.core.loaders.{name} encore exposé — devrait être retiré (ADR-0016)"


def test_core_writers_no_longer_exposes_odoo_writer() -> None:
    """C : `electricore.core.writers` n'expose plus `OdooWriter`."""
    module = importlib.import_module("electricore.core.writers")
    assert not hasattr(module, "OdooWriter")


# Helpers supprimés en #85 : 4 helpers à 0 caller retirés pour amincir la surface publique.
# - `factures`, `partenaires`, `commandes_factures` : 0 caller (presets shallow et navigation dead code)
# - `consommations_mensuelles` : était un alias explicite de `commandes_lignes`
# Supprimés en #507 : 0 caller prod
# - `lignes_factures`, `commandes` : presets morts (live helpers : `query` + `commandes_lignes`)
# - `OdooConfig`, `FieldsCache` : double-clé ODOO_URL/ODOO_DB jamais fournie en prod
REMOVED_HELPERS = (
    "factures",
    "partenaires",
    "consommations_mensuelles",
    "commandes_factures",
    "lignes_factures",
    "commandes",
    "OdooConfig",
    "FieldsCache",
)


@pytest.mark.parametrize("name", REMOVED_HELPERS)
def test_removed_helper_no_longer_exposed(name: str) -> None:
    """D : les helpers retirés en #85 ne sont plus importables depuis `integrations.odoo`."""
    module = importlib.import_module("electricore.integrations.odoo")
    assert not hasattr(module, name), (
        f"{name} reste exposé par electricore.integrations.odoo — devrait avoir été retiré (#85)"
    )
