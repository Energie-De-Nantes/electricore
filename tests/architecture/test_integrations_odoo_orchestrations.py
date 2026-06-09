"""Tests de topologie pour les builds EDN-shaped dans `integrations/odoo/` (issues #39, #40).

Pinnent le contrat :
- chaque build ERP-shaped vit dans son sous-module `integrations.odoo.<domaine>`
- chaque build est callable

Depuis ADR-0019 (issue #108), les builds de taxes (`rapport_accise`, `rapport_cta`)
ont été migrés vers `core/builds/rapport_taxe.py`. `integrations/odoo/taxes.py` n'existe plus.

Voir [ADR-0016](../../docs/adr/0016-core-erp-agnostique.md).
"""

import importlib

import pytest

FACTURATION_ORCHESTRATIONS = (
    "facturation_du_mois",
    "rapport_facturation",
    "documents_facturation_du_mois",
)


@pytest.mark.parametrize("name", FACTURATION_ORCHESTRATIONS)
def test_facturation_orchestrations_live_in_integrations_odoo_facturation(name: str) -> None:
    """#39 : les builds de facturation EDN-shaped vivent dans `integrations.odoo.facturation`."""
    module = importlib.import_module("electricore.integrations.odoo.facturation")
    assert hasattr(module, name), f"{name} absent de electricore.integrations.odoo.facturation"
    assert callable(getattr(module, name))


def test_taxes_builds_live_in_core_builds() -> None:
    """ADR-0019 : `rapport_accise` et `rapport_cta` vivent dans `core.builds.rapport_taxe`."""
    module = importlib.import_module("electricore.core.builds.rapport_taxe")
    for name in ("rapport_accise", "rapport_cta"):
        assert hasattr(module, name), f"{name} absent de electricore.core.builds.rapport_taxe"
        assert callable(getattr(module, name))


def test_taxes_module_removed() -> None:
    """ADR-0019 : `integrations.odoo.taxes` n'existe plus (migré vers core/builds/ + api/services/)."""
    import importlib.util

    spec = importlib.util.find_spec("electricore.integrations.odoo.taxes")
    assert spec is None, "electricore.integrations.odoo.taxes existe encore — supprimer après migration ADR-0019"
