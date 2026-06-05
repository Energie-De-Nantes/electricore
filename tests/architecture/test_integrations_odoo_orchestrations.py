"""Tests de topologie pour les orchestrations EDN-shaped déplacées vers `integrations/odoo/` (issues #39, #40).

Pinnent le contrat :
- chaque orchestration vit dans son sous-module `integrations.odoo.<domaine>`
- chaque orchestration est callable

Voir [ADR-0016](../../docs/adr/0016-core-erp-agnostique.md).
"""

import importlib

import pytest

FACTURATION_ORCHESTRATIONS = ("facturation_du_mois", "documents_facturation_du_mois")


@pytest.mark.parametrize("name", FACTURATION_ORCHESTRATIONS)
def test_facturation_orchestrations_live_in_integrations_odoo_facturation(name: str) -> None:
    """#39 : les orchestrations de facturation EDN-shaped vivent dans `integrations.odoo.facturation`."""
    module = importlib.import_module("electricore.integrations.odoo.facturation")
    assert hasattr(module, name), f"{name} absent de electricore.integrations.odoo.facturation"
    assert callable(getattr(module, name))
