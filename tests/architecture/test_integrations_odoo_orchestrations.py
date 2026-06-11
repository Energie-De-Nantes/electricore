"""Tests de topologie des domaines taxes et facturation (issues #39, #40, #108, #144).

Pinnent le contrat ADR-0019 : l'assemblage des livrables vit en `core/builds/`,
le wire-up (sources ERP + loaders + builds) en `api/services/`, et
`integrations/odoo/` ne garde que les sources/sinks.

Historique des migrations :
- taxes (issue #108) : `integrations/odoo/taxes.py` → `core/builds/rapport_taxe.py`
  + `api/services/taxes_service.py` ;
- facturation (issues #142→#144) : `integrations/odoo/facturation.py` →
  `core/builds/rapport_facturation.py` + `api/services/facturation_service.py`.

Voir [ADR-0016](../../docs/adr/0016-core-erp-agnostique.md) et
[ADR-0019](../../docs/adr/0019-roles-loaders-pipelines-builds-integrations.md).
"""

import importlib
import importlib.util

import pytest

FACTURATION_WIREUPS = (
    "facturation_du_mois",
    "rapport_facturation",
    "documents_facturation_du_mois",
)


@pytest.mark.parametrize("name", FACTURATION_WIREUPS)
def test_facturation_wireups_live_in_api_services(name: str) -> None:
    """#144 : les wire-ups facturation vivent dans `api.services.facturation_service`."""
    module = importlib.import_module("electricore.api.services.facturation_service")
    assert hasattr(module, name), f"{name} absent de electricore.api.services.facturation_service"
    assert callable(getattr(module, name))


def test_facturation_build_lives_in_core_builds() -> None:
    """#143 : l'assemblage du livrable facturation vit dans `core.builds.rapport_facturation`."""
    module = importlib.import_module("electricore.core.builds.rapport_facturation")
    for name in ("rapport_facturation", "feuilles_rapport_facturation", "RapportFacturation"):
        assert hasattr(module, name), f"{name} absent de electricore.core.builds.rapport_facturation"


def test_facturation_module_removed() -> None:
    """#144 : `integrations.odoo.facturation` n'existe plus (assemblage en core, wire-up en services)."""
    spec = importlib.util.find_spec("electricore.integrations.odoo.facturation")
    assert spec is None, "electricore.integrations.odoo.facturation existe encore — supprimer après migration #144"


def test_taxes_builds_live_in_core_builds() -> None:
    """ADR-0019 : `rapport_accise` et `rapport_cta` vivent dans `core.builds.rapport_taxe`."""
    module = importlib.import_module("electricore.core.builds.rapport_taxe")
    for name in ("rapport_accise", "rapport_cta"):
        assert hasattr(module, name), f"{name} absent de electricore.core.builds.rapport_taxe"
        assert callable(getattr(module, name))


def test_taxes_module_removed() -> None:
    """ADR-0019 : `integrations.odoo.taxes` n'existe plus (migré vers core/builds/ + api/services/)."""
    spec = importlib.util.find_spec("electricore.integrations.odoo.taxes")
    assert spec is None, "electricore.integrations.odoo.taxes existe encore — supprimer après migration ADR-0019"
