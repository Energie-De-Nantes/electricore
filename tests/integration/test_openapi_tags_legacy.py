"""Invariant tag OpenAPI `legacy` ⟺ dépendance au vieil Odoo (issue #584).

Le tag `legacy` doit couvrir exactement les endpoints couplés au vieil Odoo
(lecture XML-RPC) : les endpoints déclarés `binary_endpoint(requires_odoo=True)`
plus `/facturation/check/odoo`, qui porte une garde Odoo **inline** (pas le
marqueur `requires_odoo` posé par `binary_endpoint`) — piège connu, couvert
explicitement ici plutôt que par introspection.
"""

from electricore.api.main import app

ENDPOINTS_LEGACY_A_GARDE_INLINE = {"/facturation/check/odoo"}


def _routes_taguees_legacy() -> set[str]:
    return {route.path for route in app.routes if "legacy" in getattr(route, "tags", [])}


def _routes_marquees_requires_odoo() -> set[str]:
    return {route.path for route in app.routes if getattr(getattr(route, "endpoint", None), "requires_odoo", False)}


def test_tag_legacy_correspond_exactement_aux_endpoints_couples_au_vieil_odoo():
    attendu = _routes_marquees_requires_odoo() | ENDPOINTS_LEGACY_A_GARDE_INLINE
    assert _routes_taguees_legacy() == attendu


def test_tag_legacy_compte_douze_endpoints():
    """Les 12 endpoints listés par l'issue #584 (6 `/facturation/*` + 6 `/taxes/*`)."""
    assert len(_routes_taguees_legacy()) == 12
