"""Adaptateur Odoo (issue #35, ADR-0016).

Adaptateur ERP pour Odoo. Voir [ADR-0016](../../../docs/adr/0016-core-erp-agnostique.md).
"""

from .config import FieldsCache, OdooConfig
from .helpers import (
    commandes,
    commandes_factures,
    commandes_lignes,
    consommations_mensuelles,
    factures,
    flags_etat_facturation,
    lignes_factures,
    lignes_factures_du_mois,
    partenaires,
    query,
)
from .query import OdooQuery
from .reader import OdooReader
from .transforms import (
    add_computed_columns,
    aggregate_by_period,
    convert_odoo_dates,
    explode_one2many_field,
    expr_calculer_trimestre_facturation,
    filter_active_records,
    normalize_many2one_fields,
)
from .writers import OdooWriter

__all__ = [
    "OdooConfig",
    "FieldsCache",
    "OdooReader",
    "OdooQuery",
    "OdooWriter",
    "query",
    "factures",
    "lignes_factures",
    "commandes",
    "partenaires",
    "commandes_factures",
    "commandes_lignes",
    "consommations_mensuelles",
    "lignes_factures_du_mois",
    "flags_etat_facturation",
    "normalize_many2one_fields",
    "convert_odoo_dates",
    "add_computed_columns",
    "filter_active_records",
    "explode_one2many_field",
    "aggregate_by_period",
    "expr_calculer_trimestre_facturation",
]
