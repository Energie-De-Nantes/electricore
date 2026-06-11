"""Adaptateur Odoo (issue #35, ADR-0016).

Adaptateur ERP pour Odoo. Voir [ADR-0016](../../../docs/adr/0016-core-erp-agnostique.md).
"""

from .config import FieldsCache, OdooConfig
from .helpers import (
    commandes,
    commandes_lignes,
    lignes_factures,
    query,
)
from .liens import enrichir_liens, url_pour_enregistrement
from .query import OdooQuery
from .reader import OdooReader
from .sources import lignes_factures_du_mois
from .verification import ResultatVerification, verifier
from .writers import OdooWriter

__all__ = [
    "OdooConfig",
    "FieldsCache",
    "OdooReader",
    "OdooQuery",
    "OdooWriter",
    "query",
    "lignes_factures",
    "commandes",
    "commandes_lignes",
    "lignes_factures_du_mois",
    "verifier",
    "ResultatVerification",
    "url_pour_enregistrement",
    "enrichir_liens",
]
