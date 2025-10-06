"""
Writers pour l'écriture et l'export de données vers des systèmes externes.

Ce module contient les connecteurs en écriture pour exporter les données
traitées vers des systèmes externes (Odoo, CSV, bases de données, etc.).
"""

from .odoo import OdooWriter

__all__ = ["OdooWriter"]