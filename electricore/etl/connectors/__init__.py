"""
Module des connecteurs externes pour l'ETL.

Ce module contient les connecteurs vers des systèmes externes comme Odoo,
permettant l'extraction et l'écriture de données.
"""

from .odoo import OdooReader, OdooWriter

__all__ = ["OdooReader", "OdooWriter"]