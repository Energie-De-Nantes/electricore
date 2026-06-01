"""
Modèles Pandera pour validation de données Odoo.

Ce module fournit des schémas de validation pour les principales
entités Odoo (factures, commandes, partenaires).
"""

from .commande import CommandeVenteOdoo
from .facture import FactureOdoo, LigneFactureOdoo

__all__ = [
    'FactureOdoo',
    'LigneFactureOdoo',
    'CommandeVenteOdoo',
]