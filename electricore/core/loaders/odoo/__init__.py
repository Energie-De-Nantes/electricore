"""
Chargeur Odoo pour ElectriCore - Architecture fonctionnelle.

Ce module fournit une API fluide et fonctionnelle pour charger les données
depuis Odoo vers Polars avec query builder immutable.

Architecture :
- config.py : Configuration et cache (immutable)
- reader.py : OdooReader (connexion XML-RPC + métadonnées)
- query.py : OdooQuery (builder immutable avec follow/enrich)
- helpers.py : Fonctions helpers pour modèles courants
- transforms.py : Transformations Polars composables

API publique :
- OdooReader, OdooQuery : Classes principales
- query(), factures(), commandes(), partenaires() : Fonctions helpers
- OdooConfig : Configuration immutable
"""

# Imports internes
from .config import OdooConfig, FieldsCache
from .reader import OdooReader
from .query import OdooQuery
from .helpers import (
    query,
    factures,
    lignes_factures,
    commandes,
    partenaires,
    # Helpers avec navigation multi-niveaux
    commandes_factures,
    commandes_lignes
)
from .transforms import (
    normalize_many2one_fields,
    convert_odoo_dates,
    add_computed_columns,
    filter_active_records,
    explode_one2many_field,
    aggregate_by_period,
    expr_calculer_trimestre_facturation
)


# =============================================================================
# EXPORTS PUBLICS
# =============================================================================

__all__ = [
    # Configuration
    'OdooConfig',
    'FieldsCache',
    # Connecteur et query builder
    'OdooReader',
    'OdooQuery',
    # API fonctionnelle - Helpers simples
    'query',
    'factures',
    'lignes_factures',
    'commandes',
    'partenaires',
    # API fonctionnelle - Helpers avec navigation multi-niveaux
    'commandes_factures',
    'commandes_lignes',
    # Transformations
    'normalize_many2one_fields',
    'convert_odoo_dates',
    'add_computed_columns',
    'filter_active_records',
    'explode_one2many_field',
    'aggregate_by_period',
    'expr_calculer_trimestre_facturation',
]