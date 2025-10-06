"""
Pipelines de traitement ElectriCore utilisant Polars.

Ce module contient les implémentations fonctionnelles des pipelines
de traitement de données énergétiques utilisant les expressions Polars.
"""

from .orchestration import (
    ResultatFacturationPolars,
    calculer_historique_enrichi,
    calculer_abonnements,
    calculer_energie,
    facturation
)

__all__ = [
    "ResultatFacturationPolars",
    "calculer_historique_enrichi",
    "calculer_abonnements",
    "calculer_energie",
    "facturation"
]