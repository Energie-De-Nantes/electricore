"""
Pipelines de traitement ElectriCore utilisant Polars.

Ce module contient les implémentations fonctionnelles des pipelines
de traitement de données énergétiques utilisant les expressions Polars.
"""

from .orchestration_polars import (
    ResultatFacturationPolars,
    calculer_historique_enrichi_polars,
    calculer_abonnements_polars,
    calculer_energie_polars,
    facturation_polars
)

__all__ = [
    "ResultatFacturationPolars",
    "calculer_historique_enrichi_polars",
    "calculer_abonnements_polars",
    "calculer_energie_polars",
    "facturation_polars"
]