"""
Modèles Pandera pour Polars DataFrames.

Ces modèles sont adaptés pour fonctionner avec Polars et remplacent 
progressivement les modèles pandas existants.
"""

from .historique_perimetre import HistoriquePérimètre
from .releve_index import RelevéIndex

__all__ = [
    "RelevéIndex", 
    "HistoriquePérimètre"
]