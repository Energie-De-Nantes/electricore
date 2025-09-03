"""
Chargeurs de données pour ElectriCore.

Modules de chargement et validation des données depuis différentes sources.
"""

from .polars_loader import ParquetLoader

__all__ = ["ParquetLoader"]