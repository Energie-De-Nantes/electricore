"""
Fonctions de chargement Parquet avec validation Polars/Pandera.

Ce module fournit des fonctions simples pour charger les données parquet exportées
depuis les flux Enedis et les valider avec les modèles Polars.
"""

import polars as pl
from pathlib import Path
from typing import Union
from electricore.core.models.releve_index import RelevéIndex
from electricore.core.models.historique_perimetre import HistoriquePérimètre


def charger_releves(path: Union[str, Path], valider: bool = True) -> pl.DataFrame:
    """
    Charge et valide un fichier parquet de relevés d'index.
    
    Args:
        path: Chemin vers le fichier parquet des relevés
        valider: Active la validation Pandera (défaut: True)
        
    Returns:
        DataFrame Polars (validé si demandé)
        
    Raises:
        FileNotFoundError: Si le fichier n'existe pas
        pandera.errors.SchemaError: Si la validation échoue
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Le fichier parquet n'existe pas : {path}")

    # Charger avec Polars pour les performances
    df = pl.read_parquet(path)
    
    # Nettoyer les colonnes d'index pandas si présentes
    if "__index_level_0__" in df.columns:
        df = df.drop("__index_level_0__")
    
    # Ajouter les colonnes manquantes avec des valeurs par défaut
    if "ordre_index" not in df.columns:
        df = df.with_columns(pl.lit(False).alias("ordre_index"))
    if "Unité" not in df.columns:
        df = df.with_columns(pl.lit("kWh").alias("Unité"))
    if "Précision" not in df.columns:
        df = df.with_columns(pl.lit("kWh").alias("Précision"))

    # Validation avec Pandera si demandée
    if valider:
        df = RelevéIndex.validate(df)

    return df


def charger_historique(path: Union[str, Path], valider: bool = True) -> pl.DataFrame:
    """
    Charge et valide un fichier parquet d'historique de périmètre.
    
    Args:
        path: Chemin vers le fichier parquet de l'historique
        valider: Active la validation Pandera (défaut: True)
        
    Returns:
        DataFrame Polars (validé si demandé)
        
    Raises:
        FileNotFoundError: Si le fichier n'existe pas
        pandera.errors.SchemaError: Si la validation échoue
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Le fichier parquet n'existe pas : {path}")

    # Charger avec Polars
    df = pl.read_parquet(path)
    
    # Nettoyer les colonnes d'index pandas si présentes
    index_columns = [col for col in df.columns if col.startswith("__index_level_")]
    if index_columns:
        df = df.drop(index_columns)

    # Validation avec Pandera si demandée
    if valider:
        df = HistoriquePérimètre.validate(df)

    return df