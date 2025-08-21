"""
Utilitaires de formatage pour les données ElectriCore.

Ce module fournit des fonctions pures pour le formatage de dates,
montants et autres données dans les pipelines de transformation.
"""

import pandas as pd
from babel.dates import format_date


def formater_date_francais(date, format: str = "d MMMM yyyy") -> str | None:
    """
    Formate une date en français selon le format spécifié.
    
    Args:
        date: Date à formater (datetime, pd.Timestamp, etc.)
        format: Format Babel pour la date (défaut: "d MMMM yyyy")
        
    Returns:
        Date formatée en français ou None si la date est manquante
        
    Examples:
        >>> formater_date_francais(pd.Timestamp('2025-03-15'))
        '15 mars 2025'
        >>> formater_date_francais(pd.Timestamp('2025-03-15'), "LLLL yyyy")
        'mars 2025'
    """
    if pd.isna(date):
        return None
    return format_date(date, format, locale="fr_FR")