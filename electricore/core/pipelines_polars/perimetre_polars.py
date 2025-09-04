"""
Expressions Polars pour le pipeline périmètre.

Ce module contient des expressions composables suivant la philosophie
fonctionnelle de Polars. Les expressions sont des transformations pures 
qui peuvent être composées entre elles.
"""

import polars as pl


def expr_changement(col_name: str, over: str = "Ref_Situation_Contractuelle") -> pl.Expr:
    """
    Détecte si une colonne a changé par rapport à sa valeur précédente dans la partition.
    
    Cette expression compare la valeur actuelle avec la valeur précédente
    (obtenue via shift) au sein d'une partition définie par 'over'.
    
    Seuls les changements entre deux valeurs non-nulles sont détectés,
    conformément à la logique de l'implémentation pandas existante.
    
    Args:
        col_name: Nom de la colonne à analyser
        over: Colonne(s) définissant les partitions pour la window function
        
    Returns:
        Expression Polars retournant un booléen (True si changement détecté)
        
    Example:
        >>> df.with_columns(
        ...     expr_changement("Puissance_Souscrite").alias("puissance_change")
        ... )
    """
    current = pl.col(col_name)
    previous = current.shift(1).over(over)
    
    return (
        pl.when(previous.is_not_null() & current.is_not_null())
        .then(previous != current)
        .otherwise(False)
    )


def expr_resume_changement(col_name: str, label: str, over: str = "Ref_Situation_Contractuelle") -> pl.Expr:
    """
    Génère un texte résumant le changement d'une colonne.
    
    Cette expression compose expr_changement pour détecter les changements
    et génère un texte formaté "label: valeur_avant → valeur_après" quand
    un changement est détecté.
    
    Args:
        col_name: Nom de la colonne à analyser
        label: Préfixe à utiliser dans le résumé (ex: "P" pour Puissance)
        over: Colonne(s) définissant les partitions pour la window function
        
    Returns:
        Expression Polars retournant une chaîne de caractères (vide si pas de changement)
        
    Example:
        >>> df.with_columns(
        ...     expr_resume_changement("Puissance_Souscrite", "P").alias("resume_puissance")
        ... )
        # Produit des valeurs comme "P: 6.0 → 9.0" ou ""
    """
    current = pl.col(col_name)
    previous = current.shift(1).over(over)
    
    return (
        pl.when(expr_changement(col_name, over))  # Composition avec expr_changement
        .then(
            pl.concat_str([
                pl.lit(f"{label}: "),
                previous.cast(pl.Utf8),
                pl.lit(" → "),
                current.cast(pl.Utf8)
            ])
        )
        .otherwise(pl.lit(""))
    )