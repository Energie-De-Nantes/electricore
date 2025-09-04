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


def expr_impacte_abonnement(over: str = "Ref_Situation_Contractuelle") -> pl.Expr:
    """
    Détecte si un changement impacte l'abonnement.
    
    Un changement impacte l'abonnement s'il y a :
    - Un changement de puissance souscrite OU
    - Un changement de formule tarifaire d'acheminement (FTA) OU
    - Un événement structurant (entrée/sortie du périmètre)
    
    Cette expression compose les détections de changement individuelles
    et les événements structurants en suivant la logique métier du calcul 
    de facturation.
    
    Args:
        over: Colonne(s) définissant les partitions pour la window function
        
    Returns:
        Expression Polars retournant un booléen (True si impact détecté)
        
    Example:
        >>> df.with_columns(
        ...     expr_impacte_abonnement().alias("impacte_abonnement")
        ... )
    """
    changement_puissance = expr_changement("Puissance_Souscrite", over)
    changement_fta = expr_changement("Formule_Tarifaire_Acheminement", over)
    est_structurant = expr_evenement_structurant()
    
    return changement_puissance | changement_fta | est_structurant


def expr_evenement_structurant() -> pl.Expr:
    """
    Détecte si un événement est structurant (entrée/sortie du périmètre).
    
    Les événements structurants ont toujours un impact sur la facturation,
    indépendamment des changements de données. Ce sont les événements qui
    modifient la structure même du périmètre contractuel :
    - CFNE : Changement de Fournisseur - Nouveau Entrant
    - MES : Mise En Service
    - PMES : Première Mise En Service
    - CFNS : Changement de Fournisseur - Nouveau Sortant
    - RES : RÉSiliation
    
    Returns:
        Expression Polars retournant True si l'événement est structurant
        
    Example:
        >>> df.with_columns(
        ...     expr_evenement_structurant().alias("evenement_structurant")
        ... )
    """
    evenements_structurants = ["CFNE", "MES", "PMES", "CFNS", "RES"]
    return pl.col("Evenement_Declencheur").is_in(evenements_structurants)