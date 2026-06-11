"""
Expressions partagées entre les pipelines de périodes (abonnements, énergie, facturation).

Ce module porte les *formules* transverses aux périodes — pas les découpages :
les périodisations abonnement et énergie restent séparées, chacune sur sa
propre ligne de temps (cf. ADR-0023). Les filtres de validité restent locaux
à chaque pipeline (duals miroir, conséquence directe de cette séparation).
"""

import polars as pl


def expr_nb_jours() -> pl.Expr:
    """
    Calcule le nombre de jours entre les bornes `debut` et `fin` d'une période.

    La formule canonique — différence des dates civiles, heures normalisées —
    partagée par tous les pipelines de périodes (issue #178). Le check Pandera
    `verifier_nb_jours_coherent` du modèle garde sa propre copie : c'est un
    contrat qui valide le pipeline, l'indépendance est voulue.

    Returns:
        Expression Polars retournant le nombre de jours (Int32, non aliasée)

    Example:
        >>> df.with_columns(expr_nb_jours().alias("nb_jours"))
    """
    return (pl.col("fin").dt.date() - pl.col("debut").dt.date()).dt.total_days().cast(pl.Int32)


def expr_mois_annee() -> pl.Expr:
    """
    Clé calculable du mois d'une période : `"YYYY-MM"` depuis `debut` (issue #115).

    Triable chronologiquement, garantie `str_matches` dans les schémas Pandera.
    Les libellés français restent portés par `debut_lisible` / `fin_lisible`.

    Returns:
        Expression Polars aliasée `mois_annee`

    Example:
        >>> df.with_columns(expr_mois_annee())
    """
    return pl.col("debut").dt.strftime("%Y-%m").alias("mois_annee")


def expr_date_formatee_fr(col: str) -> pl.Expr:
    """
    Formate une colonne de date en libellé français « 01 mars 2025 ».

    Réservé aux champs d'affichage (`debut_lisible`, `fin_lisible`) — les clés
    de calcul utilisent le format `YYYY-MM` (issue #115). Copie unique partagée
    par les pipelines abonnements, énergie et facturation (issue #178).

    Args:
        col: Nom de la colonne à formater

    Returns:
        Expression Polars retournant la date formatée

    Example:
        >>> df.with_columns(expr_date_formatee_fr("debut").alias("debut_lisible"))
    """
    # Dictionnaire de correspondance anglais -> français
    mois_mapping = {
        "January": "janvier",
        "February": "février",
        "March": "mars",
        "April": "avril",
        "May": "mai",
        "June": "juin",
        "July": "juillet",
        "August": "août",
        "September": "septembre",
        "October": "octobre",
        "November": "novembre",
        "December": "décembre",
    }

    expr = pl.col(col).dt.strftime("%d %B %Y")
    for en_mois, fr_mois in mois_mapping.items():
        expr = expr.str.replace_all(en_mois, fr_mois)
    return expr


def expr_fin_lisible() -> pl.Expr:
    """
    Formate la date de fin avec gestion du cas « en cours ».

    Cette expression formate la fin en français ou retourne « en cours »
    si la date de fin est nulle (période ouverte).

    Returns:
        Expression Polars retournant la fin formatée

    Example:
        >>> df.with_columns(expr_fin_lisible().alias("fin_lisible"))
    """
    return pl.when(pl.col("fin").is_null()).then(pl.lit("en cours")).otherwise(expr_date_formatee_fr("fin"))


def exprs_meta_periode() -> list[pl.Expr]:
    """
    Bundle canonique des méta-colonnes d'une période : `nb_jours`,
    `debut_lisible`, `fin_lisible`, `mois_annee`.

    C'est le *contrat d'assemblage* partagé par les périodisations
    (abonnements, énergie) : les quatre colonnes ne dérivent que des bornes
    `debut`/`fin`, le bundle s'applique donc dès que les bornes existent,
    avant tout filtre de validité. `fin_lisible` passe par
    `expr_fin_lisible` : une fin nulle (période ouverte) s'affiche
    « en cours » au lieu de null.

    Returns:
        Liste d'expressions Polars aliasées, à éclater dans un with_columns

    Example:
        >>> df.with_columns(exprs_meta_periode())
    """
    return [
        expr_nb_jours().alias("nb_jours"),
        expr_date_formatee_fr("debut").alias("debut_lisible"),
        expr_fin_lisible().alias("fin_lisible"),
        expr_mois_annee(),
    ]
