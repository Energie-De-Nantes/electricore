"""
Sélection du taux en vigueur à une date donnée.

Module profond qui encapsule la mécanique « étant donné une date et un
historique de taux par date d'entrée en vigueur, retourner le taux applicable ».

Consommé par les pipelines de taxes (Accise, CTA, …) via des adaptateurs qui
configurent uniquement le nom de la colonne de taux et la colonne de date.
"""

import polars as pl

from electricore.core.models.historique_taux import HistoriqueTaux


def ajouter_taux_en_vigueur(
    df: pl.LazyFrame,
    historique: pl.LazyFrame,
    *,
    date_col: str,
    taux_col: str,
) -> pl.LazyFrame:
    """
    Attache à chaque ligne de `df` le taux en vigueur à `df[date_col]`.

    Sémantique de `historique` : chaque ligne représente l'entrée en vigueur
    d'un nouveau taux à `start`, qui remplace le précédent jusqu'à la ligne
    suivante (la dernière s'étend indéfiniment).
    """
    HistoriqueTaux.to_schema(taux_col).validate(historique)

    joint = (
        df.sort(date_col)
        .join_asof(
            historique.sort("start"),
            left_on=date_col,
            right_on="start",
            strategy="backward",
        )
        .drop("start")
    )

    nb_orphelines = joint.filter(pl.col(taux_col).is_null()).select(pl.len()).collect().item()
    if nb_orphelines > 0:
        raise ValueError(
            f"{nb_orphelines} ligne(s) sans taux en vigueur — date(s) antérieure(s) au premier start de l'historique"
        )

    return joint
