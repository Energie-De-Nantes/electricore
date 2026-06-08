"""Contexte mensuel de facturation : composition pure pour un mois donné.

Voir `core/CONTEXT.md` (entrée *Contexte mensuel de facturation*).

`charger()` prend `historique` et `relevés` en LazyFrame — les loaders restent
à la charge de l'appelant (adapter ERP, router API). Le module ne déclenche
aucune I/O.
"""

from dataclasses import dataclass

import polars as pl

from electricore.core.pipelines.orchestration import facturation


@dataclass(frozen=True)
class ContexteMensuel:
    """Bundle immutable des 4 frames dérivés pour produire la facturation d'un mois.

    Voir `core/CONTEXT.md` (entrée *Contexte mensuel de facturation*).
    """

    mois: str
    historique_enrichi: pl.LazyFrame
    abonnements: pl.LazyFrame
    energie: pl.LazyFrame
    facturation_mensuelle: pl.DataFrame


def charger(
    historique: pl.LazyFrame,
    releves: pl.LazyFrame,
    mois: str | None = None,
) -> ContexteMensuel:
    """Compose les pipelines de facturation et résout le mois cible.

    Args:
        historique: événements C15 (sortie de `c15().lazy()` côté DuckDB).
        releves: relevés harmonisés (sortie de `releves_harmonises().lazy()`).
        mois: format `YYYY-MM-DD` (premier jour du mois). `None` → dernier mois
            disponible dans `facturation_mensuelle`.
    """
    result = facturation(historique=historique, releves=releves)

    if mois is None:
        debut_mois_expr = pl.col("debut").dt.truncate("1mo").dt.date()
        mois = str(result.facturation.select(debut_mois_expr.alias("m"))["m"].max())

    return ContexteMensuel(
        mois=mois,
        historique_enrichi=result.historique_enrichi,
        abonnements=result.abonnements,
        energie=result.energie,
        facturation_mensuelle=result.facturation,
    )
