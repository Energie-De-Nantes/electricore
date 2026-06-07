"""Contexte mensuel de facturation — éléments dérivés une seule fois pour un mois.

`charger_contexte_facturation` centralise la séquence répétée par
plusieurs orchestrations (`facturation_du_mois`, `cta_par_contrat` côté
`integrations.odoo`) : charger l'historique et les relevés harmonisés,
déclencher le pipeline `facturation()`, et résoudre le mois cible quand
il n'est pas fourni.

Voir issue #17.
"""

from dataclasses import dataclass

import polars as pl

from electricore.core.loaders import c15, releves_harmonises
from electricore.core.pipelines.orchestration import facturation


@dataclass(frozen=True)
class ContexteFacturation:
    """Bundle immutable des éléments dérivés pour un mois de facturation."""

    mois: str
    historique_enrichi: pl.LazyFrame
    facturation_mensuelle: pl.DataFrame


def charger_contexte_facturation(mois: str | None) -> ContexteFacturation:
    """Charge l'historique et les relevés, lance `facturation()`, range le résultat.

    Args:
        mois: format "YYYY-MM-DD" (premier jour du mois). `None` à compléter.

    Returns:
        `ContexteFacturation` consommable par les services de facturation
        et de taxes mensuelles.
    """
    hist_enrichi, _, _, fact = facturation(historique=c15().lazy(), releves=releves_harmonises().lazy())

    if mois is None:
        debut_mois_expr = pl.col("debut").dt.truncate("1mo").dt.date()
        mois = str(fact.select(debut_mois_expr.alias("m"))["m"].max())

    return ContexteFacturation(mois=mois, historique_enrichi=hist_enrichi, facturation_mensuelle=fact)
