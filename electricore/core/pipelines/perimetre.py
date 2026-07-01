"""Domaine périmètre : la *présence au périmètre*, dérivée du flux C15 (ADR-0052).

Un **span de présence** = un passage d'une RSC (`ref_situation_contractuelle`) dans le
périmètre : `[debut, fin)` en **jour civil**, un par RSC. `debut` = jour du 1er événement
de la RSC (l'entrée) ; `fin` = jour du **code sortie** (`RES`/`CFNS`), ou `null` si encore
présente. Prouvé sur données réelles (spike du 2026-07-01) : une RSC est une timeline
monotone entrée → (modifs) → sortie, **jamais rouverte** ; les ré-entrées sont de *nouvelles*
RSC. Le flag `etat_contractuel` (EN SERVICE/RESILIE) concorde à 100 % avec les codes — gardé
en **invariant** (cf. ADR-0052), les codes portent la date de `fin` (événement unique).

Pur, ERP-agnostique, jamais matérialisé (comme `affaires_ouvertes`). « Actif à une date »
prend la date en **paramètre explicite** (l'horloge est injectée au boundary, cf. pureté #179).
"""

from datetime import date

import polars as pl

# Codes de sortie C15 inlinés : un pipeline n'importe pas les loaders (pureté ADR-0019,
# comme historique.py inline ses événements structurants). Miroir de `SORTIES_C15` du
# registre loader (`core/loaders/duckdb/registry.py`).
_SORTIES_C15 = ("RES", "CFNS")


def presence_perimetre(c15: pl.LazyFrame | pl.DataFrame) -> pl.DataFrame:
    """Spans de présence au périmètre, **un par RSC** — `[debut, fin)` en jour civil.

    Args:
        c15: événements `flux_c15` (au moins `pdl`, `ref_situation_contractuelle`,
            `date_evenement`, `evenement_declencheur`).

    Returns:
        Une ligne par `ref_situation_contractuelle` : `pdl`, `debut` (jour du 1er événement),
        `fin` (jour du code sortie `RES`/`CFNS`, ou `null` si encore présente). Trié par RSC.
    """
    return (
        c15.lazy()
        .filter(pl.col("ref_situation_contractuelle").is_not_null())
        .group_by("ref_situation_contractuelle")
        .agg(
            pl.col("pdl").first(),  # constant par RSC
            pl.col("date_evenement").min().dt.date().alias("debut"),
            # fin = jour du code sortie (événement unique et net) ; null si pas de sortie.
            pl.col("date_evenement")
            .filter(pl.col("evenement_declencheur").is_in(_SORTIES_C15))
            .min()
            .dt.date()
            .alias("fin"),
        )
        .sort("ref_situation_contractuelle")
        .collect()
    )


def pdls_actifs_a(c15: pl.LazyFrame | pl.DataFrame, a_date: date) -> pl.DataFrame:
    """PDL présents dans le périmètre au jour `a_date` — `[debut, fin)` demi-ouvert.

    Un PDL est actif s'il porte au moins une RSC dont le span contient `a_date`
    (`debut <= a_date` et `fin` nulle ou `a_date < fin`). Dédoublonné en PDL distincts
    (un PDL multi-RSC — ré-entrée, changement de fournisseur — reste une seule ligne).

    Args:
        c15: événements `flux_c15`.
        a_date: jour civil de référence (injecté au boundary — l'aujourd'hui n'est pas
            calculé ici, cf. `affaires_ouvertes(maintenant=...)`).

    Returns:
        Une colonne `pdl`, distincte et triée.
    """
    return (
        presence_perimetre(c15)
        .filter((pl.col("debut") <= a_date) & (pl.col("fin").is_null() | (pl.col("fin") > a_date)))
        .select("pdl")
        .unique()
        .sort("pdl")
    )
