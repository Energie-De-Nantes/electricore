"""Sorties du périmètre par RSC, **sans état** (#632, ADR-0052).

`POST /perimetre/sorties` sert la fin de souscription gouvernée par le fait C15
(souscriptions_odoo#21, ADR 0031 côté addon) : à chaque campagne, Odoo interroge un lot
de `ref_situation_contractuelle` et ne reçoit que celles qui sont **sorties** du
périmètre — événement C15 de code `{RES, CFNS}`, symétrique pour le fournisseur. Une RSC
encore présente ou inconnue du fournisseur n'apparaît simplement pas dans la réponse
(pas d'erreur : c'est le cas nominal « pas sortie »).

ERP-agnostique ([ADR-0016](docs/adr/0016-core-erp-agnostique.md)) : le filtre RSC vient
du consommateur (les périmètres Enedis peuvent être partagés entre entités,
indistinguables dans les flux — l'autorité du « à nous » est la souscription Odoo).
"""

from __future__ import annotations

import polars as pl

# Version du contrat exposée dans l'enveloppe (cf. rsc_service / turpe-variable). Bump
# sur rupture ; un ajout de champ optionnel reste additif et ne change pas la version.
CONTRAT_VERSION = 1


def lister_sorties(c15_sorties: pl.DataFrame) -> list[dict]:
    """Une ligne par RSC sortie, projetée depuis des événements C15 déjà filtrés.

    Args:
        c15_sorties: événements `flux_c15` déjà filtrés aux codes de sortie canoniques
            (`RES`/`CFNS`, cf. `c15().sorties()`) et aux RSC demandées — colonnes
            `ref_situation_contractuelle`, `pdl`, `evenement_declencheur`, `date_evenement`
            (instant `TIMESTAMPTZ`, ADR-0042).

    Returns:
        Une ligne `{ref_situation_contractuelle, pdl, evenement_declencheur,
        date_sortie}` par RSC, triée par RSC. `date_sortie` est le jour civil (jour du
        code sortie) — convention demi-ouverte (ADR-0042/0052) : la RSC est absente du
        périmètre dès ce jour, la conversion en « dernier jour servi » appartient au
        consommateur. Dédupliquée par RSC (une sortie au plus par RSC au spike
        ADR-0052 ; défense contre un doublon de ré-ingestion).
    """
    if c15_sorties.height == 0:
        return []
    return (
        c15_sorties.lazy()
        .group_by("ref_situation_contractuelle")
        .agg(
            pl.col("pdl").first(),
            pl.col("evenement_declencheur").sort_by("date_evenement").first(),
            pl.col("date_evenement").min().dt.date().alias("date_sortie"),
        )
        .sort("ref_situation_contractuelle")
        .collect()
        .to_dicts()
    )
