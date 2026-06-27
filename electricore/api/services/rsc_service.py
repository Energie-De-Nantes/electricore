"""Résolution RSC **sans état** : `id_Affaire` → `ref_situation_contractuelle` (#282, #5).

`POST /facturation/rsc` recoupe deux flux Enedis pour le pull `souscriptions_odoo`
(réconciliation par `id_Affaire`, ADR-0010) :

- **C15** porte sur son événement déclencheur l'`id_affaire` **et** le
  `ref_situation_contractuelle` — même identifiant que l'affaire X12 (cf.
  `core/CONTEXT.md`). La résolution est donc un **match exact** sur `id_affaire`, lu sur
  l'événement natif (pas la valeur forward-fillée du mart `spine_contrat`).
- **X12** (`flux_affaires`) sert au **recoupement** : il distingue une affaire *connue
  mais sans situation contractuelle* (précurseur en cours, ou affaire non contractuelle
  type AME) d'une affaire *inconnue*.

ERP-agnostique ([ADR-0016](docs/adr/0016-core-erp-agnostique.md)) : aucun import
`integrations.odoo` ; l'`id_affaire` est ré-émis tel quel à côté de la RSC.
"""

from __future__ import annotations

import polars as pl

# Version du contrat exposée dans l'enveloppe (cf. méta-périodes / turpe-variable). Bump
# sur rupture ; un ajout de champ optionnel reste additif et ne change pas la version.
CONTRAT_VERSION = 1


def resoudre_rsc(ids: list[str], c15: pl.DataFrame, affaires: pl.DataFrame) -> list[dict]:
    """`ref_situation_contractuelle` (ou motif d'erreur) pour chaque `id_Affaire` (#282).

    Args:
        ids: lot d'`id_Affaire` à résoudre (ré-émis tels quels, jamais interprétés).
        c15: événements `flux_c15` dont `id_affaire ∈ ids` — colonnes `id_affaire`,
            `ref_situation_contractuelle` (valeurs natives, **pas** forward-fillées).
        affaires: lignes `flux_affaires` (X12) dont `affaire_id ∈ ids` — recoupement
            d'existence (colonne `affaire_id`).

    Returns:
        Liste, dans l'ordre d'entrée, de `{id_affaire, ref_situation_contractuelle}`
        (succès) **ou** `{id_affaire, error}` (xor) — jamais de silent-drop.
    """
    rsc_par_affaire = _rsc_par_affaire(c15)
    affaires_connues = _affaires_connues(affaires)
    return [_resoudre_un(id_aff, rsc_par_affaire, affaires_connues) for id_aff in ids]


def _resoudre_un(id_aff: str, rsc_par_affaire: dict[str, list[str]], affaires_connues: set[str]) -> dict:
    """Résout un seul `id_Affaire` en `{id_affaire, ref_situation_contractuelle}` (xor `error`)."""
    rscs = rsc_par_affaire.get(id_aff, [])
    if len(rscs) == 1:
        return {"id_affaire": id_aff, "ref_situation_contractuelle": rscs[0]}
    if not rscs:
        if id_aff in affaires_connues:
            return _erreur(
                id_aff,
                "Affaire connue (X12) sans situation contractuelle C15 "
                "(précurseur en cours ou affaire non contractuelle).",
            )
        return _erreur(id_aff, f"Affaire inconnue : {id_aff}")
    rscs_triees = sorted(rscs)
    return _erreur(
        id_aff,
        f"Résolution ambiguë : {len(rscs_triees)} situations contractuelles "
        f"pour l'affaire {id_aff} ({', '.join(rscs_triees)}).",
    )


def _erreur(id_aff: str, motif: str) -> dict:
    return {"id_affaire": id_aff, "error": motif}


def _affaires_connues(affaires: pl.DataFrame) -> set[str]:
    """Ensemble des `affaire_id` présents dans le flux X12 (recoupement d'existence)."""
    if affaires.height == 0:
        return set()
    return set(affaires.get_column("affaire_id").drop_nulls().to_list())


def _rsc_par_affaire(c15: pl.DataFrame) -> dict[str, list[str]]:
    """`{id_affaire: [ref_situation_contractuelle distinctes]}` depuis les événements C15."""
    if c15.height == 0:
        return {}
    grouped = (
        c15.select("id_affaire", "ref_situation_contractuelle")
        .filter(pl.col("id_affaire").is_not_null() & pl.col("ref_situation_contractuelle").is_not_null())
        .unique()
        .group_by("id_affaire")
        .agg(pl.col("ref_situation_contractuelle"))
    )
    return {row["id_affaire"]: row["ref_situation_contractuelle"] for row in grouped.iter_rows(named=True)}
