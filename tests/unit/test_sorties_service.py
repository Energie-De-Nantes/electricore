"""Tests du service des sorties du périmètre (#632, ADR-0052).

`lister_sorties` reçoit des événements `flux_c15` **déjà filtrés** aux codes de sortie
(RES/CFNS) et aux RSC demandées (le filtre RSC + le filtre code sont poussés côté
loader, comme `_charger_c15` pour la résolution RSC) : une simple projection/dédup, pas
de logique métier de filtrage.
"""

from __future__ import annotations

from datetime import date, datetime

import polars as pl

from electricore.api.services.sorties_service import lister_sorties


def _c15_sorties(rows: list[tuple[str, str, datetime, str]]) -> pl.DataFrame:
    """Événements C15 de sortie minimaux : (rsc, pdl, date_evenement, evenement_declencheur)."""
    return pl.DataFrame(
        rows,
        schema={
            "ref_situation_contractuelle": pl.Utf8,
            "pdl": pl.Utf8,
            "date_evenement": pl.Datetime,
            "evenement_declencheur": pl.Utf8,
        },
        orient="row",
    )


def test_une_ligne_par_rsc_sortie_avec_code_et_date_exacts():
    """Tracer : une sortie RES → sa ligne, code et date jour civil exacts."""
    resultats = lister_sorties(_c15_sorties([("rA", "PDL-A", datetime(2026, 3, 15, 10, 30), "RES")]))
    assert resultats == [
        {
            "ref_situation_contractuelle": "rA",
            "pdl": "PDL-A",
            "evenement_declencheur": "RES",
            "date_sortie": date(2026, 3, 15),
        }
    ]


def test_code_cfns_egalement_reconnu():
    resultats = lister_sorties(_c15_sorties([("rB", "PDL-B", datetime(2026, 4, 1), "CFNS")]))
    assert resultats[0]["evenement_declencheur"] == "CFNS"


def test_plusieurs_rsc_triees():
    """Lot de plusieurs RSC sorties → une ligne chacune, triées par RSC."""
    resultats = lister_sorties(
        _c15_sorties(
            [
                ("rZ", "PDL-Z", datetime(2026, 1, 1), "RES"),
                ("rA", "PDL-A", datetime(2026, 2, 1), "CFNS"),
            ]
        )
    )
    assert [r["ref_situation_contractuelle"] for r in resultats] == ["rA", "rZ"]


def test_lot_vide_renvoie_liste_vide():
    """Frame vide (aucune RSC demandée n'est sortie) → liste vide, sans erreur."""
    assert lister_sorties(_c15_sorties([])) == []


def test_evenement_duplique_sur_la_meme_rsc_reste_dedupe():
    """Défense : si le même événement de sortie apparaît deux fois pour une RSC (ré-ingestion),
    une seule ligne en sort — pas de doublon."""
    resultats = lister_sorties(
        _c15_sorties(
            [
                ("rA", "PDL-A", datetime(2026, 3, 15), "RES"),
                ("rA", "PDL-A", datetime(2026, 3, 15), "RES"),
            ]
        )
    )
    assert len(resultats) == 1
    assert resultats[0]["ref_situation_contractuelle"] == "rA"
