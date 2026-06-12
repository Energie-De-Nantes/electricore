"""
Millésime des taux régulés (ADR-0024, issue #185).

Le millésime dit ce que la lib « sait » de la réglementation : dernière ligne
entrée en vigueur d'un fichier de taux + sa référence réglementaire. Dérivé
des CSV versionnés (`electricore/config/*_rules.csv`), jamais déclaré.
"""

import datetime as dt
from dataclasses import dataclass

import polars as pl

from electricore.core.pipelines.accise import load_accise_rules
from electricore.core.pipelines.cta import load_cta_rules
from electricore.core.pipelines.turpe import load_turpe_rules


@dataclass(frozen=True, slots=True)
class Millesime:
    """Dernier changement réglementaire intégré dans un fichier de taux régulés.

    `valeur`/`unite` portent le taux scalaire quand la taxe en a un (Accise en
    €/MWh, CTA en %) ; None pour une grille multi-colonnes (TURPE).
    """

    taxe: str
    date_vigueur: dt.date
    reference: str
    valeur: float | None = None
    unite: str | None = None


def deriver_millesime(
    historique: pl.LazyFrame,
    *,
    taxe: str,
    taux_col: str | None = None,
    unite: str | None = None,
) -> Millesime:
    """Dérive le millésime d'un historique de taux : ligne au `start` le plus récent.

    Args:
        historique: LazyFrame avec au moins `start` (datetime) et `reference` (str).
        taxe: libellé du registre ("TURPE", "Accise", "CTA").
        taux_col: colonne du taux scalaire, si la taxe en a un.
        unite: unité d'affichage du taux scalaire.
    """
    colonnes = ["start", "reference", *([taux_col] if taux_col else [])]
    derniere = historique.select(colonnes).sort("start").last().collect()
    return Millesime(
        taxe=taxe,
        date_vigueur=derniere["start"][0].date(),
        reference=derniere["reference"][0],
        valeur=derniere[taux_col][0] if taux_col else None,
        unite=unite if taux_col else None,
    )


def millesimes() -> tuple[Millesime, Millesime, Millesime]:
    """Les millésimes des trois registres de taux régulés : TURPE, Accise, CTA."""
    return (
        deriver_millesime(load_turpe_rules(), taxe="TURPE"),
        deriver_millesime(load_accise_rules(), taxe="Accise", taux_col="taux_accise_eur_mwh", unite="€/MWh"),
        deriver_millesime(load_cta_rules(), taxe="CTA", taux_col="taux_cta_pct", unite="%"),
    )
