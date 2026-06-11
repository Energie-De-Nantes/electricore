"""Wire-up facturation Odoo ↔ Enedis (issue #39, ADR-0016 ; assemblage en core depuis #143).

Lit les lignes de factures du mois côté Odoo et délègue l'assemblage aux
builds core (`contexte_mensuel`, `rapport_facturation`). Transitional : le
wire-up monte en `api/services/` avec #144, ce module disparaîtra.

Deux interfaces cohabitent (cf. issue #64, calque de #56 / #63) :

- `facturation_du_mois` : calcul brut, une ligne par ligne de facture Odoo
  enrichie Enedis. Pour exploration et streaming Arrow.
- `rapport_facturation` : livrable agrégé (`Résumé` / `Lignes` /
  `Changements puissance`) destiné au facturiste, XLSX multi-onglets.
"""

import polars as pl

from electricore.core.builds.contexte_mensuel import charger, documents, rapprocher
from electricore.core.builds.rapport_facturation import (
    RapportFacturation,
    feuilles_rapport_facturation,
)
from electricore.core.builds.rapport_facturation import (
    rapport_facturation as _rapport_facturation_core,
)
from electricore.core.loaders import c15, f15, releves_harmonises

from .helpers import lignes_factures_du_mois
from .reader import OdooReader

__all__ = [
    "RapportFacturation",
    "documents_facturation_du_mois",
    "facturation_du_mois",
    "feuilles_rapport_facturation",
    "rapport_facturation",
]


def facturation_du_mois(odoo: OdooReader, mois: str | None = None) -> pl.DataFrame:
    """Réconciliation Odoo ↔ Enedis pour le mois cible.

    Charge le contexte mensuel (historique enrichi + facturation Enedis), lit
    les lignes de factures Odoo du mois, et applique le rapprochement.

    Args:
        odoo: `OdooReader` déjà ouvert (le caller est responsable de la connexion).
        mois: format "YYYY-MM-DD" (premier jour du mois). `None` = dernier mois
            des données disponibles.

    Returns:
        `pl.DataFrame` une ligne par ligne de facture Odoo du mois cible,
        enrichie des données Enedis et des flags `a_facturer` / `a_supprimer`
        (cf. ADR-0014).
    """
    contexte = charger(c15().lazy(), releves_harmonises().lazy(), mois=mois)
    lignes_df = lignes_factures_du_mois(odoo, contexte.mois).collect()
    return rapprocher(contexte, lignes_df)


def rapport_facturation(odoo: OdooReader, mois: str | None = None) -> RapportFacturation:
    """Livrable mensuel facturation : `Résumé` / `Lignes` / `Changements puissance`.

    Wire-up : charge le contexte mensuel et les lignes Odoo du mois, délègue
    l'assemblage au build core `rapport_facturation` (#143). Le mois effectif
    est porté par le contexte (résolu par `charger()`).

    Args:
        odoo: `OdooReader` déjà ouvert.
        mois: format "YYYY-MM-DD". `None` = dernier mois disponible.

    Returns:
        `RapportFacturation(resume, lignes, changements_puissance)`.
    """
    contexte = charger(c15().lazy(), releves_harmonises().lazy(), mois=mois)
    lignes_df = lignes_factures_du_mois(odoo, contexte.mois).collect()
    return _rapport_facturation_core(contexte, lignes_df)


def documents_facturation_du_mois(odoo: OdooReader, mois: str | None = None) -> tuple[dict[str, pl.DataFrame], str]:
    """Documents utiles à la campagne de facturation mensuelle (audit + injection).

    Produit 6 DataFrames consommés par l'endpoint
    `/facturation/documents.xlsx` (livrable XLSX multi-onglets, cf. #78) :

    - `F15 complet` : flux F15 brut du mois (audit TURPE distributeur)
    - `F15 prestations` : F15 filtré sur `unite = "UNITE"` (prestations)
    - `C15 complet` : flux C15 brut du mois (audit événements)
    - `C15 sorties` : C15 filtré sur `RES` + `CFNS` (résiliations / sorties)
    - `Réconciliation` : sortie de `facturation_du_mois`
    - `Changements puissance` : reconciliation filtrée sur `memo_puissance`

    Args:
        odoo: `OdooReader` déjà ouvert.
        mois: format "YYYY-MM-DD". `None` = dernier mois des données.

    Returns:
        `(documents, suffix)` — dict `{libellé_onglet: DataFrame}` consommable
        directement par `xlsx_multi_sheet`, et `suffix` au format "YYYY-MM"
        pour la nomenclature du fichier final.
    """
    contexte = charger(c15().lazy(), releves_harmonises().lazy(), mois=mois)
    lignes_df = lignes_factures_du_mois(odoo, contexte.mois).collect()
    return documents(contexte, lignes_df, f15=f15().lazy(), c15=c15().lazy())
