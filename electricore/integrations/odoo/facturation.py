"""Orchestrations de facturation Odoo ↔ Enedis (issue #39, ADR-0016).

Compose les calculs `core/` (rapprochement facturation mensuelle, contexte
mensuel) avec l'adaptateur Odoo (lecture des lignes de factures du mois)
pour produire les artefacts consommés par les endpoints API et les notebooks.

Deux interfaces cohabitent (cf. issue #64, calque de #56 / #63) :

- `facturation_du_mois` : calcul brut, une ligne par ligne de facture Odoo
  enrichie Enedis. Pour exploration et streaming Arrow.
- `rapport_facturation` : livrable agrégé (`Résumé` / `Lignes` /
  `Changements puissance`) destiné au facturiste, XLSX multi-onglets.

EDN-shaped aujourd'hui ; sert de prototype pour un futur module Odoo libre
couvrant les fournisseurs alternatifs (cf. CONTEXT.md, ADR-0016).
"""

from dataclasses import dataclass

import polars as pl

from electricore.core.builds.contexte_mensuel import charger, documents, rapprocher
from electricore.core.loaders import c15, f15, releves_harmonises
from electricore.core.models.lignes_facture_rapprochees import LignesFactureRapprochees

from .helpers import lignes_factures_du_mois
from .models.rapport_facturation import RapportFacturationResume
from .reader import OdooReader


@dataclass(frozen=True, slots=True)
class RapportFacturation:
    """Livrable mensuel de facturation (= les 3 onglets de l'XLSX facturiste).

    - `resume` : totaux du mois (1 ligne).
    - `lignes` : sortie brute de `facturation_du_mois` (toutes les lignes).
    - `changements_puissance` : sous-ensemble de `lignes` où `memo_puissance != ""`.
      Les lignes apparaissent dans les deux onglets (drill-down, pas partition exclusive).
    """

    resume: pl.DataFrame
    lignes: pl.DataFrame
    changements_puissance: pl.DataFrame


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

    Compose `facturation_du_mois` avec un sous-ensemble (`memo_puissance != ""`)
    et un résumé des compteurs `a_facturer` / `a_supprimer`.

    Args:
        odoo: `OdooReader` déjà ouvert.
        mois: format "YYYY-MM-DD". `None` = dernier mois disponible.

    Returns:
        `RapportFacturation(resume, lignes, changements_puissance)`.
    """
    lignes = facturation_du_mois(odoo, mois)
    changements_puissance = lignes.filter(pl.col("memo_puissance") != "")

    # Mois effectif (= ce que `lignes` a vu — peut différer de l'arg si `mois=None`)
    mois_effectif = mois if mois is not None else lignes.select(pl.col("debut").min().dt.strftime("%Y-%m-%d")).item()
    resume = pl.DataFrame(
        {
            "mois": [mois_effectif or ""],
            "nb_pdl": [lignes["pdl"].drop_nulls().n_unique()],
            "total_a_facturer": [int(lignes["a_facturer"].fill_null(False).sum())],
            "total_a_supprimer": [int(lignes["a_supprimer"].fill_null(False).sum())],
        }
    )

    RapportFacturationResume.validate(resume)
    LignesFactureRapprochees.validate(lignes)
    LignesFactureRapprochees.validate(changements_puissance)
    return RapportFacturation(resume=resume, lignes=lignes, changements_puissance=changements_puissance)


def feuilles_rapport_facturation(r: RapportFacturation) -> dict[str, pl.DataFrame]:
    """Mapping onglet → DataFrame pour le livrable XLSX facturation (cf. CONTEXT.md).

    Consommable directement par `xlsx_multi_sheet`. Co-localisée avec
    `rapport_facturation` parce que le shape du livrable et son contenu sont
    indissociables.
    """
    return {
        "Résumé": r.resume,
        "Lignes": r.lignes,
        "Changements puissance": r.changements_puissance,
    }


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
