"""Orchestrations de facturation Odoo ↔ Enedis (issue #39, ADR-0016).

Compose les calculs `core/` (rapprochement facturation mensuelle, contexte
mensuel) avec l'adaptateur Odoo (lecture des lignes de factures du mois)
pour produire les artefacts consommés par les endpoints API et les notebooks.

EDN-shaped aujourd'hui ; sert de prototype pour un futur module Odoo libre
couvrant les fournisseurs alternatifs (cf. CONTEXT.md, ADR-0016).
"""

import polars as pl

from electricore.core.loaders import c15, f15
from electricore.core.loaders.contexte_mensuel import charger_contexte_facturation
from electricore.core.pipelines.facturation import rapprocher_facturation_mensuelle

from .helpers import lignes_factures_du_mois
from .reader import OdooReader


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
    contexte = charger_contexte_facturation(mois)
    lignes_df = lignes_factures_du_mois(odoo, contexte.mois).collect()
    return rapprocher_facturation_mensuelle(
        lignes_odoo=lignes_df,
        fact_mensuelle=contexte.facturation_mensuelle.lazy(),
        historique=contexte.historique_enrichi,
        mois=contexte.mois,
    )


def documents_facturation_du_mois(odoo: OdooReader, mois: str | None = None) -> tuple[dict[str, pl.DataFrame], str]:
    """Documents utiles à la campagne de facturation mensuelle (audit + injection).

    Produit 6 DataFrames consommés par le ZIP de l'endpoint
    `/facturation/documents` et par les notebooks d'opérateur :

    - `f15_complet.csv` : flux F15 brut du mois (audit TURPE distributeur)
    - `f15_prestas.csv` : F15 filtré sur `unite = "UNITE"` (prestations)
    - `c15_complet.csv` : flux C15 brut du mois (audit événements)
    - `c15_sorties.csv` : C15 filtré sur `RES` + `CFNS` (résiliations / sorties)
    - `reconciliation.csv` : sortie de `facturation_du_mois`
    - `changements_puissance.csv` : reconciliation filtrée sur `memo_puissance`

    Args:
        odoo: `OdooReader` déjà ouvert.
        mois: format "YYYY-MM-DD". `None` = dernier mois des données.

    Returns:
        `(documents, suffix)` — dict `{nom_fichier: DataFrame}` et `suffix` au
        format "YYYY-MM" pour la nomenclature du ZIP. La sérialisation
        (ZIP/CSV) reste à charge du caller.
    """
    contexte = charger_contexte_facturation(mois)
    suffix = contexte.mois[:7]
    mois_date = pl.lit(contexte.mois).str.to_date()

    lignes_df = lignes_factures_du_mois(odoo, contexte.mois).collect()
    reconciliation = rapprocher_facturation_mensuelle(
        lignes_odoo=lignes_df,
        fact_mensuelle=contexte.facturation_mensuelle.lazy(),
        historique=contexte.historique_enrichi,
        mois=contexte.mois,
    )
    changements_puissance = reconciliation.filter(pl.col("memo_puissance") != "")

    f15_df = f15().lazy().filter(pl.col("date_facture").dt.truncate("1mo").dt.date() == mois_date).collect()
    f15_prestas = f15_df.filter(pl.col("unite") == "UNITE")

    c15_df = c15().lazy().filter(pl.col("date_evenement").dt.truncate("1mo").dt.date() == mois_date).collect()
    c15_sorties = c15_df.filter(pl.col("evenement_declencheur").is_in(["RES", "CFNS"]))

    documents = {
        "f15_complet.csv": f15_df,
        "f15_prestas.csv": f15_prestas,
        "c15_complet.csv": c15_df,
        "c15_sorties.csv": c15_sorties,
        "reconciliation.csv": reconciliation,
        "changements_puissance.csv": changements_puissance,
    }
    return documents, suffix
