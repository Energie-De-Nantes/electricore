"""Wire-up de la facturation mensuelle Odoo ↔ Enedis (ADR-0019, issue #144).

Compose la source Odoo (`integrations/odoo/sources.py`), les loaders DuckDB
(`core/loaders/`) et les builds purs (`core/builds/contexte_mensuel.py`,
`core/builds/rapport_facturation.py`) — calque de `taxes_service.py`.
Dernière tranche de la migration #142→#144 : l'ex
`integrations/odoo/facturation.py` est supprimé, l'assemblage vit en core,
l'I/O Odoo dans les sources, le wire-up ici.

Les 3 fonctions de service correspondent aux 4 endpoints :
- `rapport_facturation` → `/facturation/rapport.xlsx`
- `facturation_du_mois` → `/facturation/detail.xlsx` + `.arrow`
- `documents_facturation_du_mois` → `/facturation/documents.xlsx`
"""

import polars as pl

from electricore.core.builds.contexte_mensuel import (
    _MAPPING_CATEGORIE_COLONNE,
    ContexteMensuel,
    contexte_du_mois,
    documents,
    rapprocher,
)
from electricore.core.builds.rapport_facturation import RapportFacturation
from electricore.core.builds.rapport_facturation import rapport_facturation as _rapport_facturation_core
from electricore.core.loaders import c15, f15
from electricore.integrations.odoo.reader import OdooReader
from electricore.integrations.odoo.sources import lignes_factures_du_mois


def _contexte_et_lignes(odoo: OdooReader, mois: str | None) -> tuple[ContexteMensuel, pl.DataFrame]:
    """Charge le contexte mensuel (sources par défaut, #145) et les lignes Odoo du mois résolu.

    La facturation legacy ne peuple que les lignes d'énergie et d'abonnement : on
    écarte ici les catégories produit hors scope (`Prestation-Enedis`, catégorie
    racine Odoo « All »…) que le catalogue Odoo porte en plus. Sans ce filtre, le
    `isin` du contrat `LignesFacture` ferait échouer `rapprocher()` (503, #335).
    Le sous-ensemble facturable est dérivé du mapping cadran→colonne Enedis (SSOT).
    """
    contexte = contexte_du_mois(mois)
    categories_facturables = list(_MAPPING_CATEGORIE_COLONNE)
    lignes_df = (
        lignes_factures_du_mois(odoo, contexte.mois)
        .filter(pl.col("categorie_produit").is_in(categories_facturables))
        .collect()
    )
    return contexte, lignes_df


def facturation_du_mois(odoo: OdooReader, mois: str | None = None) -> pl.DataFrame:
    """Réconciliation Odoo ↔ Enedis pour le mois cible (détail brut, une ligne
    par ligne de facture Odoo enrichie Enedis — flags ADR-0014 compris).

    Args:
        odoo: `OdooReader` déjà ouvert (le caller gère le contexte).
        mois: format "YYYY-MM-DD" (premier jour du mois). `None` = dernier mois
            des données disponibles.
    """
    contexte, lignes_df = _contexte_et_lignes(odoo, mois)
    return rapprocher(contexte, lignes_df)


def rapport_facturation(odoo: OdooReader, mois: str | None = None) -> RapportFacturation:
    """Livrable mensuel facturation : `Résumé` / `Lignes` / `Changements puissance`.

    Délègue l'assemblage au build core `rapport_facturation` (#143) ; le mois
    effectif est porté par le contexte (résolu par `charger()`).

    Args:
        odoo: `OdooReader` déjà ouvert.
        mois: format "YYYY-MM-DD". `None` = dernier mois disponible.
    """
    contexte, lignes_df = _contexte_et_lignes(odoo, mois)
    return _rapport_facturation_core(contexte, lignes_df)


def documents_facturation_du_mois(odoo: OdooReader, mois: str | None = None) -> tuple[dict[str, pl.DataFrame], str]:
    """Documents de la campagne de facturation mensuelle (audit + injection, cf. #78).

    Retourne `(documents, suffix)` — dict 6 onglets (`F15 complet`,
    `F15 prestations`, `C15 complet`, `C15 sorties`, `Réconciliation`,
    `Changements puissance`) consommable par `xlsx_multi_sheet`, et `suffix`
    "YYYY-MM" pour la nomenclature du fichier.

    Args:
        odoo: `OdooReader` déjà ouvert.
        mois: format "YYYY-MM-DD". `None` = dernier mois des données.
    """
    contexte, lignes_df = _contexte_et_lignes(odoo, mois)
    return documents(contexte, lignes_df, f15=f15().lazy(), c15=c15().lazy())
