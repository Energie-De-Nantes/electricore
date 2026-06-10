"""Wire-up des taxes énergétiques (Accise TICFE, CTA) pour l'API (ADR-0019, issues #108, #116).

Compose les sources Odoo (`integrations/odoo/sources.py`), les loaders DuckDB
(`core/loaders/`) et les builds purs (`core/builds/rapport_taxe.py`).

La validation des rapports est portée par les builds (issue #116) ; les deux
services *détail brut* valident leur frame collectée avec le schéma `*Mensuel`
— seul point de matérialisation de ce chemin, où le check d'unicité du grain
devient effectif (sur LazyFrame, le décorateur du pipeline ne vérifie que
colonnes et dtypes).

Les 4 fonctions de service correspondent aux 6 endpoints :
- `rapport_accise_service` → `/taxes/accise/rapport.xlsx`
- `accise_par_contrat_service` → `/taxes/accise/detail.xlsx` + `.arrow`
- `rapport_cta_service` → `/taxes/cta/rapport.xlsx`
- `cta_par_contrat_service` → `/taxes/cta/detail.xlsx` + `.arrow`
"""

import polars as pl

from electricore.core.builds.contexte_mensuel import charger
from electricore.core.builds.rapport_taxe import RapportTaxe, rapport_accise, rapport_cta
from electricore.core.loaders import c15, releves_harmonises
from electricore.core.models.accise_mensuel import AcciseMensuel
from electricore.core.models.cta_mensuel import CtaMensuel
from electricore.core.pipelines.accise import pipeline_accise
from electricore.core.pipelines.cta import pipeline_cta
from electricore.integrations.odoo.reader import OdooReader
from electricore.integrations.odoo.sources import lignes_factures_taxe, mapping_pdl_order


def rapport_accise_service(odoo: OdooReader, trimestre: str | None = None) -> RapportTaxe:
    """Livrable Accise — wire-up sources Odoo + build pur (validé par le build).

    Args:
        odoo: `OdooReader` déjà ouvert (le caller gère le contexte).
        trimestre: format "YYYY-TX". `None` = tous les trimestres.

    Returns:
        `RapportTaxe(resume, par_taux, detail)` validé.
    """
    return rapport_accise(lignes_factures_taxe(odoo), trimestre)


def accise_par_contrat_service(odoo: OdooReader, trimestre: str | None = None) -> pl.DataFrame:
    """Détail brut d'accise par PDL × mois (interface technique Arrow/XLSX mono-onglet).

    Args:
        odoo: `OdooReader` déjà ouvert.
        trimestre: format "YYYY-TX". `None` = pas de filtre.

    Returns:
        `DataFrame` shape `AcciseMensuel` (grain (pdl, mois_annee) garanti).
    """
    # Collect au boundary du service (ADR-0019) ; filtre trimestre au caller
    # du pipeline (décision #116).
    detail = pipeline_accise(lignes_factures_taxe(odoo)).collect()
    if trimestre is not None:
        detail = detail.filter(pl.col("trimestre") == trimestre)
    AcciseMensuel.validate(detail)
    return detail


def rapport_cta_service(odoo: OdooReader, trimestre: str | None = None) -> RapportTaxe:
    """Livrable CTA — wire-up sources Odoo + loaders DuckDB + build pur (validé par le build).

    Args:
        odoo: `OdooReader` déjà ouvert.
        trimestre: format "YYYY-TX". `None` = tous les trimestres.

    Returns:
        `RapportTaxe(resume, par_taux, detail)` validé.
    """
    contexte = charger(c15().lazy(), releves_harmonises().lazy(), mois=None)
    return rapport_cta(contexte.facturation_mensuelle, mapping_pdl_order(odoo), trimestre)


def cta_par_contrat_service(odoo: OdooReader, trimestre: str | None = None) -> pl.DataFrame:
    """Détail brut CTA mensuel (interface technique Arrow/XLSX mono-onglet).

    Args:
        odoo: `OdooReader` déjà ouvert.
        trimestre: format "YYYY-TX". `None` = pas de filtre.

    Returns:
        `DataFrame` shape `CtaMensuel` (grain (situation contractuelle, mois) garanti).
    """
    contexte = charger(c15().lazy(), releves_harmonises().lazy(), mois=None)
    df_mensuel = pipeline_cta(
        contexte.facturation_mensuelle.lazy(),
        mapping_pdl_order(odoo).lazy(),
    ).collect()
    if trimestre is not None:
        df_mensuel = df_mensuel.filter(pl.col("trimestre") == trimestre)
    CtaMensuel.validate(df_mensuel)
    return df_mensuel
