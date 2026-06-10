"""Wire-up des taxes énergétiques (Accise TICFE, CTA) pour l'API (ADR-0019, issue #108).

Compose les sources Odoo (`integrations/odoo/sources.py`), les loaders DuckDB
(`core/loaders/`) et les builds purs (`core/builds/rapport_taxe.py`), puis
applique la validation Pandera avant de retourner au router.

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
from electricore.integrations.odoo.models.rapport_accise import (
    RapportAcciseDetail,
    RapportAcciseParTaux,
    RapportAcciseResume,
)
from electricore.integrations.odoo.models.rapport_cta import (
    RapportCtaDetail,
    RapportCtaParTaux,
    RapportCtaResume,
)
from electricore.integrations.odoo.reader import OdooReader
from electricore.integrations.odoo.sources import lignes_factures_taxe, mapping_pdl_order


def rapport_accise_service(odoo: OdooReader, trimestre: str | None = None) -> RapportTaxe:
    """Livrable Accise validé Pandera — wire-up sources Odoo + build pur.

    Args:
        odoo: `OdooReader` déjà ouvert (le caller gère le contexte).
        trimestre: format "YYYY-TX". `None` = tous les trimestres.

    Returns:
        `RapportTaxe(resume, par_taux, detail)` validé.
    """
    lignes = lignes_factures_taxe(odoo)
    r = rapport_accise(lignes, trimestre)
    RapportAcciseResume.validate(r.resume)
    RapportAcciseParTaux.validate(r.par_taux)
    RapportAcciseDetail.validate(r.detail)
    return r


def accise_par_contrat_service(odoo: OdooReader, trimestre: str | None = None) -> pl.DataFrame:
    """Détail brut d'accise par PDL × mois (interface technique Arrow/XLSX mono-onglet).

    Args:
        odoo: `OdooReader` déjà ouvert.
        trimestre: format "YYYY-TX". `None` = pas de filtre.

    Returns:
        `DataFrame` `(pdl, mois_consommation, energie_mwh, taux_accise_eur_mwh, accise_eur, trimestre)`.
    """
    from electricore.core.pipelines.accise import pipeline_accise

    # pipeline_accise est lazy (ADR-0019) — collect au boundary du service avant
    # filtre et retour DataFrame (consommé par les sérialiseurs XLSX/Arrow).
    detail = pipeline_accise(lignes_factures_taxe(odoo)).collect()
    if trimestre is not None:
        detail = detail.filter(pl.col("trimestre") == trimestre)
    return detail


def rapport_cta_service(odoo: OdooReader, trimestre: str | None = None) -> RapportTaxe:
    """Livrable CTA validé Pandera — wire-up sources Odoo + loaders DuckDB + build pur.

    Args:
        odoo: `OdooReader` déjà ouvert.
        trimestre: format "YYYY-TX". `None` = tous les trimestres.

    Returns:
        `RapportTaxe(resume, par_taux, detail)` validé.
    """
    contexte = charger(c15().lazy(), releves_harmonises().lazy(), mois=None)
    pdl_map = mapping_pdl_order(odoo)
    r = rapport_cta(contexte.facturation_mensuelle, pdl_map, trimestre)
    RapportCtaResume.validate(r.resume)
    RapportCtaParTaux.validate(r.par_taux)
    RapportCtaDetail.validate(r.detail)
    return r


def cta_par_contrat_service(odoo: OdooReader, trimestre: str | None = None) -> pl.DataFrame:
    """Détail brut CTA mensuel par PDL × mois (interface technique Arrow/XLSX mono-onglet).

    Args:
        odoo: `OdooReader` déjà ouvert.
        trimestre: format "YYYY-TX". `None` = pas de filtre.

    Returns:
        `DataFrame` mensuel enrichi de `cta_eur`, `taux_cta_pct`, `trimestre`, `order_name`.
    """
    from electricore.core.pipelines.cta import ajouter_cta
    from electricore.core.pipelines.facturation import expr_calculer_trimestre

    contexte = charger(c15().lazy(), releves_harmonises().lazy(), mois=None)
    pdl_map = mapping_pdl_order(odoo)
    df_mensuel = (
        ajouter_cta(
            contexte.facturation_mensuelle.join(pdl_map.select(["pdl", "order_name"]), on="pdl", how="inner").lazy()
        )
        .with_columns(expr_calculer_trimestre().alias("trimestre"))
        .collect()
    )
    if trimestre is not None:
        df_mensuel = df_mensuel.filter(pl.col("trimestre") == trimestre)
    return df_mensuel
