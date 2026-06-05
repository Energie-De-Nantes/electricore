"""Orchestrations des taxes énergétiques (Accise TICFE, CTA) côté Odoo (#40, ADR-0016).

Compose les calculs `core/` (pipelines accise / CTA, contexte mensuel) avec
l'adaptateur Odoo (lecture des lignes de factures, des PDLs des `sale.order`)
pour produire les DataFrames consommés par les endpoints API et les notebooks.

EDN-shaped aujourd'hui ; sert de prototype pour un futur module Odoo libre
couvrant les fournisseurs alternatifs (cf. CONTEXT.md, ADR-0016).
"""

import polars as pl

from electricore.core.loaders.contexte_mensuel import charger_contexte_facturation
from electricore.core.pipelines.accise import pipeline_accise
from electricore.core.pipelines.cta import ajouter_cta
from electricore.core.pipelines.facturation import expr_calculer_trimestre

from .helpers import commandes_lignes, query
from .reader import OdooReader


def accise_du_trimestre(odoo: OdooReader, trimestre: str | None = None) -> pl.DataFrame:
    """Charge les lignes Odoo et applique `pipeline_accise` (+ filtre trimestre).

    Args:
        odoo: `OdooReader` déjà ouvert (le caller est responsable de la connexion).
        trimestre: format "YYYY-TX" (ex: "2025-T1"). `None` = pas de filtre.

    Returns:
        `pl.DataFrame` avec colonnes `pdl`, `mois_consommation`, `energie_mwh`,
        `taux_accise_eur_mwh`, `accise_eur`, `trimestre`.
    """
    df_lignes = commandes_lignes(odoo).collect()
    df_accise = pipeline_accise(df_lignes.lazy())
    if trimestre is not None:
        df_accise = df_accise.filter(pl.col("trimestre") == trimestre)
    return df_accise


def cta_du_trimestre(odoo: OdooReader, trimestre: str | None = None) -> pl.DataFrame:
    """Charge Odoo + flux, applique la pipeline CTA mensuelle (+ filtre trimestre).

    Args:
        odoo: `OdooReader` déjà ouvert.
        trimestre: format "YYYY-TX". `None` = pas de filtre.

    Returns:
        `pl.DataFrame` mensuel (pdl × mois) enrichi de `cta_eur`, `taux_cta_pct`,
        `trimestre`, `order_name`. Les agrégations « par taux » / « résumé »
        sont à charge du caller (ou du notebook).
    """
    df_pdl = (
        query(odoo, "sale.order", domain=[("x_pdl", "!=", False)], fields=["name", "x_pdl"])
        .filter(pl.col("x_pdl").is_not_null())
        .select(
            [
                pl.col("x_pdl").str.strip_chars().alias("pdl"),
                pl.col("name").alias("order_name"),
            ]
        )
        .collect()
        .unique("pdl")
    )

    # CTA opère sur tous les mois (filtrage par trimestre en aval) — `mois=None`
    # déclenche la résolution interne mais le champ `contexte.mois` n'est pas utilisé ici.
    contexte = charger_contexte_facturation(None)

    df_mensuel = (
        ajouter_cta(
            contexte.facturation_mensuelle.join(df_pdl.select(["pdl", "order_name"]), on="pdl", how="inner").lazy()
        )
        .with_columns(expr_calculer_trimestre().alias("trimestre"))
        .collect()
    )

    if trimestre is not None:
        df_mensuel = df_mensuel.filter(pl.col("trimestre") == trimestre)
    return df_mensuel
