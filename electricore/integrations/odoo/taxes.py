"""Orchestrations des taxes énergétiques (Accise TICFE, CTA) côté Odoo (#40, ADR-0016).

Compose les calculs `core/` (pipelines accise / CTA, contexte mensuel) avec
l'adaptateur Odoo (lecture des lignes de factures, des PDLs des `sale.order`)
pour produire les DataFrames consommés par les endpoints API et les notebooks.

Deux interfaces cohabitent (cf. issue #56) :

- `accise_par_contrat` / `cta_par_contrat` : calcul brut par PDL × période, pour
  exploration et réinjection (Arrow / XLSX mono-onglet).
- `rapport_accise` / `rapport_cta` : livrable agrégé (`Résumé` / `Par taux` /
  `Détail`) destiné à la déclaration trimestrielle facturiste (XLSX multi-onglets).

EDN-shaped aujourd'hui ; sert de prototype pour un futur module Odoo libre
couvrant les fournisseurs alternatifs (cf. CONTEXT.md, ADR-0016).
"""

from typing import NamedTuple

import polars as pl

from electricore.core.builds.contexte_mensuel import charger
from electricore.core.loaders import c15, releves_harmonises
from electricore.core.pipelines.accise import pipeline_accise
from electricore.core.pipelines.cta import ajouter_cta
from electricore.core.pipelines.facturation import expr_calculer_trimestre

from .helpers import commandes_lignes, query
from .models.rapport_accise import (
    RapportAcciseDetail,
    RapportAcciseParTaux,
    RapportAcciseResume,
)
from .models.rapport_cta import (
    RapportCtaDetail,
    RapportCtaParTaux,
    RapportCtaResume,
)
from .reader import OdooReader


class RapportAccise(NamedTuple):
    """Livrable trimestriel d'accise (= les 3 onglets de l'export XLSX facturiste)."""

    resume: pl.DataFrame
    par_taux: pl.DataFrame
    detail: pl.DataFrame


class RapportCta(NamedTuple):
    """Livrable trimestriel CTA (= les 3 onglets de l'export XLSX facturiste).

    `detail` est une agrégation par PDL (sommes + taux successifs string-joined),
    pas la sortie brute de `cta_par_contrat` (qui reste mensuelle).
    """

    resume: pl.DataFrame
    par_taux: pl.DataFrame
    detail: pl.DataFrame


def accise_par_contrat(odoo: OdooReader, trimestre: str | None = None) -> pl.DataFrame:
    """Calcul brut de l'accise par PDL × mois de consommation.

    Interface "brute" pour les cas techniques (exploration, réinjection, Arrow stream).
    Pour le livrable agrégé destiné au facturiste, utiliser `rapport_accise(...)`.

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


def rapport_accise(odoo: OdooReader, trimestre: str | None = None) -> RapportAccise:
    """Livrable agrégé pour déclaration trimestrielle d'accise TICFE.

    Compose `accise_par_contrat` (calcul brut) avec les agrégations « Par taux »
    et « Résumé » nécessaires au facturiste. Les 3 frames du `NamedTuple` sont
    validées Pandera avant retour.

    Args:
        odoo: `OdooReader` déjà ouvert.
        trimestre: format "YYYY-TX". `None` = tous les trimestres.

    Returns:
        `RapportAccise(resume, par_taux, detail)`. `detail` est trié
        `(pdl, mois_consommation)` — invariant porté par cette fonction.
    """
    detail = accise_par_contrat(odoo, trimestre).sort(["pdl", "mois_consommation"])
    par_taux = (
        detail.group_by("taux_accise_eur_mwh")
        .agg(
            [
                pl.col("energie_mwh").sum().round(3),
                pl.col("accise_eur").sum().round(2),
                pl.col("pdl").n_unique().cast(pl.Int64).alias("nb_pdl"),
            ]
        )
        .sort("taux_accise_eur_mwh", descending=True)
    )
    resume = (
        detail.group_by("trimestre")
        .agg(
            [
                pl.col("pdl").n_unique().cast(pl.Int64).alias("nb_pdl"),
                pl.col("energie_mwh").sum().round(3).alias("energie_mwh_total"),
                pl.col("accise_eur").sum().round(2).alias("accise_eur_total"),
            ]
        )
        .sort("trimestre")
    )
    RapportAcciseResume.validate(resume)
    RapportAcciseParTaux.validate(par_taux)
    RapportAcciseDetail.validate(detail)
    return RapportAccise(resume=resume, par_taux=par_taux, detail=detail)


def feuilles_rapport_accise(r: RapportAccise) -> dict[str, pl.DataFrame]:
    """Mapping onglet → DataFrame pour le livrable XLSX Accise (cf. CONTEXT.md).

    Consommable directement par `xlsx_multi_sheet`. Co-localisée avec
    `rapport_accise` parce que le shape du livrable et son contenu sont
    indissociables.
    """
    return {"Résumé": r.resume, "Par taux": r.par_taux, "Détail": r.detail}


def cta_par_contrat(odoo: OdooReader, trimestre: str | None = None) -> pl.DataFrame:
    """Calcul brut de la CTA mensuelle par PDL × mois (interface technique).

    Pour le livrable facturiste (multi-onglets Résumé / Par taux / Détail),
    utiliser `rapport_cta(...)`.

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
    contexte = charger(c15().lazy(), releves_harmonises().lazy(), mois=None)

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


def rapport_cta(odoo: OdooReader, trimestre: str | None = None) -> RapportCta:
    """Livrable agrégé pour déclaration trimestrielle CTA.

    Compose `cta_par_contrat` (mensuel brut) avec les agrégations « Par taux »,
    « Résumé » et une synthèse « Détail » par PDL où la liste des taux appliqués
    est sérialisée en string (utile quand un décret CRE découpe le trimestre).

    Les 3 frames du `NamedTuple` sont validées Pandera avant retour.

    Args:
        odoo: `OdooReader` déjà ouvert.
        trimestre: format "YYYY-TX". `None` = tous les trimestres.

    Returns:
        `RapportCta(resume, par_taux, detail)`.
    """
    df_mensuel = cta_par_contrat(odoo, trimestre)

    par_taux = (
        df_mensuel.group_by(["trimestre", "taux_cta_pct"])
        .agg(
            [
                pl.col("pdl").n_unique().cast(pl.Int64).alias("nb_pdl"),
                pl.col("turpe_fixe_eur").sum().round(2),
                pl.col("cta_eur").sum().round(2),
            ]
        )
        .sort(["trimestre", "taux_cta_pct"])
    )

    resume = (
        df_mensuel.group_by("trimestre")
        .agg(
            [
                pl.col("pdl").n_unique().cast(pl.Int64).alias("nb_pdl"),
                pl.col("turpe_fixe_eur").sum().round(2).alias("turpe_fixe_total_eur"),
                pl.col("cta_eur").sum().round(2).alias("cta_total_eur"),
            ]
        )
        .sort("trimestre")
    )

    detail = (
        df_mensuel.lazy()
        .group_by("pdl")
        .agg(
            [
                pl.col("order_name").first(),
                pl.col("turpe_fixe_eur").sum().round(2).alias("turpe_fixe_total_eur"),
                pl.col("cta_eur").sum().round(2).alias("cta_total_eur"),
                pl.col("taux_cta_pct").unique().sort().alias("_taux_list"),
            ]
        )
        .sort("cta_total_eur", descending=True)
        .collect()
        .with_columns(
            pl.col("_taux_list").list.eval(pl.element().cast(pl.Utf8)).list.join(" ; ").alias("taux_cta_appliques")
        )
        .drop("_taux_list")
    )

    RapportCtaResume.validate(resume)
    RapportCtaParTaux.validate(par_taux)
    RapportCtaDetail.validate(detail)
    return RapportCta(resume=resume, par_taux=par_taux, detail=detail)


def feuilles_rapport_cta(r: RapportCta) -> dict[str, pl.DataFrame]:
    """Mapping onglet → DataFrame pour le livrable XLSX CTA (cf. CONTEXT.md).

    Consommable directement par `xlsx_multi_sheet`. Co-localisée avec
    `rapport_cta` parce que le shape du livrable et son contenu sont
    indissociables.
    """
    return {"Résumé": r.resume, "Par taux": r.par_taux, "Détail": r.detail}
