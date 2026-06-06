"""Service de sérialisation des taxes énergétiques (Accise TICFE, CTA).

Le calcul et l'orchestration métier vivent dans
`electricore.integrations.odoo.taxes`. Ce service ne fait que :

1. Ouvrir la connexion `OdooReader` (responsabilité HTTP)
2. Déléguer à l'orchestration (`accise_par_contrat` / `rapport_accise` / `cta_du_trimestre`)
3. Sérialiser le résultat (XLSX multi-onglets / Arrow IPC).

La shape du livrable (`Résumé` / `Par taux` / `Détail`) vit désormais dans
`integrations.odoo.taxes.rapport_accise` (cf. issue #56).
"""

import polars as pl

from electricore.api.config import settings
from electricore.api.serializers import arrow_stream, xlsx_multi_sheet
from electricore.integrations.odoo import OdooReader
from electricore.integrations.odoo.taxes import (
    accise_par_contrat,
    cta_du_trimestre,
    rapport_accise,
)


def calculer_accise_detail(trimestre: str | None = None) -> pl.DataFrame:
    """Binding HTTP : ouvre Odoo + délègue à `accise_par_contrat`."""
    with OdooReader(config=settings.get_odoo_config()) as odoo:
        return accise_par_contrat(odoo, trimestre)


def generer_accise_detail_arrow(trimestre: str | None = None) -> bytes:
    """Sérialise le détail accise (PDL × mois) en flux Arrow IPC."""
    return arrow_stream(calculer_accise_detail(trimestre))


def generer_accise_detail_xlsx(trimestre: str | None = None) -> bytes:
    """Sérialise le détail accise (PDL × mois) en XLSX mono-onglet."""
    return xlsx_multi_sheet({"Détail": calculer_accise_detail(trimestre)})


def generer_accise_rapport_xlsx(trimestre: str | None = None) -> bytes:
    """Livrable facturiste : 3 onglets (Résumé / Par taux / Détail) depuis `rapport_accise`."""
    with OdooReader(config=settings.get_odoo_config()) as odoo:
        r = rapport_accise(odoo, trimestre)
    return xlsx_multi_sheet(
        {
            "Résumé": r.resume,
            "Par taux": r.par_taux,
            "Détail": r.detail.sort(["pdl", "mois_consommation"]),
        }
    )


def calculer_cta_detail(trimestre: str | None = None) -> pl.DataFrame:
    """Binding HTTP : ouvre Odoo + délègue à `cta_du_trimestre`.

    Seam testable au niveau du service (les endpoints peuvent la monkeypatch
    pour bypasser Odoo). La logique métier vit dans
    `electricore.integrations.odoo.taxes.cta_du_trimestre`.
    """
    with OdooReader(config=settings.get_odoo_config()) as odoo:
        return cta_du_trimestre(odoo, trimestre)


def generer_cta_arrow(trimestre: str | None = None) -> bytes:
    """Sérialise le détail CTA mensuel en flux Arrow IPC."""
    return arrow_stream(calculer_cta_detail(trimestre))


def generer_cta_xlsx(trimestre: str | None = None) -> bytes:
    """Calcule la CTA et retourne un fichier XLSX multi-onglets.

    Les taux sont chargés automatiquement depuis electricore/config/cta_rules.csv
    et appliqués au niveau mensuel. Un changement de taux en cours de trimestre
    (décret CRE) est donc géré automatiquement dans l'agrégation trimestrielle.

    Args:
        trimestre: Filtre optionnel au format "YYYY-TX" (ex: "2025-T1").
            Si None, retourne toutes les données disponibles.

    Returns:
        Contenu XLSX en bytes avec 3 onglets : Résumé, Par taux, Détail.
    """
    df_mensuel = calculer_cta_detail(trimestre)

    df_detail_cta = (
        df_mensuel.lazy()
        .group_by("pdl")
        .agg(
            [
                pl.col("order_name").first(),
                pl.col("turpe_fixe_eur").sum().round(2).alias("turpe_fixe_total"),
                pl.col("cta_eur").sum().round(2).alias("cta"),
                pl.col("taux_cta_pct").unique().sort().alias("taux_cta_appliques"),
            ]
        )
        .sort("cta", descending=True)
        .collect()
        # Sérialiser la liste des taux en string pour un rendu XLSX lisible
        .with_columns(pl.col("taux_cta_appliques").list.eval(pl.element().cast(pl.Utf8)).list.join(" ; "))
    )

    df_par_taux = (
        df_mensuel.group_by(["trimestre", "taux_cta_pct"])
        .agg(
            [
                pl.col("pdl").n_unique().alias("Nombre de PDL"),
                pl.col("turpe_fixe_eur").sum().round(2).alias("TURPE fixe (€)"),
                pl.col("cta_eur").sum().round(2).alias("CTA (€)"),
            ]
        )
        .sort(["trimestre", "taux_cta_pct"])
        .rename({"taux_cta_pct": "Taux CTA (%)"})
    )

    df_resume = (
        df_mensuel.group_by("trimestre")
        .agg(
            [
                pl.col("pdl").n_unique().alias("Nombre de PDL"),
                pl.col("turpe_fixe_eur").sum().round(2).alias("TURPE fixe total (€)"),
                pl.col("cta_eur").sum().round(2).alias("CTA totale (€)"),
            ]
        )
        .sort("trimestre")
    )

    return xlsx_multi_sheet(
        {
            "Résumé": df_resume,
            "Par taux": df_par_taux,
            "Détail": df_detail_cta,
        }
    )
