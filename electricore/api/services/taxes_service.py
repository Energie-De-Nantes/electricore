"""Service de sérialisation des taxes énergétiques (Accise TICFE, CTA).

Le calcul et l'orchestration métier vivent dans
`electricore.integrations.odoo.taxes`. Ce service ne fait que :

1. Ouvrir la connexion `OdooReader` (responsabilité HTTP)
2. Déléguer à l'orchestration
3. Sérialiser le résultat (XLSX multi-onglets / Arrow IPC), avec les
   agrégations « par taux » / « résumé » spécifiques au livrable.
"""

import io

import polars as pl
import xlsxwriter

from electricore.api.config import settings
from electricore.integrations.odoo import OdooReader
from electricore.integrations.odoo.taxes import accise_du_trimestre, cta_du_trimestre


def calculer_accise_detail(trimestre: str | None = None) -> pl.DataFrame:
    """Binding HTTP : ouvre Odoo + délègue à `accise_du_trimestre`.

    Seam testable au niveau du service (les endpoints peuvent la monkeypatch
    pour bypasser Odoo). La logique métier vit dans
    `electricore.integrations.odoo.taxes.accise_du_trimestre`.
    """
    with OdooReader(config=settings.get_odoo_config()) as odoo:
        return accise_du_trimestre(odoo, trimestre)


def generer_accise_arrow(trimestre: str | None = None) -> bytes:
    """Sérialise le détail accise en flux Arrow IPC."""
    df = calculer_accise_detail(trimestre)
    buf = io.BytesIO()
    df.write_ipc_stream(buf)
    return buf.getvalue()


def generer_accise_xlsx(trimestre: str | None = None) -> bytes:
    """Calcule l'accise TICFE et retourne un fichier XLSX multi-onglets.

    Args:
        trimestre: Filtre optionnel au format "YYYY-TX" (ex: "2025-T1").
            Si None, retourne toutes les données disponibles.

    Returns:
        Contenu XLSX en bytes avec 3 onglets : Résumé, Par taux, Détail.
    """
    df_accise = calculer_accise_detail(trimestre)

    df_par_taux = (
        df_accise.group_by("taux_accise_eur_mwh")
        .agg(
            [
                pl.col("energie_mwh").sum().round(3),
                pl.col("accise_eur").sum().round(2),
                pl.col("pdl").n_unique().alias("nb_pdl"),
            ]
        )
        .sort("taux_accise_eur_mwh", descending=True)
    )

    df_resume = (
        df_accise.group_by("trimestre")
        .agg(
            [
                pl.col("pdl").n_unique().alias("Nombre de PDL"),
                pl.col("energie_mwh").sum().round(3).alias("Énergie totale (MWh)"),
                pl.col("accise_eur").sum().round(2).alias("Accise totale (€)"),
            ]
        )
        .sort("trimestre")
    )

    return _construire_xlsx(
        {
            "Résumé": df_resume,
            "Par taux": df_par_taux,
            "Détail": df_accise.sort(["pdl", "mois_consommation"]),
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
    df = calculer_cta_detail(trimestre)
    buf = io.BytesIO()
    df.write_ipc_stream(buf)
    return buf.getvalue()


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

    return _construire_xlsx(
        {
            "Résumé": df_resume,
            "Par taux": df_par_taux,
            "Détail": df_detail_cta,
        }
    )


def _construire_xlsx(onglets: dict[str, pl.DataFrame]) -> bytes:
    """Construit un fichier XLSX multi-onglets à partir d'un dict {nom: DataFrame}."""
    buf = io.BytesIO()
    wb = xlsxwriter.Workbook(buf, {"remove_timezone": True})
    for nom, df in onglets.items():
        df.write_excel(workbook=wb, worksheet=nom)
    wb.close()
    return buf.getvalue()
