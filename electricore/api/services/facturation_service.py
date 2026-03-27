"""
Service de réconciliation facturation Odoo ↔ Enedis.
Produit updates_rsc sans écriture dans Odoo.
"""

import io
from typing import Optional

import polars as pl
import xlsxwriter

from electricore.api.config import settings
from electricore.core.loaders import OdooReader, c15, lignes_a_facturer, releves_harmonises
from electricore.core.pipelines.orchestration import facturation

MAPPING_CATEGORIE = {
    "HP":          "energie_hp_kwh",
    "HC":          "energie_hc_kwh",
    "Base":        "energie_base_kwh",
    "Abonnements": "nb_jours",
}


def generer_facturation_xlsx(mois: Optional[str] = None) -> bytes:
    """
    Réconciliation Odoo ↔ Enedis pour le mois donné (défaut : dernier mois disponible).

    Args:
        mois: format "YYYY-MM-DD" (premier jour du mois). None = dernier mois des données.

    Returns:
        XLSX bytes — 2 onglets : "Lignes fusionnées" et "Changements puissance".
    """
    # 1. Charger les lignes Odoo en brouillon
    with OdooReader(config=settings.get_odoo_config()) as odoo:
        lignes_df = lignes_a_facturer(odoo).collect()

    # 2. Données Enedis
    lf_historique = c15().lazy()
    lf_releves = releves_harmonises().lazy()

    # 3. Orchestration → fact (DataFrame mensuel)
    _, _, _, fact = facturation(historique=lf_historique, releves=lf_releves)

    # 4. Filtrer au mois — debut est un datetime TZ Europe/Paris ; on compare la date tronquée au mois
    debut_date_mois = pl.col("debut").dt.truncate("1mo").dt.date()
    if mois is not None:
        mois_cible = pl.lit(mois).str.to_date()
        fact_mois = fact.filter(debut_date_mois == mois_cible)
    else:
        mois_en_cours = fact.select(debut_date_mois.alias("m"))["m"].max()
        fact_mois = fact.filter(debut_date_mois == mois_en_cours)

    # 5. Jointure + calcul quantite_enedis par catégorie produit
    quantite_enedis_expr = pl.coalesce([
        pl.when(pl.col("name_product_category") == cat).then(pl.col(col).cast(pl.Float64))
        for cat, col in MAPPING_CATEGORIE.items()
    ]).alias("quantite_enedis")

    updates_rsc = (
        lignes_df
        .join(
            fact_mois,
            left_on="x_ref_situation_contractuelle",
            right_on="ref_situation_contractuelle",
            how="left",
        )
        .with_columns(quantite_enedis_expr)
        .select([
            "invoice_line_ids", "x_pdl", "x_lisse", "name_account_move",
            "name_product_category", "name_product_product",
            "quantity", "quantite_enedis", "memo_puissance",
        ])
    )

    changements_puissance = updates_rsc.filter(pl.col("memo_puissance") != "")

    # 6. XLSX multi-onglets
    buf = io.BytesIO()
    wb = xlsxwriter.Workbook(buf, {"remove_timezone": True})
    updates_rsc.write_excel(workbook=wb, worksheet="Lignes fusionnées")
    changements_puissance.write_excel(workbook=wb, worksheet="Changements puissance")
    wb.close()
    return buf.getvalue()
