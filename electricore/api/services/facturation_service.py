"""
Service de réconciliation facturation Odoo ↔ Enedis.
Produit updates_rsc sans écriture dans Odoo.
"""

import io
import zipfile

import polars as pl
import xlsxwriter

from electricore.api.config import settings
from electricore.core.loaders import OdooReader, c15, f15, lignes_a_facturer, releves_harmonises
from electricore.core.pipelines.orchestration import facturation

MAPPING_CATEGORIE = {
    "HP": "energie_hp_kwh",
    "HC": "energie_hc_kwh",
    "Base": "energie_base_kwh",
    "Abonnements": "nb_jours",
}


def generer_facturation_xlsx(mois: str | None = None) -> bytes:
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
    quantite_enedis_expr = pl.coalesce(
        [
            pl.when(pl.col("name_product_category") == cat).then(pl.col(col).cast(pl.Float64))
            for cat, col in MAPPING_CATEGORIE.items()
        ]
    ).alias("quantite_enedis")

    updates_rsc = (
        lignes_df.join(
            fact_mois,
            left_on="x_ref_situation_contractuelle",
            right_on="ref_situation_contractuelle",
            how="left",
        )
        .with_columns(quantite_enedis_expr)
        .select(
            [
                "invoice_line_ids",
                "x_pdl",
                "x_lisse",
                "name_account_move",
                "name_product_category",
                "name_product_product",
                "quantity",
                "quantite_enedis",
                "memo_puissance",
            ]
        )
    )

    changements_puissance = updates_rsc.filter(pl.col("memo_puissance") != "")

    # 6. XLSX multi-onglets (compatibilité ascendante)
    buf = io.BytesIO()
    wb = xlsxwriter.Workbook(buf, {"remove_timezone": True})
    updates_rsc.write_excel(workbook=wb, worksheet="Lignes fusionnées")
    changements_puissance.write_excel(workbook=wb, worksheet="Changements puissance")
    wb.close()
    return buf.getvalue()


def generer_documents_facturation(mois: str | None = None) -> tuple[bytes, str]:
    """
    Génère un ZIP avec les 6 documents utiles pour la facturation.

    Args:
        mois: format "YYYY-MM-DD" (premier jour du mois). None = dernier mois disponible.

    Returns:
        Tuple (zip_bytes, suffix) — suffix au format "YYYY-MM" pour le nom du fichier.

    Contenu du ZIP :
    - f15_complet.csv
    - f15_prestas.csv  (filtre unite = 'UNITE')
    - c15_complet.csv
    - c15_sorties.csv  (filtre evenement_declencheur IN ('RES', 'CFNS'))
    - reconciliation.csv
    - changements_puissance.csv
    """
    # 1. Lignes Odoo en brouillon
    with OdooReader(config=settings.get_odoo_config()) as odoo:
        lignes_df = lignes_a_facturer(odoo).collect()

    # 2. Pipeline facturation Enedis
    lf_historique = c15().lazy()
    lf_releves = releves_harmonises().lazy()
    _, _, _, fact = facturation(historique=lf_historique, releves=lf_releves)

    # 3. Mois cible
    debut_date_mois = pl.col("debut").dt.truncate("1mo").dt.date()
    if mois is not None:
        mois_cible = pl.lit(mois).str.to_date()
        fact_mois = fact.filter(debut_date_mois == mois_cible)
    else:
        mois_en_cours = fact.select(debut_date_mois.alias("m"))["m"].max()
        fact_mois = fact.filter(debut_date_mois == mois_en_cours)
        mois = str(mois_en_cours)

    suffix = mois[:7]  # YYYY-MM

    # 4. Réconciliation Odoo ↔ Enedis
    quantite_enedis_expr = pl.coalesce(
        [
            pl.when(pl.col("name_product_category") == cat).then(pl.col(col).cast(pl.Float64))
            for cat, col in MAPPING_CATEGORIE.items()
        ]
    ).alias("quantite_enedis")

    reconciliation = (
        lignes_df.join(
            fact_mois, left_on="x_ref_situation_contractuelle", right_on="ref_situation_contractuelle", how="left"
        )
        .with_columns(quantite_enedis_expr)
        .select(
            [
                "invoice_line_ids",
                "x_pdl",
                "x_lisse",
                "name_account_move",
                "name_product_category",
                "name_product_product",
                "quantity",
                "quantite_enedis",
                "memo_puissance",
            ]
        )
    )
    changements_puissance = reconciliation.filter(pl.col("memo_puissance") != "")

    # 5. F15 du mois
    mois_date = pl.lit(mois).str.to_date()
    f15_df = f15().lazy().filter(pl.col("date_facture").dt.truncate("1mo").dt.date() == mois_date).collect()
    f15_prestas = f15_df.filter(pl.col("unite") == "UNITE")

    # 6. C15 du mois
    c15_df = c15().lazy().filter(pl.col("date_evenement").dt.truncate("1mo").dt.date() == mois_date).collect()
    c15_sorties = c15_df.filter(pl.col("evenement_declencheur").is_in(["RES", "CFNS"]))

    # 7. ZIP
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("f15_complet.csv", f15_df.write_csv())
        zf.writestr("f15_prestas.csv", f15_prestas.write_csv())
        zf.writestr("c15_complet.csv", c15_df.write_csv())
        zf.writestr("c15_sorties.csv", c15_sorties.write_csv())
        zf.writestr("reconciliation.csv", reconciliation.write_csv())
        zf.writestr("changements_puissance.csv", changements_puissance.write_csv())
    return buf.getvalue(), suffix
