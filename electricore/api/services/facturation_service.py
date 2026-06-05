"""Service XLSX de facturation Odoo ↔ Enedis.

Le calcul métier (rapprochement) vit en core. Ce service ne gère que le
chargement des données et la sérialisation XLSX.
"""

import io
import zipfile

import polars as pl
import xlsxwriter

from electricore.api.config import settings
from electricore.core.loaders import c15, f15
from electricore.core.loaders.contexte_mensuel import charger_contexte_facturation
from electricore.core.pipelines.facturation import rapprocher_facturation_mensuelle
from electricore.integrations.odoo import OdooReader, lignes_factures_du_mois


def calculer_lignes_facture_rapprochees(mois: str | None = None) -> pl.DataFrame:
    """Charge Odoo + Enedis et applique `rapprocher_facturation_mensuelle`.

    Args:
        mois: format "YYYY-MM-DD" (premier jour du mois). None = dernier mois des données.
    """
    contexte = charger_contexte_facturation(mois)

    with OdooReader(config=settings.get_odoo_config()) as odoo:
        lignes_df = lignes_factures_du_mois(odoo, contexte.mois).collect()

    return rapprocher_facturation_mensuelle(
        lignes_odoo=lignes_df,
        fact_mensuelle=contexte.facturation_mensuelle.lazy(),
        historique=contexte.historique_enrichi,
        mois=contexte.mois,
    )


def generer_facturation_xlsx(mois: str | None = None) -> bytes:
    """Réconciliation Odoo ↔ Enedis du mois (défaut : dernier mois disponible).

    Args:
        mois: format "YYYY-MM-DD" (premier jour du mois). None = dernier mois des données.

    Returns:
        XLSX bytes — 2 onglets : "Lignes fusionnées" et "Changements puissance".
    """
    lignes_rapprochees = calculer_lignes_facture_rapprochees(mois)
    changements_puissance = lignes_rapprochees.filter(pl.col("memo_puissance") != "")

    buf = io.BytesIO()
    wb = xlsxwriter.Workbook(buf, {"remove_timezone": True})
    lignes_rapprochees.write_excel(workbook=wb, worksheet="Lignes fusionnées")
    changements_puissance.write_excel(workbook=wb, worksheet="Changements puissance")
    wb.close()
    return buf.getvalue()


def generer_facturation_arrow(mois: str | None = None) -> bytes:
    """Sérialise `lignes_facture_rapprochees` en flux Arrow IPC."""
    lignes_rapprochees = calculer_lignes_facture_rapprochees(mois)
    buf = io.BytesIO()
    lignes_rapprochees.write_ipc_stream(buf)
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
    # 1. Contexte mensuel (mois résolu + facturation Enedis exécutée une fois)
    contexte = charger_contexte_facturation(mois)
    suffix = contexte.mois[:7]
    mois_date = pl.lit(contexte.mois).str.to_date()

    # 2. Lignes Odoo du mois cible (toutes, avec flags a_facturer / a_supprimer)
    with OdooReader(config=settings.get_odoo_config()) as odoo:
        lignes_df = lignes_factures_du_mois(odoo, contexte.mois).collect()

    # 3. Rapprochement Odoo ↔ Enedis (même fonction core que XLSX / Arrow)
    reconciliation = rapprocher_facturation_mensuelle(
        lignes_odoo=lignes_df,
        fact_mensuelle=contexte.facturation_mensuelle.lazy(),
        historique=contexte.historique_enrichi,
        mois=contexte.mois,
    )
    changements_puissance = reconciliation.filter(pl.col("memo_puissance") != "")

    # 4. F15 du mois (CSV brut, indépendant du contexte de facturation)
    f15_df = f15().lazy().filter(pl.col("date_facture").dt.truncate("1mo").dt.date() == mois_date).collect()
    f15_prestas = f15_df.filter(pl.col("unite") == "UNITE")

    # 5. C15 du mois (CSV brut — la version enrichie est dans le contexte mais
    #    on veut ici le flux non transformé pour audit)
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
