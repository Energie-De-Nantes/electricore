"""Service XLSX de facturation Odoo ↔ Enedis.

Le calcul métier (rapprochement) vit en core. Ce service ne gère que le
chargement des données et la sérialisation XLSX.
"""

import io
import zipfile

import polars as pl
import xlsxwriter

from electricore.api.config import settings
from electricore.core.loaders import OdooReader, c15, f15, lignes_a_facturer, releves_harmonises
from electricore.core.pipelines.facturation import rapprocher_facturation_mensuelle
from electricore.core.pipelines.orchestration import facturation


def calculer_lignes_facture_rapprochees(mois: str | None = None) -> pl.DataFrame:
    """Charge Odoo + Enedis et applique `rapprocher_facturation_mensuelle`.

    Args:
        mois: format "YYYY-MM-DD" (premier jour du mois). None = dernier mois des données.
    """
    with OdooReader(config=settings.get_odoo_config()) as odoo:
        lignes_df = lignes_a_facturer(odoo).collect()

    _, _, _, fact = facturation(historique=c15().lazy(), releves=releves_harmonises().lazy())

    return rapprocher_facturation_mensuelle(lignes_odoo=lignes_df, fact_mensuelle=fact.lazy(), mois=mois)


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
    # 1. Lignes Odoo en brouillon
    with OdooReader(config=settings.get_odoo_config()) as odoo:
        lignes_df = lignes_a_facturer(odoo).collect()

    # 2. Pipeline facturation Enedis (un seul calcul, partagé pour le rapprochement + le filtre F15/C15)
    lf_historique = c15().lazy()
    lf_releves = releves_harmonises().lazy()
    _, _, _, fact = facturation(historique=lf_historique, releves=lf_releves)

    # 3. Résoudre le mois cible (None → dernier mois disponible)
    debut_mois_expr = pl.col("debut").dt.truncate("1mo").dt.date()
    if mois is None:
        mois = str(fact.select(debut_mois_expr.alias("m"))["m"].max())
    suffix = mois[:7]
    mois_date = pl.lit(mois).str.to_date()

    # 4. Rapprochement Odoo ↔ Enedis (même fonction core que XLSX / Arrow)
    reconciliation = rapprocher_facturation_mensuelle(lignes_odoo=lignes_df, fact_mensuelle=fact.lazy(), mois=mois)
    changements_puissance = reconciliation.filter(pl.col("memo_puissance") != "")

    # 5. F15 du mois
    f15_df = f15().lazy().filter(pl.col("date_facture").dt.truncate("1mo").dt.date() == mois_date).collect()
    f15_prestas = f15_df.filter(pl.col("unite") == "UNITE")

    # 6. C15 du mois
    c15_df = lf_historique.filter(pl.col("date_evenement").dt.truncate("1mo").dt.date() == mois_date).collect()
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
