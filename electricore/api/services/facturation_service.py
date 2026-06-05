"""Service de sérialisation de la facturation Odoo ↔ Enedis.

Le calcul et l'orchestration métier vivent dans
`electricore.integrations.odoo.facturation`. Ce service ne fait que :

1. Ouvrir la connexion `OdooReader` (responsabilité HTTP)
2. Déléguer à l'orchestration
3. Sérialiser le résultat (XLSX / Arrow IPC / ZIP)
"""

import io
import zipfile

import polars as pl
import xlsxwriter

from electricore.api.config import settings
from electricore.integrations.odoo import OdooReader
from electricore.integrations.odoo.facturation import (
    documents_facturation_du_mois,
    facturation_du_mois,
)


def calculer_lignes_facture_rapprochees(mois: str | None = None) -> pl.DataFrame:
    """Binding HTTP : ouvre la connexion Odoo + délègue à l'orchestration `facturation_du_mois`.

    Cette fonction existe comme seam testable au niveau du service (les endpoints peuvent
    la monkeypatch pour bypasser Odoo). La logique métier (chargement contexte + rapprochement)
    vit dans `electricore.integrations.odoo.facturation.facturation_du_mois`.

    Args:
        mois: format "YYYY-MM-DD" (premier jour du mois). None = dernier mois des données.
    """
    with OdooReader(config=settings.get_odoo_config()) as odoo:
        return facturation_du_mois(odoo, mois)


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
    """Génère un ZIP des 6 documents de campagne de facturation.

    Args:
        mois: format "YYYY-MM-DD" (premier jour du mois). None = dernier mois disponible.

    Returns:
        Tuple (zip_bytes, suffix) — suffix au format "YYYY-MM" pour le nom du fichier.
    """
    with OdooReader(config=settings.get_odoo_config()) as odoo:
        documents, suffix = documents_facturation_du_mois(odoo, mois)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, df in documents.items():
            zf.writestr(name, df.write_csv())
    return buf.getvalue(), suffix
