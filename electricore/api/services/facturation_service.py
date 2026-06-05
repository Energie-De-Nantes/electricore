"""Service de sérialisation de la facturation Odoo ↔ Enedis.

Le calcul et l'orchestration métier vivent dans
`electricore.integrations.odoo.facturation`. Ce service ne fait que :

1. Ouvrir la connexion `OdooReader` (responsabilité HTTP)
2. Déléguer à l'orchestration
3. Sérialiser le résultat via `api/serializers/` (XLSX / Arrow IPC / ZIP)
"""

import polars as pl

from electricore.api.config import settings
from electricore.api.serializers import arrow_stream, xlsx_multi_sheet, zip_csv
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
    """Réconciliation Odoo ↔ Enedis du mois sérialisée en XLSX (2 onglets)."""
    lignes_rapprochees = calculer_lignes_facture_rapprochees(mois)
    changements_puissance = lignes_rapprochees.filter(pl.col("memo_puissance") != "")
    return xlsx_multi_sheet(
        {
            "Lignes fusionnées": lignes_rapprochees,
            "Changements puissance": changements_puissance,
        }
    )


def generer_facturation_arrow(mois: str | None = None) -> bytes:
    """Sérialise `lignes_facture_rapprochees` en flux Arrow IPC."""
    return arrow_stream(calculer_lignes_facture_rapprochees(mois))


def generer_documents_facturation(mois: str | None = None) -> tuple[bytes, str]:
    """Génère un ZIP des 6 documents de campagne de facturation.

    Args:
        mois: format "YYYY-MM-DD" (premier jour du mois). None = dernier mois disponible.

    Returns:
        Tuple (zip_bytes, suffix) — suffix au format "YYYY-MM" pour le nom du fichier.
    """
    with OdooReader(config=settings.get_odoo_config()) as odoo:
        documents, suffix = documents_facturation_du_mois(odoo, mois)
    return zip_csv(documents), suffix
