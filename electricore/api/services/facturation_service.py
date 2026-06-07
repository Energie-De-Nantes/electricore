"""Service de sérialisation de la facturation Odoo ↔ Enedis.

Le calcul et l'orchestration métier vivent dans
`electricore.integrations.odoo.facturation`. Ce service ne fait que :

1. Ouvrir la connexion `OdooReader` (responsabilité HTTP)
2. Déléguer à `facturation_du_mois` ou `rapport_facturation`
3. Sérialiser le résultat via `api/serializers/` (XLSX / Arrow IPC / ZIP)

La shape du livrable (`Résumé` / `Lignes` / `Changements puissance`) vit
désormais dans `integrations.odoo.facturation.rapport_facturation` (#64).
"""

import polars as pl

from electricore.api.config import settings
from electricore.api.serializers import arrow_stream, xlsx_multi_sheet, zip_csv
from electricore.integrations.odoo import OdooReader
from electricore.integrations.odoo.facturation import (
    documents_facturation_du_mois,
    facturation_du_mois,
    rapport_facturation,
)


def calculer_lignes_facture_rapprochees(mois: str | None = None) -> pl.DataFrame:
    """Binding HTTP : ouvre Odoo + délègue à `facturation_du_mois`."""
    with OdooReader(config=settings.get_odoo_config()) as odoo:
        return facturation_du_mois(odoo, mois)


def generer_facturation_detail_arrow(mois: str | None = None) -> bytes:
    """Sérialise les lignes brutes du mois en flux Arrow IPC (cas technique)."""
    return arrow_stream(calculer_lignes_facture_rapprochees(mois))


def generer_facturation_detail_xlsx(mois: str | None = None) -> bytes:
    """Sérialise les lignes brutes du mois en XLSX mono-onglet (cas technique)."""
    return xlsx_multi_sheet({"Détail": calculer_lignes_facture_rapprochees(mois)})


def generer_facturation_rapport_xlsx(mois: str | None = None) -> bytes:
    """Livrable facturiste : 3 onglets (Résumé / Lignes / Changements puissance)."""
    with OdooReader(config=settings.get_odoo_config()) as odoo:
        r = rapport_facturation(odoo, mois)
    return xlsx_multi_sheet(
        {
            "Résumé": r.resume,
            "Lignes": r.lignes,
            "Changements puissance": r.changements_puissance,
        }
    )


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
