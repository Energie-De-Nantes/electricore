"""Service de sérialisation pour `/facturation/documents` (ZIP CSV).

Volet rapport/detail migré (issue #77) : les endpoints `/facturation/rapport.xlsx`,
`/facturation/detail.xlsx`, `/facturation/detail.arrow` consomment désormais
directement `feuilles_rapport_facturation` + `facturation_du_mois` côté
`integrations.odoo.facturation`, sans couche service intermédiaire.

Le wrapper `_documents_facturation` reste ici tant que `/facturation/documents`
n'est pas migré au format XLSX multi-onglets (issue #78).
"""

from electricore.api.serializers import zip_csv
from electricore.integrations.odoo.decorators import with_odoo
from electricore.integrations.odoo.facturation import documents_facturation_du_mois


@with_odoo
def _documents_facturation(odoo, mois: str | None = None):
    return documents_facturation_du_mois(odoo, mois)


def generer_documents_facturation(mois: str | None = None) -> tuple[bytes, str]:
    """Génère un ZIP des 6 documents de campagne de facturation.

    Args:
        mois: format "YYYY-MM-DD" (premier jour du mois). None = dernier mois disponible.

    Returns:
        Tuple (zip_bytes, suffix) — suffix au format "YYYY-MM" pour le nom du fichier.
    """
    documents, suffix = _documents_facturation(mois)
    return zip_csv(documents), suffix
