"""Modèle de contrat des prestations F15 à refacturer (contrat v1, souscriptions_odoo#37).

Miroir de `prestations_service.COLONNES_CONTRAT` + `reference` (référence de
contenu electricore, clé de dédup calculée serveur : le F15 n'a pas
d'identifiant de ligne, cf. service et `docs/contrat-prestations.md`). Les
dates F15 sont des JOURS CIVILS rendus en ISO (`YYYY-MM-DD`), pas des instants.

`taux_tva_applicable` reste la chaîne Enedis brute (`'NS'` = non soumis, sinon
un taux) : c'est le consommateur qui en dérive sa classification (côté
souscriptions_odoo : nature prestation taxée / indemnité, ADR 0009 §5 addon).

`model_config = extra="ignore"` : le contrat est additif-tolérant — un serveur
plus récent qui ajoute une colonne ne casse pas un client plus ancien.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

# Version du contrat (cf. prestations_service.CONTRAT_VERSION). Source unique.
CONTRAT_VERSION_PRESTATIONS = 1


class PrestationF15(BaseModel):
    """Une prestation ou indemnité ponctuelle du flux F15 (`unite = 'UNITE'`)."""

    model_config = ConfigDict(extra="ignore")

    reference: str
    pdl: str | None = None
    ref_situation_contractuelle: str | None = None
    id_ev: str | None = None
    nature_ev: str | None = None
    libelle_ev: str | None = None
    taux_tva_applicable: str | None = None
    prix_unitaire: float | None = None
    quantite: float | None = None
    montant_ht: float | None = None
    date_debut: str | None = None
    date_fin: str | None = None
    num_facture: str | None = None
    date_facture: str | None = None
