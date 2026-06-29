"""
Modèles de données pour l'API ElectriCore.
Définit les structures de données Pydantic pour les réponses API.
"""

from datetime import date, datetime

from pydantic import BaseModel, ConfigDict, Field

# Modèles ingestion


class IngestionRunRequest(BaseModel):
    """Corps de la requête pour lancer le pipeline d'ingestion."""

    mode: str = Field("test", description="Mode d'exécution : test | r151 | all | reset")


class IngestionJobResponse(BaseModel):
    """Statut d'un job d'ingestion."""

    model_config = ConfigDict(from_attributes=True)

    id: str = Field(..., description="Identifiant unique du job")
    mode: str = Field(..., description="Mode d'exécution utilisé")
    status: str = Field(..., description="Statut : running | completed | failed")
    started_at: datetime = Field(..., description="Horodatage de démarrage")
    finished_at: datetime | None = Field(None, description="Horodatage de fin")
    error: str | None = Field(None, description="Message d'erreur si failed")
    output: str | None = Field(None, description="Sortie stdout/stderr du pipeline")


# Modèles taxes


class MillesimeResponse(BaseModel):
    """Millésime d'un registre de taux régulés (ADR-0024) : dernière ligne entrée en vigueur."""

    taxe: str = Field(..., description="Registre : TURPE | Accise | CTA")
    date_vigueur: date = Field(..., description="Date d'entrée en vigueur de la dernière ligne")
    reference: str = Field(..., description="Référence réglementaire (texte fondateur + lien public)")
    valeur: float | None = Field(None, description="Taux scalaire si la taxe en a un (None pour une grille)")
    unite: str | None = Field(None, description="Unité du taux scalaire (€/MWh, %)")


class PeremptionResponse(BaseModel):
    """Avertissement de péremption d'un taux régulé (#186, ADR-0024) — warning, jamais d'auto-correction."""

    taxe: str = Field(..., description="Registre concerné : TURPE | Accise")
    attendu_depuis: date = Field(..., description="Jalon du rythme attendu resté sans nouvelle ligne")
    dernier_start: date = Field(..., description="Dernière entrée en vigueur connue")
    consigne: str = Field(..., description="Quoi vérifier (délibération CRE, loi de finances)")
    message: str = Field(..., description="Message prêt à afficher")
