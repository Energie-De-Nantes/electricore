"""
Modèles de données pour l'API ElectriCore.
Définit les structures de données Pydantic pour les réponses API.
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class APIResponse(BaseModel):
    """Modèle de base pour toutes les réponses API."""

    message: str = Field(..., description="Message descriptif de la réponse")
    timestamp: datetime = Field(default_factory=datetime.now, description="Horodatage de la réponse")


class FluxData(BaseModel):
    """Modèle pour les données de flux Enedis."""

    table: str = Field(..., description="Nom de la table flux")
    filters: dict[str, Any] | None = Field(None, description="Filtres appliqués")
    pagination: dict[str, int] = Field(..., description="Informations de pagination")
    data: list[dict[str, Any]] = Field(..., description="Données du flux")


class TableInfo(BaseModel):
    """Modèle pour les informations sur une table."""

    table: str = Field(..., description="Nom complet de la table")
    db_schema: str = Field(..., description="Schéma de la table")
    count: int = Field(..., description="Nombre total de lignes")
    columns: list[dict[str, str]] = Field(..., description="Liste des colonnes avec leurs types")


class APIKeyConfiguration(BaseModel):
    """Modèle pour la configuration des clés API."""

    total_keys: int = Field(..., description="Nombre total de clés configurées")
    methods_enabled: dict[str, bool] = Field(..., description="Méthodes d'authentification activées")
    public_endpoints: list[str] = Field(..., description="Liste des endpoints publics")


class APIKeyInfo(BaseModel):
    """Modèle pour les informations sur une clé API utilisée."""

    preview: str = Field(..., description="Aperçu de la clé API (masquée)")
    source: str = Field(..., description="Source de la clé (header/query)")
    is_valid: bool = Field(..., description="Validité de la clé")


class HealthStatus(BaseModel):
    """Modèle pour le statut de santé de l'API."""

    status: str = Field(..., description="Statut global de l'API")
    api_version: str = Field(..., description="Version de l'API")
    database: str = Field(..., description="Statut de la base de données")
    tables_count: int = Field(..., description="Nombre de tables disponibles")
    authentication: dict[str, Any] = Field(..., description="Configuration de l'authentification")


class APIError(BaseModel):
    """Modèle pour les erreurs API."""

    error: str = Field(..., description="Type d'erreur")
    message: str = Field(..., description="Message d'erreur détaillé")
    details: dict[str, Any] | None = Field(None, description="Détails supplémentaires sur l'erreur")


class WelcomeMessage(BaseModel):
    """Modèle pour le message d'accueil de l'API."""

    message: str = Field(..., description="Message de bienvenue")
    version: str = Field(..., description="Version de l'API")
    authentication: dict[str, Any] = Field(..., description="Informations d'authentification")
    available_tables: list[str] = Field(..., description="Tables disponibles")
    examples: dict[str, str] = Field(..., description="Exemples d'utilisation")
    docs: str = Field(..., description="URL de la documentation")


# Modèles ingestion


class IngestionRunRequest(BaseModel):
    """Corps de la requête pour lancer le pipeline d'ingestion."""

    mode: str = Field("test", description="Mode d'exécution : test | r151 | all | reset")


class IngestionJobResponse(BaseModel):
    """Statut d'un job d'ingestion."""

    id: str = Field(..., description="Identifiant unique du job")
    mode: str = Field(..., description="Mode d'exécution utilisé")
    status: str = Field(..., description="Statut : running | completed | failed")
    started_at: datetime = Field(..., description="Horodatage de démarrage")
    finished_at: datetime | None = Field(None, description="Horodatage de fin")
    error: str | None = Field(None, description="Message d'erreur si failed")
    output: str | None = Field(None, description="Sortie stdout/stderr du pipeline")


# Modèles pour les requêtes (si nécessaire pour des POST/PUT futurs)


class FluxQuery(BaseModel):
    """Modèle pour les requêtes de flux avancées."""

    table_name: str = Field(..., description="Nom de la table à interroger")
    filters: dict[str, Any] | None = Field(None, description="Filtres à appliquer")
    columns: list[str] | None = Field(None, description="Colonnes à sélectionner")
    limit: int = Field(100, ge=1, le=1000, description="Nombre maximum de résultats")
    offset: int = Field(0, ge=0, description="Décalage pour la pagination")
    order_by: str | None = Field(None, description="Colonne pour le tri")


class APIKeyRequest(BaseModel):
    """Modèle pour les requêtes de gestion des clés API (futur)."""

    name: str | None = Field(None, description="Nom descriptif de la clé API")
    permissions: list[str] = Field(default=["read"], description="Permissions accordées")
    expires_at: datetime | None = Field(None, description="Date d'expiration optionnelle")


class APIKeyResponse(BaseModel):
    """Modèle pour les réponses de création de clés API (futur)."""

    api_key: str = Field(..., description="Clé API générée")
    name: str | None = Field(None, description="Nom descriptif")
    permissions: list[str] = Field(..., description="Permissions accordées")
    created_at: datetime = Field(default_factory=datetime.now, description="Date de création")
    expires_at: datetime | None = Field(None, description="Date d'expiration")
