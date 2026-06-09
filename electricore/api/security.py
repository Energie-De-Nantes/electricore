"""
Système de sécurité pour l'API ElectriCore.
Gestion de l'authentification par clés API via header X-API-Key uniquement.
"""

from dataclasses import dataclass

from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader

from electricore.api.config import settings

# Schéma de sécurité unique
api_key_header = APIKeyHeader(name="X-API-Key", description="Clé API dans le header X-API-Key")


def get_api_key(api_key: str | None = Security(api_key_header)) -> str:
    """
    Extrait et valide la clé API du header X-API-Key.

    Args:
        api_key: Clé API du header

    Returns:
        str: Clé API validée

    Raises:
        HTTPException: Si la clé API est manquante ou invalide
    """
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Clé API requise dans le header 'X-API-Key'",
            headers={"WWW-Authenticate": "APIKey"},
        )

    if not settings.is_valid_api_key(api_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Clé API invalide", headers={"WWW-Authenticate": "APIKey"}
        )

    return api_key


# Alias pour la compatibilité
get_current_api_key = get_api_key


def is_public_endpoint(path: str) -> bool:
    """
    Vérifie si un endpoint est public (sans authentification requise).

    Args:
        path: Chemin de l'endpoint

    Returns:
        bool: True si l'endpoint est public
    """
    return path in settings.public_endpoints


@dataclass(frozen=True, slots=True)
class APIKeyInfo:
    """Informations sur une clé API utilisée (pour logging/monitoring)."""

    key_preview: str
    is_valid: bool
    source: str = "header"

    @classmethod
    def from_key(cls, key: str) -> "APIKeyInfo":
        return cls(
            key_preview=f"{key[:8]}..." if len(key) > 8 else "***",
            is_valid=settings.is_valid_api_key(key),
        )


def get_api_key_info(api_key: str = Security(get_api_key)) -> APIKeyInfo:
    """Obtient les informations sur la clé API utilisée (pour monitoring)."""
    return APIKeyInfo.from_key(api_key)
