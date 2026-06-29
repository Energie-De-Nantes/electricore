"""
Système de sécurité pour l'API ElectriCore.
Gestion de l'authentification par clés API via header X-API-Key uniquement.
"""

from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader

from electricore.config import runtime

# Endpoints publics (sans authentification) — constante, pas de la config env (ADR-0025).
PUBLIC_ENDPOINTS = ["/", "/health", "/docs", "/redoc", "/openapi.json"]

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

    if not runtime.api().cle_valide(api_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Clé API invalide", headers={"WWW-Authenticate": "APIKey"}
        )

    return api_key


# Alias pour la compatibilité
get_current_api_key = get_api_key
