"""Router des endpoints d'administration de l'API (issue #82)."""

from fastapi import APIRouter, Depends

from electricore.api.config import settings
from electricore.api.security import PUBLIC_ENDPOINTS, APIKeyInfo, get_api_key_info, get_current_api_key

router = APIRouter(tags=["admin"])


@router.get("/admin/api-keys")
async def list_api_keys(api_key: str = Depends(get_current_api_key), key_info: APIKeyInfo = Depends(get_api_key_info)):
    """
    Informations sur la configuration des clés API.

    **Authentification requise** - Endpoint d'administration.

    Returns:
        Dict avec les informations sur les clés API configurées
    """
    return {
        "message": "Configuration des clés API",
        "current_key": {"preview": key_info.key_preview, "source": key_info.source},
        "configuration": {
            "total_keys": len(settings.get_valid_api_keys()),
            "method": "X-API-Key header",
            "public_endpoints": PUBLIC_ENDPOINTS,
        },
    }
