"""
Configuration de l'API ElectriCore avec gestion des clés API.
Utilise Pydantic Settings pour une configuration basée sur les variables d'environnement.
"""

import secrets
import os
from pathlib import Path
from typing import Dict, List
from pydantic import BaseModel, Field, validator

# Charger le fichier .env s'il existe
def load_env_file():
    """Charge le fichier .env dans les variables d'environnement."""
    env_file = Path(".env")
    if env_file.exists():
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()

# Charger le .env au démarrage du module
load_env_file()


class APISettings(BaseModel):
    """
    Configuration de l'API avec support des clés API multiples.

    Les clés API peuvent être définies soit :
    - Comme une seule clé via API_KEY
    - Comme plusieurs clés via API_KEYS (séparées par des virgules)
    """

    # Configuration générale de l'API
    api_title: str = Field(default="ElectriCore API")
    api_version: str = Field(default="0.1.0")
    api_description: str = Field(default="API sécurisée pour accéder aux données flux Enedis")

    # Configuration des clés API
    api_key: str = Field(default="")
    api_keys: str = Field(default="")

    # Plus utilisé, gardé pour compatibilité
    enable_api_key_header: bool = Field(default=True)

    # Endpoints publics (sans authentification)
    public_endpoints: List[str] = Field(
        default=["/", "/health", "/docs", "/redoc", "/openapi.json"]
    )

    def __init__(self, **kwargs):
        # Charger depuis les variables d'environnement
        env_values = {
            "api_title": os.getenv("API_TITLE", "ElectriCore API"),
            "api_version": os.getenv("API_VERSION", "0.1.0"),
            "api_description": os.getenv("API_DESCRIPTION", "API sécurisée pour accéder aux données flux Enedis"),
            "api_key": os.getenv("API_KEY", ""),
            "api_keys": os.getenv("API_KEYS", ""),
            "enable_api_key_header": os.getenv("ENABLE_API_KEY_HEADER", "true").lower() == "true",
        }

        # Combiner avec les kwargs fournis
        env_values.update(kwargs)
        super().__init__(**env_values)

    @validator("api_keys", pre=True)
    def parse_api_keys(cls, v, values):
        """Parse les clés API multiples et combine avec la clé principale."""
        keys = []

        # Ajouter la clé principale si définie
        if values.get("api_key"):
            keys.append(values["api_key"])

        # Ajouter les clés multiples si définies
        if v:
            additional_keys = [k.strip() for k in v.split(",") if k.strip()]
            keys.extend(additional_keys)

        return ",".join(keys) if keys else ""

    def get_valid_api_keys(self) -> List[str]:
        """
        Retourne la liste des clés API valides.

        Returns:
            List[str]: Liste des clés API configurées
        """
        if not self.api_keys:
            return []
        return [k.strip() for k in self.api_keys.split(",") if k.strip()]

    def is_valid_api_key(self, key: str) -> bool:
        """
        Vérifie si une clé API est valide en utilisant une comparaison sécurisée.

        Args:
            key: Clé API à vérifier

        Returns:
            bool: True si la clé est valide
        """
        if not key:
            return False

        valid_keys = self.get_valid_api_keys()
        if not valid_keys:
            return False

        # Utilisation de secrets.compare_digest pour éviter les attaques de timing
        return any(secrets.compare_digest(key, valid_key) for valid_key in valid_keys)

    def generate_api_key(self) -> str:
        """
        Génère une nouvelle clé API sécurisée.

        Returns:
            str: Clé API générée
        """
        return secrets.token_urlsafe(32)


# Instance globale de la configuration
settings = APISettings()