"""
Configuration de l'API ElectriCore avec gestion des clés API.
Utilise Pydantic Settings pour une configuration basée sur les variables d'environnement.
"""

import secrets
import os
from typing import Dict, List
from pydantic import BaseModel, Field, validator

from electricore.config.env import charger_env

# Charger le .env au démarrage du module
charger_env()


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

    # Configuration Telegram
    telegram_bot_token: str = Field(default="")
    api_base_url: str = Field(default="http://localhost:8001")
    telegram_allowed_users: str = Field(default="")  # IDs séparés par virgule

    # Endpoints publics (sans authentification)
    public_endpoints: List[str] = Field(
        default=["/", "/health", "/docs", "/redoc", "/openapi.json"]
    )

    # Environnement Odoo actif ("test" ou "prod")
    odoo_env: str = Field(default="test")

    @property
    def is_odoo_configured(self) -> bool:
        """Vérifie que la config Odoo de l'environnement actif est complète."""
        try:
            self.get_odoo_config()
            return True
        except (ValueError, Exception):
            return False

    def get_odoo_config(self) -> Dict[str, str]:
        """Retourne la config Odoo sous forme de dict compatible avec OdooReader."""
        from electricore.config.odoo import charger_config_odoo
        return charger_config_odoo(env=self.odoo_env)

    def __init__(self, **kwargs):
        # Charger depuis les variables d'environnement
        env_values = {
            "api_title": os.getenv("API_TITLE", "ElectriCore API"),
            "api_version": os.getenv("API_VERSION", "0.1.0"),
            "api_description": os.getenv("API_DESCRIPTION", "API sécurisée pour accéder aux données flux Enedis"),
            "api_key": os.getenv("API_KEY", ""),
            "api_keys": os.getenv("API_KEYS", ""),
            "enable_api_key_header": os.getenv("ENABLE_API_KEY_HEADER", "true").lower() == "true",
            "telegram_bot_token": os.getenv("TELEGRAM_BOT_TOKEN", ""),
            "api_base_url": os.getenv("API_BASE_URL", "http://localhost:8001"),
            "telegram_allowed_users": os.getenv("TELEGRAM_ALLOWED_USERS", ""),
            "odoo_env": os.getenv("ODOO_ENV", "test"),
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

    def get_telegram_allowed_users(self) -> set[int]:
        """Retourne l'ensemble des user IDs Telegram autorisés."""
        if not self.telegram_allowed_users:
            return set()
        return {int(uid.strip()) for uid in self.telegram_allowed_users.split(",") if uid.strip().isdigit()}

    def generate_api_key(self) -> str:
        """
        Génère une nouvelle clé API sécurisée.

        Returns:
            str: Clé API générée
        """
        return secrets.token_urlsafe(32)


# Instance globale de la configuration
settings = APISettings()
