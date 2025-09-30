"""
Configuration et gestion des connexions Odoo.

Ce module fournit les primitives de configuration pour l'accès
aux serveurs Odoo dans un style fonctionnel immutable.
"""

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass(frozen=True)
class OdooConfig:
    """
    Configuration immutable pour les connexions Odoo.

    Attributes:
        url: URL du serveur Odoo (ex: 'https://mycompany.odoo.com')
        db: Nom de la base de données
        username: Nom d'utilisateur
        password: Mot de passe API
    """
    url: str
    db: str
    username: str
    password: str

    @classmethod
    def from_dict(cls, config: Dict[str, str]) -> 'OdooConfig':
        """
        Crée une OdooConfig depuis un dictionnaire de configuration.

        Args:
            config: Dictionnaire avec clés 'url', 'db', 'username', 'password'
                   (ou variantes 'ODOO_URL', 'ODOO_DB', etc.)

        Returns:
            OdooConfig validée

        Raises:
            ValueError: Si des paramètres requis manquent

        Example:
            >>> config = OdooConfig.from_dict({
            ...     'url': 'https://demo.odoo.com',
            ...     'db': 'demo',
            ...     'username': 'admin',
            ...     'password': 'admin'
            ... })
        """
        url = config.get('url') or config.get('ODOO_URL')
        db = config.get('db') or config.get('ODOO_DB')
        username = config.get('username') or config.get('ODOO_USERNAME')
        password = config.get('password') or config.get('ODOO_PASSWORD')

        # Validation
        missing = []
        if not url: missing.append('url')
        if not db: missing.append('db')
        if not username: missing.append('username')
        if not password: missing.append('password')

        if missing:
            raise ValueError(
                f"Paramètres manquants dans la configuration: {', '.join(missing)}"
            )

        return cls(
            url=url,
            db=db,
            username=username,
            password=password
        )


class FieldsCache:
    """
    Cache mutable pour les métadonnées des champs Odoo.

    Améliore les performances en évitant les appels répétés à fields_get.
    """

    def __init__(self):
        """Initialise un cache vide."""
        self._cache: Dict[str, Dict[str, Dict]] = {}

    def get(self, model: str, field_name: str) -> Optional[Dict]:
        """
        Récupère les métadonnées d'un champ depuis le cache.

        Args:
            model: Modèle Odoo (ex: 'sale.order')
            field_name: Nom du champ (ex: 'partner_id')

        Returns:
            Dict avec métadonnées du champ ou None si absent du cache
        """
        return self._cache.get(model, {}).get(field_name)

    def set(self, model: str, field_name: str, field_info: Optional[Dict]) -> None:
        """
        Stocke les métadonnées d'un champ dans le cache.

        Args:
            model: Modèle Odoo
            field_name: Nom du champ
            field_info: Métadonnées du champ (ou None si non trouvé)
        """
        if model not in self._cache:
            self._cache[model] = {}
        self._cache[model][field_name] = field_info

    def clear(self) -> None:
        """Vide complètement le cache."""
        self._cache.clear()