"""
Configuration et gestion des connexions Odoo.

Ce module fournit les primitives de configuration pour l'accès
aux serveurs Odoo dans un style fonctionnel immutable.
"""


class FieldsCache:
    """
    Cache mutable pour les métadonnées des champs Odoo.

    Améliore les performances en évitant les appels répétés à fields_get.
    """

    def __init__(self) -> None:
        """Initialise un cache vide."""
        self._cache: dict[str, dict[str, dict | None]] = {}

    def get(self, model: str, field_name: str) -> dict | None:
        """
        Récupère les métadonnées d'un champ depuis le cache.

        Args:
            model: Modèle Odoo (ex: 'sale.order')
            field_name: Nom du champ (ex: 'partner_id')

        Returns:
            Dict avec métadonnées du champ ou None si absent du cache
        """
        return self._cache.get(model, {}).get(field_name)

    def set(self, model: str, field_name: str, field_info: dict | None) -> None:
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
