"""Façade de compatibilité pour la configuration Odoo (#141, ADR-0025).

Le registre runtime (`runtime.odoo()`) est la source de vérité ; cette façade
préserve l'API `charger_config_odoo()` documentée dans 8 notebooks in-repo.
"""

from electricore.config import runtime


def charger_config_odoo(env: str | None = None) -> dict:
    """
    Charge la configuration Odoo pour l'environnement spécifié.

    Args:
        env: "test" ou "prod". Si None, lit le sélecteur ODOO_ENV (défaut : "test").

    Returns:
        dict: Configuration avec les clés url, db, username, password.

    Raises:
        ConfigurationManquante: Si des variables ODOO_<ENV>_* manquent
            (sous-classe ValueError — compatible avec les `except ValueError`).

    Example:
        >>> from electricore.config import charger_config_odoo
        >>> config = charger_config_odoo()           # env depuis ODOO_ENV
        >>> config = charger_config_odoo("prod")     # forcer prod
        >>> with OdooReader(config=config) as odoo:
        ...     ...
    """
    return runtime.odoo(env).model_dump()
