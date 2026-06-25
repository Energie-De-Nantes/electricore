"""Façade de compatibilité pour la configuration Odoo (#141, ADR-0025).

Le registre runtime (`runtime.odoo()`) est la source de vérité ; cette façade
préserve l'API `charger_config_odoo()` documentée dans 8 notebooks in-repo.
"""

from electricore.config import runtime


def charger_config_odoo() -> dict:
    """
    Charge la configuration Odoo (bloc unique ODOO__*, #439, ADR-0046 §5).

    Returns:
        dict: Configuration avec les clés url, db, username, password.

    Raises:
        ConfigurationManquante: Si des variables ODOO__* manquent
            (sous-classe ValueError — compatible avec les `except ValueError`).

    Example:
        >>> from electricore.config import charger_config_odoo
        >>> config = charger_config_odoo()
        >>> with OdooReader(config=config) as odoo:
        ...     ...
    """
    return runtime.odoo().model_dump()
