"""Décorateurs pour l'adaptateur Odoo (issue #67).

`@with_odoo` capture le pattern récurrent côté services HTTP : ouvrir
`OdooReader`, déléguer à une orchestration, fermer proprement. Permet aux
endpoints de consommer les orchestrations sans répéter le `with` block.
"""

import functools
import inspect
from collections.abc import Callable
from typing import Any

from electricore.api.config import settings

from .reader import OdooReader


def with_odoo[T](func: Callable[..., T]) -> Callable[..., T]:
    """Ouvre `OdooReader` avec la config courante et injecte-le en 1er arg.

    La fonction décorée prend `odoo` comme premier paramètre ; le wrapper
    expose la signature publique sans ce paramètre (utile pour FastAPI).

    Example:
        @with_odoo
        def calculer_accise_detail(odoo, trimestre=None):
            return accise_par_contrat(odoo, trimestre)

        # Au call-site :
        df = calculer_accise_detail(trimestre="2025-T1")  # odoo injecté
    """

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> T:
        with OdooReader(config=settings.get_odoo_config()) as odoo:
            return func(odoo, *args, **kwargs)

    sig = inspect.signature(func)
    wrapper.__signature__ = sig.replace(parameters=list(sig.parameters.values())[1:])
    return wrapper
