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
        @xlsx_endpoint(app, "/taxes/accise/detail.xlsx", ...)
        @with_odoo
        def export_accise_detail_xlsx(odoo, trimestre=None) -> bytes:
            return xlsx_multi_sheet({"Détail": accise_par_contrat(odoo, trimestre)})

        # FastAPI ne voit que `trimestre` dans la signature exposée (cf. issue #74).
    """

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> T:
        with OdooReader(config=settings.get_odoo_config()) as odoo:
            return func(odoo, *args, **kwargs)

    sig = inspect.signature(func)
    wrapper.__signature__ = sig.replace(parameters=list(sig.parameters.values())[1:])
    return wrapper
