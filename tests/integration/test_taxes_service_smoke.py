"""Tests smoke de `taxes_service.calculer_cta_detail`.

Garantit que la fonction délègue le chargement de la facturation à
`charger_contexte_facturation` (issue #19) au lieu de reconstruire la trio
`c15 + releves_harmonises + facturation()`.
"""

from datetime import datetime

import polars as pl
import pytest

from electricore.api.services import taxes_service
from electricore.core.loaders.contexte_mensuel import ContexteFacturation


@pytest.fixture
def df_facturation_mensuelle_cta() -> pl.DataFrame:
    """Facturation mensuelle minimale exploitable par `ajouter_cta`."""
    return pl.DataFrame(
        {
            "pdl": ["12345678901234"],
            "ref_situation_contractuelle": ["RSC001"],
            "debut": [datetime(2025, 1, 1)],
            "fin": [datetime(2025, 2, 1)],
            "mois_annee": ["janvier 2025"],
            "turpe_fixe_eur": [42.50],
            "nb_jours": [31],
        }
    ).with_columns(
        pl.col("debut").dt.replace_time_zone("Europe/Paris"),
        pl.col("fin").dt.replace_time_zone("Europe/Paris"),
    )


def test_calculer_cta_detail_delegue_a_charger_contexte_facturation(monkeypatch, df_facturation_mensuelle_cta):
    """`calculer_cta_detail` consomme `ContexteFacturation` au lieu de
    reconstruire `c15 + releves + facturation()`.
    """
    contexte_prefab = ContexteFacturation(
        mois="2025-01-01",
        historique_enrichi=pl.LazyFrame(),
        facturation_mensuelle=df_facturation_mensuelle_cta,
    )
    appels_charger: list[str | None] = []

    def _capture_charger(mois):
        appels_charger.append(mois)
        return contexte_prefab

    monkeypatch.setattr(taxes_service, "charger_contexte_facturation", _capture_charger)

    # Stubs Odoo (le service charge encore les PDLs depuis sale.order).
    class _OdooReaderMock:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    class _QueryMock:
        def __init__(self, df: pl.DataFrame):
            self._df = df

        def filter(self, *args, **kwargs):
            return self

        def select(self, *args, **kwargs):
            return _QueryMock(self._df.select(*args, **kwargs))

        def collect(self) -> pl.DataFrame:
            return self._df

    df_pdl_brut = pl.DataFrame(
        {
            "x_pdl": ["12345678901234"],
            "name": ["SO/2025/0001"],
        }
    )

    monkeypatch.setattr(taxes_service, "OdooReader", _OdooReaderMock)
    monkeypatch.setattr(taxes_service, "query", lambda odoo, model, domain, fields: _QueryMock(df_pdl_brut))
    settings_cls = type(taxes_service.settings)
    monkeypatch.setattr(settings_cls, "get_odoo_config", lambda self: {})

    result = taxes_service.calculer_cta_detail()

    assert appels_charger == [None], "charger_contexte_facturation doit être appelé une fois"
    assert isinstance(result, pl.DataFrame)
    assert "cta_eur" in result.columns
