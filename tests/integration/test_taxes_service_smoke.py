"""Tests smoke de `cta_par_contrat` (orchestration CTA).

Garantit que l'orchestration délègue le chargement de la facturation à
`core.orchestrations.contexte_mensuel.charger` plutôt que de reconstruire la
trio `c15 + releves_harmonises + facturation()`.

Depuis #77, `taxes_service.calculer_cta_detail` a disparu — l'endpoint
appelle directement `cta_par_contrat` côté `integrations.odoo.taxes`.
Depuis #87 (slice 1), la délégation passe par la nouvelle orchestration
`core/orchestrations/contexte_mensuel.py`.
"""

from contextlib import contextmanager
from datetime import datetime

import polars as pl
import pytest

from electricore.core.orchestrations.contexte_mensuel import ContexteMensuel
from electricore.integrations.odoo import taxes as taxes_orchestration


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


def test_cta_par_contrat_delegue_a_charger(monkeypatch, df_facturation_mensuelle_cta):
    """`cta_par_contrat` consomme `ContexteMensuel` via `charger()` (ADR-0016, #87).

    Invariant : l'orchestration délègue la composition à l'orchestration
    partagée du `core/`, sans reconstruire `c15 + releves + facturation()`.
    """
    contexte_prefab = ContexteMensuel(
        mois="2025-01-01",
        historique_enrichi=pl.LazyFrame(),
        abonnements=pl.LazyFrame(),
        energie=pl.LazyFrame(),
        facturation_mensuelle=df_facturation_mensuelle_cta,
    )
    appels_charger: list[tuple[object, object, object]] = []

    def _capture_charger(historique, releves, mois=None):
        appels_charger.append((historique, releves, mois))
        return contexte_prefab

    monkeypatch.setattr(taxes_orchestration, "charger", _capture_charger)

    # Les loaders sont évalués côté caller AVANT `charger()` (nouvelle topologie #87).
    # On les stub pour éviter d'ouvrir la DuckDB qui n'existe pas en CI. Le retour
    # n'est pas inspecté car `_capture_charger` ignore ses arguments.
    monkeypatch.setattr(taxes_orchestration, "c15", lambda: pl.LazyFrame())
    monkeypatch.setattr(taxes_orchestration, "releves_harmonises", lambda: pl.LazyFrame())

    # Stubs Odoo (l'orchestration charge encore les PDLs depuis sale.order).
    class _OdooReaderMock:
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

    monkeypatch.setattr(taxes_orchestration, "query", lambda odoo, model, domain, fields: _QueryMock(df_pdl_brut))

    @contextmanager
    def _fake_reader(config):
        yield _OdooReaderMock()

    with _fake_reader(config={}) as odoo:
        result = taxes_orchestration.cta_par_contrat(odoo)

    assert len(appels_charger) == 1, "charger() doit être appelé exactement une fois"
    assert appels_charger[0][2] is None, "charger() doit être appelé avec mois=None"
    assert isinstance(result, pl.DataFrame)
    assert "cta_eur" in result.columns
