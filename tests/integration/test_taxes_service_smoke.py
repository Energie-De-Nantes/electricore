"""Tests smoke de `cta_par_contrat_service` (ADR-0019, issues #108, #145).

Garantit que le service délègue le chargement de la facturation à
`core.builds.contexte_mensuel.contexte_du_mois` (sources par défaut, #145)
plutôt que de reconstruire le trio `c15 + releves_harmonises + facturation()`.

Depuis #108, la logique de wire-up vit dans `api/services/taxes_service.py`.
"""

from datetime import datetime

import polars as pl

from electricore.core.builds.contexte_mensuel import ContexteMensuel


def test_cta_par_contrat_service_delegue_a_contexte_du_mois(monkeypatch):
    """`cta_par_contrat_service` consomme `ContexteMensuel` via `contexte_du_mois()`.

    Invariant : le service délègue chargement ET composition à
    `core/builds/contexte_mensuel.py` — un seul nom à patcher (#145).
    """
    import electricore.api.services.taxes_service as svc

    df_facturation = pl.DataFrame(
        {
            "pdl": ["12345678901234"],
            "ref_situation_contractuelle": ["RSC001"],
            "debut": [datetime(2025, 1, 1)],
            "fin": [datetime(2025, 2, 1)],
            "mois_annee": ["2025-01"],
            "turpe_fixe_eur": [42.50],
            "nb_jours": [31],
        }
    ).with_columns(
        pl.col("debut").dt.replace_time_zone("Europe/Paris"),
        pl.col("fin").dt.replace_time_zone("Europe/Paris"),
    )

    contexte_prefab = ContexteMensuel(
        mois="2025-01-01",
        historique_enrichi=pl.LazyFrame(),
        abonnements=pl.LazyFrame(),
        energie=pl.LazyFrame(),
        facturation_mensuelle=df_facturation,
    )
    appels: list = []

    def _capture_contexte(mois=None):
        appels.append(mois)
        return contexte_prefab

    monkeypatch.setattr(svc, "contexte_du_mois", _capture_contexte)

    class _QueryMock:
        def __init__(self, df):
            self._df = df

        def filter(self, *args, **kwargs):
            return self

        def select(self, *args, **kwargs):
            return _QueryMock(self._df.select(*args, **kwargs))

        def collect(self):
            return self._df

    monkeypatch.setattr(
        svc, "mapping_pdl_order", lambda odoo: pl.DataFrame({"pdl": ["12345678901234"], "order_name": ["SO/2025/0001"]})
    )

    result = svc.cta_par_contrat_service(odoo=None)

    assert len(appels) == 1, "contexte_du_mois() doit être appelé exactement une fois"
    assert appels[0] is None, "contexte_du_mois() doit être appelé avec mois=None"
    assert isinstance(result, pl.DataFrame)
    assert "cta_eur" in result.columns
