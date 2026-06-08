"""Tests pour `documents_facturation_du_mois` (issue #78).

Vérifie que la fonction retourne un dict prêt pour `xlsx_multi_sheet` :
clés = libellés d'onglets FR éditoriaux, plus de suffixes `.csv`.
"""


def test_documents_facturation_du_mois_returns_fr_sheet_names(monkeypatch):
    """Les clés du dict sont des libellés d'onglets FR (pas des noms de fichiers CSV)."""
    from datetime import datetime

    import polars as pl

    from electricore.core.orchestrations.contexte_mensuel import ContexteMensuel
    from electricore.integrations.odoo import facturation as facturation_orchestration

    df_fact_mensuelle = pl.DataFrame(
        {
            "ref_situation_contractuelle": ["RSC001"],
            "pdl": ["12345678901234"],
            "debut": [datetime(2025, 1, 1)],
            "fin": [datetime(2025, 2, 1)],
            "energie_hp_kwh": [123.45],
            "energie_hc_kwh": [0.0],
            "energie_base_kwh": [0.0],
            "nb_jours": [31],
            "turpe_fixe_eur": [12.34],
            "turpe_variable_eur": [56.78],
            "data_complete": [True],
            "memo_puissance": [""],
        }
    ).with_columns(
        pl.col("debut").dt.replace_time_zone("Europe/Paris"),
        pl.col("fin").dt.replace_time_zone("Europe/Paris"),
    )

    contexte_prefab = ContexteMensuel(
        mois="2025-01-01",
        historique_enrichi=pl.LazyFrame(
            {
                "ref_situation_contractuelle": ["RSC001"],
                "num_compteur": ["12345678"],
                "type_compteur": ["LINKY"],
            }
        ),
        abonnements=pl.LazyFrame(),
        energie=pl.LazyFrame(),
        facturation_mensuelle=df_fact_mensuelle,
    )
    monkeypatch.setattr(facturation_orchestration, "charger", lambda historique, releves, mois=None: contexte_prefab)

    # Les loaders sont évalués côté caller AVANT charger() (topologie #87).
    monkeypatch.setattr(facturation_orchestration, "releves_harmonises", lambda: pl.LazyFrame())

    df_lignes_odoo = pl.DataFrame(
        {
            # Schéma LignesFacture (slice 2 de la refonte) : clés renommées + est_brouillon
            "ref_situation_contractuelle": ["RSC001"],
            "categorie_produit": ["HP"],
            "quantite": [100.0],
            "est_brouillon": [True],
            # ERP passe-plat
            "invoice_line_ids": [101],
            "x_pdl": ["12345678901234"],
            "x_lisse": [False],
            "name_account_move": ["INV/2025/0001"],
            "name_product_product": ["Énergie HP"],
        }
    )

    class _QueryMock:
        def __init__(self, df):
            self._df = df

        def collect(self):
            return self._df

        def lazy(self):
            return self._df.lazy() if isinstance(self._df, pl.DataFrame) else self._df

        def filter(self, *a, **k):
            return _QueryMock(self.lazy().filter(*a, **k))

    df_flux_vide = pl.DataFrame(
        schema={
            "pdl": pl.Utf8,
            "date_facture": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
            "date_evenement": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
            "unite": pl.Utf8,
            "evenement_declencheur": pl.Utf8,
        }
    )
    monkeypatch.setattr(
        facturation_orchestration, "lignes_factures_du_mois", lambda odoo, mois: _QueryMock(df_lignes_odoo)
    )
    monkeypatch.setattr(facturation_orchestration, "c15", lambda: _QueryMock(df_flux_vide))
    monkeypatch.setattr(facturation_orchestration, "f15", lambda: _QueryMock(df_flux_vide))

    documents, suffix = facturation_orchestration.documents_facturation_du_mois(odoo=None, mois="2025-01-01")

    assert set(documents.keys()) == {
        "F15 complet",
        "F15 prestations",
        "C15 complet",
        "C15 sorties",
        "Réconciliation",
        "Changements puissance",
    }
    assert suffix == "2025-01"
