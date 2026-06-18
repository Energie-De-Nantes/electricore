"""Tests du wire-up `documents_facturation_du_mois` (issues #78, #144 — vit en `api/services`).

Vérifie que la fonction retourne un dict prêt pour `xlsx_multi_sheet` :
clés = libellés d'onglets FR éditoriaux, plus de suffixes `.csv`.
"""


def test_documents_facturation_du_mois_returns_fr_sheet_names(monkeypatch):
    """Les clés du dict sont des libellés d'onglets FR (pas des noms de fichiers CSV)."""
    from datetime import datetime

    import polars as pl

    from electricore.api.services import facturation_service
    from electricore.core.builds.contexte_mensuel import ContexteMensuel

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
            "qualite": ["réelle"],
            "statut_communication": ["communicante"],
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
        releves_utilises=pl.LazyFrame(),
        facturation_mensuelle=df_fact_mensuelle,
    )
    # Un seul nom à patcher depuis #145 (contre charger + c15 + releves_harmonises avant) ;
    # c15/f15 restent patchés plus bas pour les onglets bruts de documents() (topologie #87).
    monkeypatch.setattr(facturation_service, "contexte_du_mois", lambda mois=None: contexte_prefab)

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
    monkeypatch.setattr(facturation_service, "lignes_factures_du_mois", lambda odoo, mois: _QueryMock(df_lignes_odoo))
    monkeypatch.setattr(facturation_service, "c15", lambda: _QueryMock(df_flux_vide))
    monkeypatch.setattr(facturation_service, "f15", lambda: _QueryMock(df_flux_vide))

    documents, suffix = facturation_service.documents_facturation_du_mois(odoo=None, mois="2025-01-01")

    assert set(documents.keys()) == {
        "F15 complet",
        "F15 prestations",
        "C15 complet",
        "C15 sorties",
        "Réconciliation",
        "Changements puissance",
    }
    assert suffix == "2025-01"


def test_categories_hors_scope_ecartees_avant_rapprochement(monkeypatch):
    """Garde #335 : les catégories produit hors scope (`Prestation-Enedis`, `All`)
    portées par le catalogue Odoo sont écartées par le service *avant* `rapprocher()`,
    qui aurait sinon échoué sur le `isin` du contrat `LignesFacture` (503 en prod).

    On exerce le vrai `rapprocher` (le chemin qui cassait) via `facturation_du_mois`
    avec une donnée Odoo prod-réaliste contenant les 6 valeurs observées en prod.
    """
    from datetime import datetime

    import polars as pl

    from electricore.api.services import facturation_service
    from electricore.core.builds.contexte_mensuel import ContexteMensuel

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
            "qualite": ["réelle"],
            "statut_communication": ["communicante"],
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
        releves_utilises=pl.LazyFrame(),
        facturation_mensuelle=df_fact_mensuelle,
    )
    monkeypatch.setattr(facturation_service, "contexte_du_mois", lambda mois=None: contexte_prefab)

    # Donnée Odoo prod-réaliste : catégories facturables + les deux hors scope (#335).
    df_lignes_odoo = pl.DataFrame(
        {
            "ref_situation_contractuelle": ["RSC001", "RSC001", "RSC001"],
            "categorie_produit": ["HP", "Prestation-Enedis", "All"],
            "quantite": [100.0, 1.0, 5.0],
            "est_brouillon": [True, True, True],
            "invoice_line_ids": [101, 102, 103],
            "x_pdl": ["12345678901234", "12345678901234", "12345678901234"],
            "x_lisse": [False, False, False],
            "name_account_move": ["INV/2025/0001", "INV/2025/0001", "INV/2025/0001"],
            "name_product_product": ["Énergie HP", "Frais Enedis", "Produit divers"],
        }
    )

    class _QueryMock:
        def __init__(self, df):
            self._df = df

        def lazy(self):
            return self._df.lazy() if isinstance(self._df, pl.DataFrame) else self._df

        def collect(self):
            return self.lazy().collect()

        def filter(self, *a, **k):
            return _QueryMock(self.lazy().filter(*a, **k))

    monkeypatch.setattr(facturation_service, "lignes_factures_du_mois", lambda odoo, mois: _QueryMock(df_lignes_odoo))

    # Ne doit pas lever (le 503 venait du isin de LignesFacture sur Prestation-Enedis/All).
    result = facturation_service.facturation_du_mois(odoo=None, mois="2025-01-01")

    # Les deux catégories hors scope sont absentes ; seule la ligne facturable survit
    # et se rapproche correctement (quantite_enedis = energie_hp_kwh).
    categories = set(result["categorie_produit"].to_list())
    assert categories == {"HP"}
    assert "Prestation-Enedis" not in categories
    assert "All" not in categories
    assert result.height == 1
    assert result["quantite_enedis"].to_list() == [123.45]
