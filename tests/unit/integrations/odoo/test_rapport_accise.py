"""Tests pour `accise_par_contrat` et `rapport_accise` (Candidate 1, issue #56).

Pattern testé : interface brute (`accise_par_contrat`) vs interface agrégée
(`rapport_accise`). Les agrégations vivent désormais dans `integrations/odoo/`
(shape leak corrigé depuis `taxes_service.py`).
"""

import polars as pl
from polars.testing import assert_frame_equal


def test_accise_par_contrat_exists_and_is_callable():
    """`accise_par_contrat` est exporté depuis `integrations.odoo.taxes`."""
    from electricore.integrations.odoo import taxes

    assert hasattr(taxes, "accise_par_contrat")
    assert callable(taxes.accise_par_contrat)


def test_accise_par_contrat_delegates_to_pipeline_accise(monkeypatch):
    """`accise_par_contrat(odoo, trimestre)` = pipeline_accise filtré + sort."""
    from electricore.integrations.odoo import taxes

    df_lignes = pl.DataFrame({"x_pdl": ["A"], "name": ["SO/1"]})
    df_accise_full = pl.DataFrame(
        {
            "pdl": ["A", "A", "B"],
            "mois_consommation": ["2025-01", "2025-04", "2025-01"],
            "trimestre": ["2025-T1", "2025-T2", "2025-T1"],
            "energie_mwh": [1.0, 2.0, 3.0],
            "taux_accise_eur_mwh": [22.5, 22.5, 22.5],
            "accise_eur": [22.50, 45.00, 67.50],
        }
    )

    # Mocke la frontière externe : commandes_lignes + pipeline_accise
    monkeypatch.setattr(taxes, "commandes_lignes", lambda _odoo: df_lignes.lazy())
    monkeypatch.setattr(taxes, "pipeline_accise", lambda _lf: df_accise_full)

    result_all = taxes.accise_par_contrat(odoo=None)
    assert result_all.height == 3

    result_q1 = taxes.accise_par_contrat(odoo=None, trimestre="2025-T1")
    assert result_q1.height == 2
    assert set(result_q1["trimestre"].unique().to_list()) == {"2025-T1"}


def test_accise_du_trimestre_is_removed():
    """L'ancien nom `accise_du_trimestre` n'existe plus."""
    from electricore.integrations.odoo import taxes

    assert not hasattr(taxes, "accise_du_trimestre")


class TestRapportAcciseSchemas:
    """Les 3 `DataFrameModel` Pandera valident bien des frames conformes."""

    def test_resume_schema_validates_correct_frame(self):
        from electricore.integrations.odoo.models.rapport_accise import RapportAcciseResume

        df = pl.DataFrame(
            {
                "trimestre": ["2025-T1"],
                "nb_pdl": [2],
                "energie_mwh_total": [10.0],
                "accise_eur_total": [225.0],
            }
        )
        RapportAcciseResume.validate(df)

    def test_par_taux_schema_validates_correct_frame(self):
        from electricore.integrations.odoo.models.rapport_accise import RapportAcciseParTaux

        df = pl.DataFrame(
            {
                "taux_accise_eur_mwh": [22.5],
                "energie_mwh": [10.0],
                "accise_eur": [225.0],
                "nb_pdl": [2],
            }
        )
        RapportAcciseParTaux.validate(df)

    def test_detail_schema_validates_correct_frame(self):
        from electricore.integrations.odoo.models.rapport_accise import RapportAcciseDetail

        df = pl.DataFrame(
            {
                "pdl": ["A"],
                "mois_consommation": ["2025-01"],
                "trimestre": ["2025-T1"],
                "taux_accise_eur_mwh": [22.5],
                "energie_mwh": [1.0],
                "accise_eur": [22.5],
            }
        )
        RapportAcciseDetail.validate(df)


def _detail_synthetique() -> pl.DataFrame:
    """Détail synthétique : 2 PDL × 2 trimestres × 2 taux différents."""
    return pl.DataFrame(
        {
            "pdl": ["A", "A", "B", "B", "C"],
            "mois_consommation": ["2025-01", "2025-02", "2025-01", "2025-04", "2025-04"],
            "trimestre": ["2025-T1", "2025-T1", "2025-T1", "2025-T2", "2025-T2"],
            "taux_accise_eur_mwh": [22.5, 22.5, 22.5, 25.0, 25.0],
            "energie_mwh": [1.0, 2.0, 3.0, 4.0, 5.0],
            "accise_eur": [22.5, 45.0, 67.5, 100.0, 125.0],
        }
    )


class TestRapportAccise:
    """`rapport_accise` produit un `RapportAccise(resume, par_taux, detail)`."""

    def test_returns_namedtuple_with_three_fields(self, monkeypatch):
        from electricore.integrations.odoo import taxes

        monkeypatch.setattr(taxes, "accise_par_contrat", lambda odoo, trimestre=None: _detail_synthetique())
        result = taxes.rapport_accise(odoo=None)

        # NamedTuple: accès par attribut
        assert hasattr(result, "resume")
        assert hasattr(result, "par_taux")
        assert hasattr(result, "detail")

    def test_detail_contains_same_rows_as_accise_par_contrat(self, monkeypatch):
        """`rapport.detail` contient les mêmes lignes que `accise_par_contrat` (réordonnées).

        Depuis #75, `rapport_accise` trie son `detail` par `(pdl, mois_consommation)`.
        Les colonnes et l'ensemble des lignes restent identiques.
        """
        from electricore.integrations.odoo import taxes

        detail_source = _detail_synthetique()
        monkeypatch.setattr(taxes, "accise_par_contrat", lambda odoo, trimestre=None: detail_source)
        result = taxes.rapport_accise(odoo=None)

        # Mêmes colonnes, mêmes lignes (après tri par pdl puis mois).
        assert_frame_equal(
            result.detail,
            detail_source.sort(["pdl", "mois_consommation"]),
        )

    def test_par_taux_groups_by_taux_descending(self, monkeypatch):
        """`par_taux` groupé par taux décroissant avec sommes + n_unique(pdl) corrects."""
        from electricore.integrations.odoo import taxes

        monkeypatch.setattr(taxes, "accise_par_contrat", lambda odoo, trimestre=None: _detail_synthetique())
        result = taxes.rapport_accise(odoo=None)

        # Tri descendant sur taux : 25.0 d'abord, puis 22.5
        assert result.par_taux["taux_accise_eur_mwh"].to_list() == [25.0, 22.5]
        # Taux 25.0 : 4+5=9.0 mwh, 100+125=225.0 €, 2 PDL distincts (B, C)
        # Taux 22.5 : 1+2+3=6.0 mwh, 22.5+45+67.5=135.0 €, 2 PDL distincts (A, B)
        assert result.par_taux["energie_mwh"].to_list() == [9.0, 6.0]
        assert result.par_taux["accise_eur"].to_list() == [225.0, 135.0]
        assert result.par_taux["nb_pdl"].to_list() == [2, 2]

    def test_resume_groups_by_trimestre_ascending(self, monkeypatch):
        """`resume` groupé par trimestre avec totaux + n_unique(pdl) corrects."""
        from electricore.integrations.odoo import taxes

        monkeypatch.setattr(taxes, "accise_par_contrat", lambda odoo, trimestre=None: _detail_synthetique())
        result = taxes.rapport_accise(odoo=None)

        assert result.resume["trimestre"].to_list() == ["2025-T1", "2025-T2"]
        # T1: PDL = {A, B} -> 2 distinct ; énergie = 1+2+3 = 6.0 ; accise = 22.5+45+67.5 = 135.0
        # T2: PDL = {B, C} -> 2 distinct ; énergie = 4+5 = 9.0 ; accise = 100+125 = 225.0
        assert result.resume["nb_pdl"].to_list() == [2, 2]
        assert result.resume["energie_mwh_total"].to_list() == [6.0, 9.0]
        assert result.resume["accise_eur_total"].to_list() == [135.0, 225.0]

    def test_detail_is_sorted_by_pdl_then_mois_consommation(self, monkeypatch):
        """Invariant : `rapport_accise(...).detail` est trié `(pdl, mois_consommation)` (issue #75).

        Le tri de présentation du livrable devient un contrat porté par
        `rapport_accise` lui-même — plus une convenance de la couche service.
        """
        from electricore.integrations.odoo import taxes

        # Détail volontairement désordonné en entrée.
        detail_desordonne = pl.DataFrame(
            {
                "pdl": ["B", "A", "B", "A"],
                "mois_consommation": ["2025-02", "2025-02", "2025-01", "2025-01"],
                "trimestre": ["2025-T1"] * 4,
                "taux_accise_eur_mwh": [22.5] * 4,
                "energie_mwh": [1.0, 2.0, 3.0, 4.0],
                "accise_eur": [22.5, 45.0, 67.5, 90.0],
            }
        )
        monkeypatch.setattr(taxes, "accise_par_contrat", lambda odoo, trimestre=None: detail_desordonne)

        result = taxes.rapport_accise(odoo=None)

        assert result.detail["pdl"].to_list() == ["A", "A", "B", "B"]
        assert result.detail["mois_consommation"].to_list() == ["2025-01", "2025-02", "2025-01", "2025-02"]

    def test_rapport_accise_propagates_trimestre_filter(self, monkeypatch):
        """`rapport_accise(odoo, trimestre)` propage le filtre à `accise_par_contrat`."""
        from electricore.integrations.odoo import taxes

        appels: list[str | None] = []

        def fake_accise_par_contrat(odoo, trimestre=None):
            appels.append(trimestre)
            return _detail_synthetique()

        monkeypatch.setattr(taxes, "accise_par_contrat", fake_accise_par_contrat)
        taxes.rapport_accise(odoo=None, trimestre="2025-T1")

        assert appels == ["2025-T1"]
