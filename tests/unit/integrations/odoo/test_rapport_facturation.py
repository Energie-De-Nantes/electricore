"""Tests pour `rapport_facturation` (Candidate 1.3, issue #64).

Réplique du pattern `rapport_X` / `X_brut` validé par #56 (accise) et #63 (CTA)
sur le domaine facturation. Spécificités :

- Le nom de l'interface brute reste `facturation_du_mois` (sémantique by-month,
  pas by-contract — cf. arbitrage de la PR).
- Le rapport porte une **partition** explicite : les lignes flaguées
  `memo_puissance != ""` ressortent dans un onglet dédié `Changements puissance`.
"""

import polars as pl


def test_facturation_du_mois_exists_and_is_callable():
    """`facturation_du_mois` est exporté depuis `integrations.odoo.facturation`."""
    from electricore.integrations.odoo import facturation

    assert hasattr(facturation, "facturation_du_mois")
    assert callable(facturation.facturation_du_mois)


class TestRapportFacturationResumeSchema:
    """Le schéma `RapportFacturationResume` valide une frame une-ligne de totaux."""

    def test_resume_schema_validates_one_row_frame(self):
        from electricore.integrations.odoo.models.rapport_facturation import RapportFacturationResume

        df = pl.DataFrame(
            {
                "mois": ["2025-03-01"],
                "nb_pdl": [42],
                "total_a_facturer": [156],
                "total_a_supprimer": [3],
            }
        )
        RapportFacturationResume.validate(df)


def _lignes_synthetique() -> pl.DataFrame:
    """`facturation_du_mois` synthétique avec mix de flags + un changement puissance."""
    return pl.DataFrame(
        {
            "invoice_line_ids": [1, 2, 3, 4, 5],
            "x_pdl": ["A", "A", "B", "C", "B"],
            "x_lisse": [False, False, True, False, True],
            "name_account_move": ["INV/1", "INV/1", "INV/2", "INV/3", "INV/2"],
            "name_product_category": ["Base", "HP", "Base", "Base", "Base"],
            "name_product_product": ["P1", "P2", "P1", "P1", "P1"],
            "quantity": [100.0, 50.0, 80.0, 60.0, 0.0],
            "quantite_enedis": [100.0, 50.0, 80.0, 60.0, 0.0],
            # Spécificité : 2 lignes avec memo_puissance non vide (B sur 2 lignes)
            "memo_puissance": ["", "", "", "", "Hausse 6 → 9 kVA"],
            "ref_situation_contractuelle": ["R-A", "R-A", "R-B", "R-C", "R-B"],
            "pdl": ["A", "A", "B", "C", "B"],
            "debut": [None, None, None, None, None],
            "fin": [None, None, None, None, None],
            "data_complete": [True, True, True, True, True],
            "turpe_fixe_eur": [10.0, 0.0, 12.0, 8.0, 0.0],
            "turpe_variable_eur": [5.0, 2.5, 4.0, 3.0, 0.0],
            "num_compteur": ["N-A", "N-A", "N-B", "N-C", "N-B"],
            "type_compteur": ["L", "L", "L", "L", "L"],
            "a_facturer": [True, True, True, False, True],
            "a_supprimer": [False, False, False, True, False],
        },
        # Coerce dtypes pour matcher LignesFactureRapprochees (debut/fin DateTime tz)
        schema_overrides={
            "debut": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
            "fin": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
        },
    )


class TestRapportFacturation:
    """`rapport_facturation` produit `RapportFacturation(resume, lignes, changements_puissance)`."""

    def test_returns_namedtuple_with_three_fields(self, monkeypatch):
        from electricore.integrations.odoo import facturation

        monkeypatch.setattr(facturation, "facturation_du_mois", lambda odoo, mois=None: _lignes_synthetique())
        result = facturation.rapport_facturation(odoo=None, mois="2025-03-01")

        assert hasattr(result, "resume")
        assert hasattr(result, "lignes")
        assert hasattr(result, "changements_puissance")

    def test_lignes_is_identity_of_facturation_du_mois(self, monkeypatch):
        """`rapport.lignes` = la sortie brute, sans filtrage."""
        from electricore.integrations.odoo import facturation

        lignes_attendues = _lignes_synthetique()
        monkeypatch.setattr(facturation, "facturation_du_mois", lambda odoo, mois=None: lignes_attendues)
        result = facturation.rapport_facturation(odoo=None, mois="2025-03-01")

        assert result.lignes.height == lignes_attendues.height
        assert set(result.lignes.columns) == set(lignes_attendues.columns)

    def test_changements_puissance_filters_on_memo(self, monkeypatch):
        """`changements_puissance` = lignes où `memo_puissance != ''`."""
        from electricore.integrations.odoo import facturation

        monkeypatch.setattr(facturation, "facturation_du_mois", lambda odoo, mois=None: _lignes_synthetique())
        result = facturation.rapport_facturation(odoo=None, mois="2025-03-01")

        # Une seule ligne dans le synthétique a memo_puissance != ""
        assert result.changements_puissance.height == 1
        assert result.changements_puissance["x_pdl"][0] == "B"
        assert result.changements_puissance["memo_puissance"][0] == "Hausse 6 → 9 kVA"

    def test_resume_totals_correct(self, monkeypatch):
        """`resume` = une ligne (mois, nb_pdl, total_a_facturer, total_a_supprimer)."""
        from electricore.integrations.odoo import facturation

        monkeypatch.setattr(facturation, "facturation_du_mois", lambda odoo, mois=None: _lignes_synthetique())
        result = facturation.rapport_facturation(odoo=None, mois="2025-03-01")

        assert result.resume.height == 1
        row = result.resume.row(0, named=True)
        assert row["mois"] == "2025-03-01"
        # 3 PDL distincts : A, B, C
        assert row["nb_pdl"] == 3
        # 4 lignes a_facturer == True (1, 2, 3, 5)
        assert row["total_a_facturer"] == 4
        # 1 ligne a_supprimer == True (4)
        assert row["total_a_supprimer"] == 1

    def test_propagates_mois_filter(self, monkeypatch):
        """Le query param `mois` est propagé jusqu'à `facturation_du_mois`."""
        from electricore.integrations.odoo import facturation

        appels: list[str | None] = []

        def fake_facturation(odoo, mois=None):
            appels.append(mois)
            return _lignes_synthetique()

        monkeypatch.setattr(facturation, "facturation_du_mois", fake_facturation)
        facturation.rapport_facturation(odoo=None, mois="2025-04-01")

        assert appels == ["2025-04-01"]
