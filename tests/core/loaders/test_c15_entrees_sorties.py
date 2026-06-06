"""Tests pour les méthodes builder .entrees() / .sorties() sur le flux C15."""

import pytest

from electricore.core.loaders.duckdb import ENTREES_C15, SORTIES_C15, c15, r151


class TestC15GroupingConstants:
    """Codes événementiels canoniques C15 (cf. CONTEXT.md)."""

    def test_entrees_codes(self):
        assert ENTREES_C15 == ("PMES", "MES", "CFNE")

    def test_sorties_codes(self):
        assert SORTIES_C15 == ("RES", "CFNS")


class TestC15EntreesMethod:
    """`.entrees()` filtre sur les codes d'entrée canoniques."""

    def test_adds_filter_on_evenement_declencheur(self):
        """`c15().entrees()` ajoute un filtre IN sur les 3 codes d'entrée."""
        q = c15().entrees()

        assert q.filters == (("evenement_declencheur", list(ENTREES_C15)),)

    def test_generates_in_clause(self):
        """Le SQL final contient une clause IN paramétrée avec les 3 codes."""
        sql, params = c15().entrees()._build_final_query()

        assert "evenement_declencheur IN (?, ?, ?)" in sql
        assert params == ["PMES", "MES", "CFNE"]

    def test_chainable_with_limit(self):
        """`.entrees()` retourne un DuckDBQuery chainable."""
        q = c15().entrees().limit(50)

        assert q.limit_value == 50
        assert q.filters == (("evenement_declencheur", list(ENTREES_C15)),)


class TestC15SortiesMethod:
    """`.sorties()` filtre sur les codes de sortie canoniques."""

    def test_adds_filter_on_evenement_declencheur(self):
        q = c15().sorties()

        assert q.filters == (("evenement_declencheur", list(SORTIES_C15)),)

    def test_generates_in_clause(self):
        sql, params = c15().sorties()._build_final_query()

        assert "evenement_declencheur IN (?, ?)" in sql
        assert params == ["RES", "CFNS"]


class TestGuardOnNonC15:
    """`.entrees()` / `.sorties()` ne s'appliquent qu'au flux C15."""

    def test_entrees_on_r151_raises(self):
        with pytest.raises(ValueError, match=r"\.entrees\(\) ne s'applique qu'au flux C15"):
            r151().entrees()

    def test_sorties_on_r151_raises(self):
        with pytest.raises(ValueError, match=r"\.sorties\(\) ne s'applique qu'au flux C15"):
            r151().sorties()
