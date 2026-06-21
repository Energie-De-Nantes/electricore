"""Tests pour la validation/paramétrisation des filtres `DuckDBQuery.filter()`.

Vérifie que :
- Les noms de colonnes inconnus sont rejetés (allowlist via FluxSchema).
- Les valeurs sont passées en paramètres liés (pas d'interpolation f-string).
- Une tentative d'injection SQL est neutralisée.
"""

import pytest

from electricore.core.loaders.duckdb import c15


class TestColumnAllowlist:
    """`.filter()` rejette les colonnes absentes du schéma du flux."""

    def test_unknown_column_raises_value_error(self):
        """Un nom de colonne hors schéma déclenche une erreur claire avant tout SQL."""
        with pytest.raises(ValueError, match="colonne inconnue"):
            c15().filter({"nonexistent_column": "X"})

    def test_error_message_lists_allowed_columns(self):
        """L'erreur cite quelques colonnes valides pour aider à corriger l'appel."""
        with pytest.raises(ValueError) as excinfo:
            c15().filter({"nonexistent_column": "X"})
        # Au moins une colonne connue de C15 doit apparaître pour orienter l'utilisateur
        assert "pdl" in str(excinfo.value)

    def test_known_column_passes(self):
        """Une colonne valide ne lève pas d'erreur."""
        q = c15().filter({"pdl": "PDL123"})
        assert q.filters == (("pdl", "PDL123"),)

    def test_raw_condition_bypasses_validation(self):
        """`.where(...)` reste une échappée pour expressions SQL brutes (non exposée HTTP)."""
        q = c15().where("pdl LIKE 'PDL%'")
        assert q.filters == (("__raw_condition", "pdl LIKE 'PDL%'"),)


class TestParameterizedFilters:
    """`.filter()` produit du SQL paramétré (`?` placeholders + params séparés)."""

    def test_single_value_is_parameterized(self):
        """Égalité simple → `col = ?`, valeur dans params, jamais interpolée."""
        sql, params = c15().filter({"pdl": "PDL123"})._build_final_query()

        assert "pdl = ?" in sql
        assert "'PDL123'" not in sql
        assert params == ["PDL123"]

    def test_no_filter_yields_empty_params(self):
        """Sans filtre, params est une liste vide."""
        sql, params = c15()._build_final_query()

        assert "?" not in sql
        assert params == []

    def test_list_filter_in_clause_parameterized(self):
        """Filtre liste → `col IN (?, ?, ...)`, valeurs dans params."""
        sql, params = c15().filter({"pdl": ["A", "B", "C"]})._build_final_query()

        assert "pdl IN (?, ?, ?)" in sql
        assert "'A'" not in sql
        assert params == ["A", "B", "C"]

    def test_operator_filter_parsed_and_parameterized(self):
        """`{date: ">= '2024-01-01'"}` sur une colonne à offset → le littéral est ancré en
        heure légale française (mirror du cast, #391) tout en restant PARAMÉTRÉ : la valeur
        transite par params, jamais interpolée."""
        sql, params = c15().filter({"date_evenement": ">= '2024-01-01'"})._build_final_query()

        assert "date_evenement >= timezone('Europe/Paris', CAST(? AS TIMESTAMP))" in sql
        assert "'2024-01-01'" not in sql
        assert params == ["2024-01-01"]

    def test_operator_without_quotes(self):
        """Opérateur avec valeur sans quotes (cas numérique)."""
        sql, params = c15().filter({"puissance_souscrite_kva": "> 6"})._build_final_query()

        assert "puissance_souscrite_kva > ?" in sql
        assert params == ["6"]


class TestSqlInjectionBlocked:
    """Une tentative d'injection SQL via une valeur de filtre est neutralisée."""

    def test_quote_injection_in_value_is_bound_as_literal(self):
        """`pdl = "X' OR '1'='1"` est passé comme paramètre, jamais interpolé."""
        sql, params = c15().filter({"pdl": "X' OR '1'='1"})._build_final_query()

        # La valeur d'attaque n'apparaît pas dans le SQL — uniquement le placeholder
        assert "X' OR '1'='1" not in sql
        assert "pdl = ?" in sql
        # Elle vit dans params, où DuckDB la traitera comme un littéral chaîne
        assert params == ["X' OR '1'='1"]

    def test_semicolon_injection_in_value_is_bound_as_literal(self):
        """Une valeur contenant `; DROP TABLE` reste un littéral."""
        sql, params = c15().filter({"pdl": "X'; DROP TABLE flux_c15; --"})._build_final_query()

        assert "DROP TABLE" not in sql
        assert params == ["X'; DROP TABLE flux_c15; --"]
