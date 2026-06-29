"""Tests pour la validation/paramétrisation des filtres `DuckDBQuery.filter()`.

Vérifie que :
- Un loader `SELECT *` (ADR-0042) accepte toute colonne (allowlist ouverte) — l'existence
  est tranchée par DuckDB à l'exécution, pas pré-validée par le builder.
- Les valeurs sont passées en paramètres liés (pas d'interpolation f-string).
- Une tentative d'injection SQL est neutralisée.

La sûreté anti-injection repose sur la PARAMÉTRISATION des valeurs (jamais interpolées),
pas sur l'allowlist : les noms de colonnes des `.filter()` de l'API sont des littéraux du
code (`pdl`, `date_releve`, `source`), jamais du HTTP.
"""

from electricore.core.loaders.duckdb import c15


class TestColumnAllowlist:
    """Loader `SELECT *` (ADR-0042) : `.filter()` n'a plus de schéma à re-déclarer → allowlist
    ouverte (comme les marts `base_sql`). L'existence de la colonne est tranchée par DuckDB."""

    def test_select_star_loader_accepte_toute_colonne(self):
        """Un loader SELECT * (c15, #397) accepte un nom de colonne arbitraire sans lever :
        la forme résiduelle ayant migré en dbt, le builder ne connaît plus le schéma. Une
        colonne inexistante échouerait au Binder DuckDB à l'exécution, pas ici."""
        q = c15().filter({"colonne_quelconque": "X"})
        assert q.filters == (("colonne_quelconque", "X"),)

    def test_known_column_passes(self):
        """Une colonne valide est acceptée et enregistrée telle quelle."""
        q = c15().filter({"pdl": "PDL123"})
        assert q.filters == (("pdl", "PDL123"),)


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
        """`{date: ">= '2024-01-01'"}` → placeholder NU `col >= ?`, PARAMÉTRÉ : la valeur
        transite par params, jamais interpolée. Plus d'enveloppe `timezone(...)` par colonne
        (loader SELECT *, #397) ; le littéral est interprété en Europe/Paris par le fuseau de
        session épinglé de la connexion (#393), pas par un wrap SQL."""
        sql, params = c15().filter({"date_evenement": ">= '2024-01-01'"})._build_final_query()

        assert "date_evenement >= ?" in sql
        assert "timezone(" not in sql
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
