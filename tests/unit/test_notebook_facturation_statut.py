"""Tests pour `notebooks/facturation.py` : la règle `a_jour` harmonisée (#579).

Cette cellule marimo n'est pas importable (fonction locale à `@app.cell`) : la garde
combine (a) une table de vérité sur une reproduction fidèle de l'expression et (b) une
assertion de source statique — même approche que
`tests/integration/test_operator_notebooks_bundle.py` — pour détecter tout drift entre
le notebook et l'expression vérifiée.
"""

from pathlib import Path

import polars as pl
import pytest

_RACINE = Path(__file__).resolve().parents[2]
_SOURCE_FACTURATION = (_RACINE / "notebooks" / "facturation.py").read_text()

# Expression harmonisée (#579), reproduite mot pour mot depuis notebooks/facturation.py.
_EXPR_A_JOUR = (
    'pl.col("qualite").is_not_null()\n'
    '            & ((pl.col("qualite") != "incalculable") | pl.col("x_lisse").fill_null(False))'
)


def _a_jour_expr() -> pl.Expr:
    """Reproduction fidèle de l'expression `a_jour` de facturation.py (#579)."""
    return pl.col("qualite").is_not_null() & (
        (pl.col("qualite") != "incalculable") | pl.col("x_lisse").fill_null(False)
    )


class TestReglesAJourFacturation:
    """#579 : draft ⟺ qualite null OU (incalculable ET non lissé)."""

    @pytest.mark.parametrize(
        "qualite,x_lisse,attendu_a_jour",
        [
            # incalculable + x_lisse null/false → PAS à jour (draft)
            ("incalculable", None, False),
            ("incalculable", False, False),
            # qualite null (sans correspondance Enedis) → PAS à jour, même lissé
            (None, True, False),
            (None, None, False),
            (None, False, False),
            # incalculable + lissé (avec correspondance) → à jour (provision, ADR-0033/0048)
            ("incalculable", True, True),
            # réelle/estimée non lissé → à jour, inchangé
            ("réelle", False, True),
            ("estimée", False, True),
            ("réelle", None, True),
            ("estimée", True, True),
        ],
    )
    def test_table_de_verite_qualite_x_lisse(self, qualite, x_lisse, attendu_a_jour):
        data = pl.DataFrame(
            {"qualite": [qualite], "x_lisse": [x_lisse]},
            schema={"qualite": pl.Utf8, "x_lisse": pl.Boolean},
        )
        result = data.with_columns(_a_jour_expr().alias("a_jour"))
        assert result["a_jour"][0] == attendu_a_jour

    def test_notebook_porte_expression_harmonisee(self):
        """Garde anti-drift : facturation.py doit utiliser l'expression a_jour vérifiée."""
        assert _EXPR_A_JOUR in _SOURCE_FACTURATION, (
            "facturation.py doit utiliser l'expression a_jour harmonisée (#579) : "
            "qualite non-null ET (qualite != incalculable OU x_lisse.fill_null(False))."
        )
