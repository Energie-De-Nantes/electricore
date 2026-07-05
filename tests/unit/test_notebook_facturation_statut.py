"""Tests pour `notebooks/facturation.py` : la règle `a_jour` harmonisée (#579) et le
signalement des brouillons dont le contrat démarre après le mois facturé (#581).

Ces cellules marimo ne sont pas importables (fonctions locales à `@app.cell`) : la
garde combine (a) une table de vérité sur une reproduction fidèle de l'expression et
(b) une assertion de source statique — même approche que
`tests/integration/test_operator_notebooks_bundle.py` — pour détecter tout drift entre
le notebook et l'expression vérifiée.
"""

from datetime import date, timedelta
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


def _fin_mois(mois: str) -> date:
    """Dernier jour du mois AAAA-MM (même calcul que la cellule de signalement)."""
    debut = date.fromisoformat(f"{mois}-01")
    return (debut.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)


class TestSignalementPostMois:
    """#581 : signaler les brouillons (qualite null) dont le contrat démarre après le
    mois facturé — pas ceux entrés en cours de mois (prorata légitime)."""

    @pytest.mark.parametrize(
        "date_mise_en_service,attendu_signale",
        [
            (date(2026, 5, 15), False),  # avant le mois facturé
            (date(2026, 6, 1), False),  # début du mois (prorata)
            (date(2026, 6, 15), False),  # en cours de mois (prorata)
            (date(2026, 6, 30), False),  # dernier jour du mois — edge exact, pas signalé
            (date(2026, 7, 1), True),  # lendemain de la fin du mois → signalé
            (date(2026, 7, 2), True),  # cas réel juin 2026 (S01070-S01076)
        ],
    )
    def test_table_de_verite_date_boundary(self, date_mise_en_service, attendu_signale):
        fin_mois = _fin_mois("2026-06")
        assert (date_mise_en_service > fin_mois) is attendu_signale

    def test_notebook_calcule_la_fin_de_mois(self):
        assert "replace(day=28) + timedelta(days=4)" in _SOURCE_FACTURATION

    def test_notebook_nomme_la_cause_et_la_date(self):
        assert "contrat mis en service après le mois facturé" in _SOURCE_FACTURATION
        assert "date_mise_en_service" in _SOURCE_FACTURATION

    def test_signalement_post_mois_est_lecture_seule(self):
        """Signalement seul (#581) : ce bloc ne doit écrire dans Odoo nulle part."""
        debut = _SOURCE_FACTURATION.index("### Brouillons dont le contrat démarre après le mois facturé")
        fin = _SOURCE_FACTURATION.index("## Préparation données factures")
        bloc = _SOURCE_FACTURATION[debut:fin]
        assert "OdooWriter" not in bloc
        assert "_writer.update" not in bloc
