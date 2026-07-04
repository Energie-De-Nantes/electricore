"""Tests du domaine de sélection de `lignes_factures_du_mois` (#561, ADR-0054).

Les tests du service facturation mockent la fonction entière : le domaine Odoo
n'était exercé nulle part — c'est ainsi que « fenêtre par date seule » a pu rendre
les brouillons de campagne invisibles pendant toute une campagne. Ici on capture
le domaine réellement passé au `follow('invoice_ids')`.

Convention date-ancre (ADR-0054) : Odoo pose `invoice_date = 05/(M+1)` sur tous
les brouillons de la campagne du mois M. La sélection lit cette même ancre par
égalité stricte — plus de fenêtre, plus d'avalement silencieux des brouillons
sans date (une violation de la convention doit être visible, pas absorbée).
"""

import polars as pl

from electricore.integrations.odoo import sources


class _QueryStub:
    """Enregistre les appels follow/enrich, rend un LazyFrame au schéma minimal."""

    def __init__(self, calls: list) -> None:
        self.calls = calls

    def follow(self, field, domain=None, fields=None, **kwargs):
        self.calls.append((field, domain))
        return self

    def enrich(self, field, domain=None, fields=None, **kwargs):
        self.calls.append((field, domain))
        return self

    def lazy(self) -> pl.LazyFrame:
        # Colonnes minimales pour que `_expr_est_brouillon()` et le rename passent.
        return pl.DataFrame(
            schema={
                "x_invoicing_state": pl.Utf8,
                "state_account_move": pl.Utf8,
                "x_ref_situation_contractuelle": pl.Utf8,
                "name_product_category": pl.Utf8,
                "quantity": pl.Float64,
            }
        ).lazy()


def test_date_ancre_05_du_mois_suivant():
    """Conso de juin → ancre 05/07 (#561, ADR-0054)."""
    assert sources.date_ancre("2026-06-01") == "2026-07-05"


def test_date_ancre_rollover_decembre():
    """Conso de décembre N → ancre 05/01/(N+1)."""
    assert sources.date_ancre("2026-12-01") == "2027-01-05"


def test_lignes_factures_du_mois_selectionne_par_egalite_stricte_sur_l_ancre(monkeypatch):
    """#561/ADR-0054 : sélection par égalité stricte sur `invoice_date`, plus de fenêtre."""
    calls: list = []
    monkeypatch.setattr(sources, "query", lambda odoo, model, domain=None, fields=None: _QueryStub(calls))

    sources.lignes_factures_du_mois(object(), "2026-06-01").collect()

    domaine_factures = next(domain for field, domain in calls if field == "invoice_ids")
    assert domaine_factures == [("invoice_date", "=", "2026-07-05")]


def test_lignes_factures_du_mois_rollover_decembre(monkeypatch):
    calls: list = []
    monkeypatch.setattr(sources, "query", lambda odoo, model, domain=None, fields=None: _QueryStub(calls))

    sources.lignes_factures_du_mois(object(), "2026-12-01").collect()

    domaine_factures = next(domain for field, domain in calls if field == "invoice_ids")
    assert domaine_factures == [("invoice_date", "=", "2027-01-05")]
