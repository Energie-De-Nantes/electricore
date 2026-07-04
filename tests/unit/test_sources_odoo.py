"""Tests du domaine de sélection de `lignes_factures_du_mois` (#561).

Les tests du service facturation mockent la fonction entière : le domaine Odoo
n'était exercé nulle part — c'est ainsi que « fenêtre par date seule » a pu rendre
les brouillons de campagne invisibles pendant toute une campagne. Ici on capture
le domaine réellement passé au `follow('invoice_ids')`.
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


def test_lignes_factures_du_mois_inclut_les_brouillons_hors_fenetre(monkeypatch):
    """#561 : les brouillons de campagne naissent sans `invoice_date` — la sélection
    doit être « fenêtre du mois OU brouillon », sinon la campagne est invisible."""
    calls: list = []
    monkeypatch.setattr(sources, "query", lambda odoo, model, domain=None, fields=None: _QueryStub(calls))

    sources.lignes_factures_du_mois(object(), "2026-06-01").collect()

    domaine_factures = next(domain for field, domain in calls if field == "invoice_ids")
    assert domaine_factures == [
        "|",
        "&",
        ("invoice_date", ">=", "2026-06-01"),
        ("invoice_date", "<", "2026-07-01"),
        ("state", "=", "draft"),
    ]
