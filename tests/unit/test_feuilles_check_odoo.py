"""Tests unitaires de `feuilles_check_odoo` (issue #173).

La projection suit le pattern Feuilles (core/CONTEXT.md) et se teste sur
DataFrames directs — sans round-trip dict JSON. Forme stable : 5 onglets fixes,
vides quand rien à signaler.
"""

import polars as pl

from electricore.api.serializers import xlsx_multi_sheet
from electricore.api.services.check_facturation_service import feuilles_check_odoo
from electricore.integrations.odoo import ResultatVerification

BASE_URL = "https://odoo.example"

ONGLETS = ("RSC manquant", "CFNE manquant", "Factures draft", "Lissés qty=1", "Invoicing state")


def _resultat(**overrides) -> ResultatVerification:
    defaults = dict(
        rsc_manquante=pl.DataFrame(schema={"sale_order_id": pl.Int64, "name": pl.Utf8, "x_pdl": pl.Utf8}),
        cfne_manquante=pl.DataFrame(schema={"sale_order_id": pl.Int64, "name": pl.Utf8, "x_pdl": pl.Utf8}),
        invoicing_state_counts=pl.DataFrame(schema={"state": pl.Utf8, "n": pl.UInt32}),
        factures_draft=pl.DataFrame(schema={"account_move_id": pl.Int64, "name": pl.Utf8}),
        lisses_quantite_1=pl.DataFrame(
            schema={"sale_order_id": pl.Int64, "name": pl.Utf8, "categ_names": pl.List(pl.Utf8)}
        ),
    )
    return ResultatVerification(**{**defaults, **overrides})


def test_forme_stable_5_onglets_meme_sans_anomalie():
    feuilles = feuilles_check_odoo(_resultat(), BASE_URL)
    assert tuple(feuilles.keys()) == ONGLETS


def test_liens_odoo_sur_les_anomalies():
    r = _resultat(rsc_manquante=pl.DataFrame({"sale_order_id": [7], "name": ["S00042"], "x_pdl": ["14000000000000"]}))
    feuilles = feuilles_check_odoo(r, BASE_URL)
    assert feuilles["RSC manquant"]["url"].to_list() == [
        "https://odoo.example/web#id=7&model=sale.order&view_type=form"
    ]


def test_factures_draft_liees_au_account_move():
    r = _resultat(factures_draft=pl.DataFrame({"account_move_id": [42], "name": ["S00007"]}))
    feuilles = feuilles_check_odoo(r, BASE_URL)
    assert feuilles["Factures draft"]["url"].to_list() == [
        "https://odoo.example/web#id=42&model=account.move&view_type=form"
    ]


def test_categ_names_aplati_en_texte():
    r = _resultat(
        lisses_quantite_1=pl.DataFrame({"sale_order_id": [3], "name": ["S00007"], "categ_names": [["Base", "HP"]]})
    )
    feuilles = feuilles_check_odoo(r, BASE_URL)
    assert feuilles["Lissés qty=1"]["categ_names"].to_list() == ["Base, HP"]


def test_invoicing_state_renomme_pour_le_livrable():
    r = _resultat(invoicing_state_counts=pl.DataFrame({"state": ["up_to_date"], "n": [12]}))
    feuilles = feuilles_check_odoo(r, BASE_URL)
    assert feuilles["Invoicing state"].columns == ["x_invoicing_state", "n"]


def test_xlsx_serialisable_meme_sans_anomalie():
    """Les onglets vides doivent rester écrivables par xlsx_multi_sheet."""
    xlsx = xlsx_multi_sheet(feuilles_check_odoo(_resultat(), BASE_URL))
    assert xlsx[:4] == b"PK\x03\x04"
