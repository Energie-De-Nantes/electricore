"""Tests du rendu HTML du check pré-facturation (`electricore/bot/handlers/facturation.py`).

Le rendu passe en `parse_mode=HTML` (#151) : liens `<a>`, gras `<b>`,
échappement centralisé — fin des pièges Markdown sur les noms Odoo.
"""

from electricore.bot.handlers.facturation import _format_check_odoo


def _resultat(**surcharges) -> dict:
    base = {
        "rsc_manquante": [],
        "cfne_manquante": [],
        "invoicing_state_counts": {},
        "factures_draft": [],
        "lisses_quantite_1": [],
        "brouillons_hors_ancre": [],
    }
    return {**base, **surcharges}


def test_les_anomalies_sont_rendues_en_liens_html_echappes():
    msg, xlsx_needed = _format_check_odoo(
        _resultat(rsc_manquante=[{"name": "S00042 <bis> & co", "url": "https://odoo.example/web?a=1&b=2"}])
    )

    assert '<a href="https://odoo.example/web?a=1&amp;b=2">S00042 &lt;bis&gt; &amp; co</a>' in msg
    assert xlsx_needed is False


def test_le_rendu_est_du_html_sans_residus_markdown():
    msg, _ = _format_check_odoo(_resultat(invoicing_state_counts={"up_to_date": 12}))

    assert "<b>" in msg, "les titres sont en gras HTML"
    assert "](" not in msg, "plus de liens Markdown"
    assert "up_to_date" in msg


def test_tout_vert_affiche_le_feu_vert():
    msg, xlsx_needed = _format_check_odoo(_resultat())

    assert "OK pour lancer le cycle de facturation" in msg
    assert xlsx_needed is False


def test_factures_draft_rendues_avec_les_colonnes_reelles():
    """Régression prod 2026-06-12 : KeyError sale_order_name — les colonnes
    réelles du check sont `name` (commande) et `name_account_move` (facture)."""
    msg, _ = _format_check_odoo(
        _resultat(
            factures_draft=[
                {"name": "S00042", "account_move_id": 7, "name_account_move": "INV/2026/0001", "url": "https://o/7"}
            ]
        )
    )

    assert "S00042" in msg and "INV/2026/0001" in msg


def test_lisses_rendus_avec_les_colonnes_reelles():
    """Même famille : les lissés portent `name` + `categ_names`, pas `sale_order_name`."""
    msg, _ = _format_check_odoo(
        _resultat(lisses_quantite_1=[{"name": "S00043", "categ_names": ["Base", "HP"], "url": "https://o/8"}])
    )

    assert "S00043" in msg and "Base, HP" in msg


def test_brouillons_hors_ancre_rendus_avec_la_date():
    """#564 : le check pré-campagne signale la commande, la facture et la date fautive."""
    msg, xlsx_needed = _format_check_odoo(
        _resultat(
            brouillons_hors_ancre=[
                {
                    "name": "S00099",
                    "account_move_id": 9,
                    "name_account_move": "INV/2026/0099",
                    "invoice_date": "2026-06-05",
                    "url": "https://o/9",
                }
            ]
        )
    )

    assert "S00099" in msg and "INV/2026/0099" in msg and "2026-06-05" in msg
    assert xlsx_needed is False


def test_brouillon_sans_date_affiche_absente():
    msg, _ = _format_check_odoo(
        _resultat(
            brouillons_hors_ancre=[
                {
                    "name": "S00100",
                    "account_move_id": 10,
                    "name_account_move": "INV/2026/0100",
                    "invoice_date": None,
                    "url": "https://o/10",
                }
            ]
        )
    )

    assert "absente" in msg


def test_brouillons_hors_ancre_comptent_dans_le_feu_vert():
    """Une anomalie hors-ancre à elle seule empêche le feu vert."""
    msg, _ = _format_check_odoo(
        _resultat(
            brouillons_hors_ancre=[
                {
                    "name": "S00099",
                    "account_move_id": 9,
                    "name_account_move": "INV/0099",
                    "invoice_date": None,
                    "url": "https://o/9",
                }
            ]
        )
    )

    assert "OK pour lancer le cycle de facturation" not in msg


def test_message_trop_long_bascule_sur_le_xlsx():
    """Un résumé qui dépasserait la limite Telegram (4096) est raccourci et
    renvoie vers le XLSX de détail au lieu de faire échouer l'édition."""
    nombreuses = [
        {"name": f"S{i:05}", "url": f"https://odoo.example/web#id={i}&model=sale.order&view_type=form"}
        for i in range(20)
    ]
    msg, xlsx_needed = _format_check_odoo(_resultat(rsc_manquante=list(nombreuses), cfne_manquante=list(nombreuses)))

    assert len(msg) <= 4096
    assert xlsx_needed is True
