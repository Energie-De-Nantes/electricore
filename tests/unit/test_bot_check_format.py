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
