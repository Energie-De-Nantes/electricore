"""Tests du rendu des messages du bot (`electricore/bot/format.py`).

Le bot rend tout en `parse_mode=HTML` (ADR-0022) : seuls `<`, `>` et `&`
sont à échapper — fin des pièges d'échappement Markdown V1/V2 (#151).
"""

from electricore.bot.format import escape


def test_escape_neutralise_les_caracteres_html():
    assert escape("a < b & c > d") == "a &lt; b &amp; c &gt; d"


def test_escape_laisse_intacts_les_caracteres_pieges_markdown():
    """Les caractères qui cassaient le Markdown (noms de tables, etc.) passent tels quels."""
    assert escape("flux_r151 *gras* [lien] `code`") == "flux_r151 *gras* [lien] `code`"


def test_escape_accepte_les_non_chaines():
    assert escape(42) == "42"
