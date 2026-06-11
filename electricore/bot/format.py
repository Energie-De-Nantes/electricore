"""Rendu des messages Telegram en HTML (ADR-0022).

Tout le bot rend en `parse_mode=HTML` : seuls `<`, `>` et `&` sont à échapper,
via `escape()` — fin du mélange Markdown V1/V2 et de ses pièges d'échappement.
"""

import html


def escape(text: object) -> str:
    """Neutralise `<`, `>` et `&` pour insertion sûre dans un message HTML Telegram."""
    return html.escape(str(text), quote=False)
