"""`/start` et `/help` : aide et annonce de l'instance servie (ADR-0022).

`COMMANDES` est la source de vérité de la surface : l'aide ci-dessous et le
menu natif publié au démarrage ([app.py](../app.py)) en dérivent tous deux.
Les tranches de migration par domaine (#152–#156) la font évoluer.
"""

from telegram import Update
from telegram.ext import ContextTypes

from electricore.api.config import settings
from electricore.bot.auth import require_allowed
from electricore.bot.format import escape

# Surface de commandes : (commande, description menu natif ≤ 256 car.).
COMMANDES: list[tuple[str, str]] = [
    ("start", "Aide et instance servie"),
    ("help", "Aide et instance servie"),
    ("etl", "Ingestion : lancer, suivre, statut des jobs"),
    ("flux", "Tables brutes : stats et exports XLSX"),
    ("entrees", "Entrées C15 (PMES, MES, CFNE)"),
    ("sorties", "Sorties C15 (RES, CFNS)"),
    ("taxes", "Accise TICFE / CTA par trimestre"),
    ("facturation", "Documents facturation Odoo ↔ Enedis"),
    ("check", "Vérifications pré-facturation"),
]


def aide_html() -> str:
    """Aide du bot : instance servie + surface de commandes."""
    if settings.instance_slug:
        titre = f"<b>ElectriCore Bot</b> — instance <b>{escape(settings.instance_slug)}</b>"
    else:
        titre = "<b>ElectriCore Bot</b>"
    lignes = [titre, ""]
    lignes += [f"/{commande} — {escape(description)}" for commande, description in COMMANDES]
    return "\n".join(lignes)


@require_allowed
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_message.reply_html(aide_html())


cmd_help = cmd_start
