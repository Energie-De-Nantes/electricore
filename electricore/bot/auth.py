"""Contrôle d'accès du bot : allowlist Telegram appliquée par décorateur.

Voir [CONTEXT.md](CONTEXT.md) — allowlist d'IDs numériques via
`TELEGRAM_ALLOWED_USERS`, pas de rôles ni de granularité (ADR-0010).
"""

import functools

from telegram import Update
from telegram.ext import ContextTypes

from electricore.api.config import settings


def require_allowed(handler):
    """N'exécute le handler que pour un utilisateur de l'allowlist ; sinon `⛔`."""

    @functools.wraps(handler)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        allowed = settings.get_telegram_allowed_users()
        if not allowed or update.effective_user.id not in allowed:
            await update.effective_message.reply_text("⛔ Accès refusé.")
            return None
        return await handler(update, context)

    return wrapper
