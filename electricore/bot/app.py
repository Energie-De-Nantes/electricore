"""Assemblage du bot Telegram : application PTB, surface, menu natif (#151).

Point d'entrée unique du bot (consommé par le lifespan de l'API). La surface
affichée (aide + menu natif) dérive de `handlers.start.COMMANDES`.
"""

from telegram import BotCommand
from telegram.ext import Application, CallbackQueryHandler, CommandHandler

from electricore.api.config import settings
from electricore.bot import surveillance
from electricore.bot.handlers import facturation, flux, ingestion, perimetre, start, taxes
from electricore.bot.tasks import create_task


async def publier_menu(application: Application) -> None:
    """Publie la surface de l'instance dans le menu natif (`setMyCommands`, ADR-0022, #159)."""
    await application.bot.set_my_commands([BotCommand(c, d) for c, d in start.commandes_disponibles()])


async def demarrer(application: Application) -> None:
    """post_init : menu natif + surveillances proactives — jobs d'ingestion (#157), péremption des taux (#186)."""
    await publier_menu(application)
    if settings.telegram_notify_chat_id:
        create_task(surveillance.boucle_surveillance(application.bot, settings.telegram_notify_chat_id))
        create_task(surveillance.boucle_peremption(application.bot, settings.telegram_notify_chat_id))


def build_application(token: str) -> Application:
    application = Application.builder().token(token).post_init(demarrer).build()
    application.add_handler(CommandHandler("start", start.cmd_start))
    application.add_handler(CommandHandler("help", start.cmd_help))
    # Domaine /ingestion (#152, ex-/etl) — clavier inline + raccourcis, absorbe /status.
    application.add_handler(CommandHandler("ingestion", ingestion.cmd_ingestion))
    # Alias de transition pour la mémoire musculaire des opérateurs.
    application.add_handler(CommandHandler("etl", ingestion.cmd_ingestion))
    application.add_handler(CallbackQueryHandler(ingestion.on_callback, pattern="^(ingestion|etl):"))
    # Domaine /flux (#153) — stats + exports des tables brutes, absorbe /stats et /export.
    application.add_handler(CommandHandler("flux", flux.cmd_flux))
    application.add_handler(CallbackQueryHandler(flux.on_callback, pattern="^flux:"))
    # Domaine /perimetre (#154) — entrées/sorties C15, absorbe /entrees et /sorties.
    application.add_handler(CommandHandler("perimetre", perimetre.cmd_perimetre))
    application.add_handler(CallbackQueryHandler(perimetre.on_callback, pattern="^perimetre:"))
    # Domaine /taxes (#155) — accise/CTA par boutons, trimestres dérivés.
    application.add_handler(CommandHandler("taxes", taxes.cmd_taxes))
    application.add_handler(CallbackQueryHandler(taxes.on_callback, pattern="^taxes:"))
    # Domaine /facturation (#156) — documents + contrôles, absorbe /check.
    application.add_handler(CommandHandler("facturation", facturation.cmd_facturation))
    application.add_handler(CallbackQueryHandler(facturation.on_callback, pattern="^facturation:"))
    return application
