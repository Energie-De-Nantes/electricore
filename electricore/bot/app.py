"""Assemblage du bot Telegram : application PTB, surface, menu natif (#151).

Point d'entrée unique du bot (consommé par le lifespan de l'API). La surface
affichée (aide + menu natif) dérive de `handlers.start.COMMANDES`.
"""

from telegram import BotCommand
from telegram.ext import Application, CallbackQueryHandler, CommandHandler

from electricore.bot.handlers import etl, facturation, flux, perimetre, start, taxes


async def publier_menu(application: Application) -> None:
    """Publie la surface de l'instance dans le menu natif (`setMyCommands`, ADR-0022, #159)."""
    await application.bot.set_my_commands([BotCommand(c, d) for c, d in start.commandes_disponibles()])


def build_application(token: str) -> Application:
    application = Application.builder().token(token).post_init(publier_menu).build()
    application.add_handler(CommandHandler("start", start.cmd_start))
    application.add_handler(CommandHandler("help", start.cmd_help))
    # Domaine /etl (#152) — clavier inline + raccourcis, absorbe /status.
    application.add_handler(CommandHandler("etl", etl.cmd_etl))
    application.add_handler(CallbackQueryHandler(etl.on_callback, pattern="^etl:"))
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
