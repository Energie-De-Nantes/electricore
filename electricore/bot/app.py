"""Assemblage du bot Telegram : application PTB, surface, menu natif (#151).

Point d'entrée unique du bot (consommé par le lifespan de l'API). La surface
affichée (aide + menu natif) dérive de `handlers.start.COMMANDES`.
"""

from telegram import BotCommand
from telegram.ext import Application, CommandHandler

from electricore.bot import bot as v1
from electricore.bot.handlers import start


async def publier_menu(application: Application) -> None:
    """Publie la surface dans le menu natif Telegram (`setMyCommands`, ADR-0022)."""
    await application.bot.set_my_commands([BotCommand(c, d) for c, d in start.COMMANDES])


def build_application(token: str) -> Application:
    application = Application.builder().token(token).post_init(publier_menu).build()
    application.add_handler(CommandHandler("start", start.cmd_start))
    application.add_handler(CommandHandler("help", start.cmd_help))
    # Commandes v1 — migrent domaine par domaine (#152–#156, ADR-0022).
    application.add_handler(CommandHandler("etl", v1.cmd_etl))
    application.add_handler(CommandHandler("status", v1.cmd_status))
    application.add_handler(CommandHandler("stats", v1.cmd_stats))
    application.add_handler(CommandHandler("export", v1.cmd_export))
    application.add_handler(CommandHandler("flux", v1.cmd_flux))
    application.add_handler(CommandHandler("entrees", v1.cmd_entrees))
    application.add_handler(CommandHandler("sorties", v1.cmd_sorties))
    application.add_handler(CommandHandler("taxes", v1.cmd_taxes))
    application.add_handler(CommandHandler("facturation", v1.cmd_facturation))
    application.add_handler(CommandHandler("check", v1.cmd_check))
    return application
