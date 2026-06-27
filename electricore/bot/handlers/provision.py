"""Domaine `/provision` : estimation de provision des lissés (#487, ADR-0048).

`/provision <pdl>` dérive en cœur-pur, depuis flux_r67 (cold-start), la *provision d'énergie*
d'un lissé en **kWh** (annuel par cadran + provision mensuelle `/12` plate) + couverture /
profondeur / qualité, et un **signal alertable** (couverture insuffisante ou qualité estimée
— la lib expose le signal, l'aval alerte, ADR-0037). **Aucun €** (prix fournisseur = ERP,
ADR-0016/0027). ERP-agnostique : pas de `@require_odoo` (chemin DuckDB pur).
"""

from telegram import Update
from telegram.ext import ContextTypes

from electricore.bot.auth import require_allowed
from electricore.bot.client import ElectriCoreClient
from electricore.bot.format import escape

_USAGE = "Usage : /provision <PDL>  (ex: /provision 12345678901234)"

# Cadrans à afficher dans l'ordre métier (base + hp/hc ; les 4 sous-cadrans restent dans
# l'API mais on garde le texte du bot lisible).
_CADRANS_AFFICHES = (("base", "Base"), ("hp", "HP"), ("hc", "HC"))


def formater_estimation(body: dict) -> str:
    """Texte HTML de l'estimation de provision (provision base/HP/HC + couverture + signal)."""
    pdl = body.get("pdl", "?")
    if not body.get("trouve"):
        return (
            f"<b>Provision</b> — <code>{escape(pdl)}</code>\n"
            "Aucune mesure R67 dans la fenêtre de 12 mois : estimation impossible (repli manuel)."
        )

    est = body["estimation"]
    lignes = [f"<b>Provision estimée</b> — <code>{escape(pdl)}</code>"]

    # Provision mensuelle (kWh) par cadran présent (profondeur cohérente déclarée).
    lignes_cadran = []
    for cle, libelle in _CADRANS_AFFICHES:
        mensuel = est.get(f"energie_{cle}_mensuel_kwh")
        annuel = est.get(f"energie_{cle}_kwh")
        if mensuel is not None:
            lignes_cadran.append(f"  • {libelle} : <b>{mensuel:.0f} kWh/mois</b> ({annuel:.0f} kWh/an)")
    lignes.append("Provision mensuelle (/12 plate) :")
    lignes.extend(lignes_cadran)

    profondeur = est.get("profondeur_cadran", "base")
    lignes.append(f"Profondeur cadran : {escape(profondeur)}")

    # Couverture.
    mois = est.get("couverture_mois")
    debut, fin = est.get("couverture_debut"), est.get("couverture_fin")
    suffisante = est.get("couverture_suffisante")
    drapeau = "✅" if suffisante else "⚠️"
    mois_txt = f"{mois:.1f}" if mois is not None else "?"
    lignes.append(f"Couverture : {drapeau} {mois_txt} mois ({escape(debut)} → {escape(fin)})")

    # Qualité + signal alertable.
    qualite = est.get("qualite", "?")
    regul = " · régularisation présente" if est.get("presence_regularisation") else ""
    lignes.append(f"Qualité : {escape(qualite)}{escape(regul)}")
    if est.get("signal_alertable"):
        lignes.append("🔔 Signal : estimation à vérifier (couverture insuffisante ou qualité estimée).")

    return "\n".join(lignes)


@require_allowed
async def cmd_provision(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.effective_message.reply_text(_USAGE)
        return

    pdl = context.args[0]
    msg = await update.effective_message.reply_html(f"⏳ Estimation de provision — <code>{escape(pdl)}</code>…")
    try:
        body = await ElectriCoreClient().get_provision_estimation(pdl)
    except Exception as e:
        await context.bot.edit_message_text(
            f"❌ Erreur : <code>{escape(e)}</code>",
            chat_id=msg.chat_id,
            message_id=msg.message_id,
            parse_mode="HTML",
        )
        return
    await context.bot.edit_message_text(
        formater_estimation(body), chat_id=msg.chat_id, message_id=msg.message_id, parse_mode="HTML"
    )
