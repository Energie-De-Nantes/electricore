# Contexte — bot (Telegram)

Vocabulaire spécifique au bot Telegram, client conversationnel de l'`api`. Voir [ADR-0010](../../docs/adr/0010-bot-telegram-ui-operationnelle.md).

## Bot

**Bot** :
Application [python-telegram-bot](https://docs.python-telegram-bot.org/) ([electricore/bot/](.)) qui expose des commandes Telegram appelant l'API ElectriCore. Pas de logique métier ; uniquement traduction commande Telegram → appel HTTP → formatage de la réponse.

**Commande** :
Handler Telegram déclenché par un message commençant par `/<nom>` (ex : `/etl`, `/taxes`, `/facturation`). Implémentée dans [bot.py](bot.py) comme une `async def cmd_<nom>(update, context)`.

**Allowlist Telegram** :
Liste d'identifiants numériques Telegram autorisés à utiliser le bot, configurée via `TELEGRAM_ALLOWED_USERS`. Tout autre utilisateur reçoit `⛔ Accès refusé`. Pas de rôles ni de granularité.

**Client API** :
Wrapper `httpx.AsyncClient` ([client.py](client.py)) qui factorise les appels HTTP vers l'API : injection automatique de la clé `X-API-Key`, gestion des timeouts (10 s pour les requêtes courtes, 300 s pour les exports lourds).
