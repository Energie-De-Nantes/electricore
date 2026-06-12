# Bot Telegram ElectriCore

UI opérationnelle d'ElectriCore ([ADR-0010](../../docs/adr/0010-bot-telegram-ui-operationnelle.md)) :
pilotage de l'ingestion, exports, taxes et contrôles pré-facturation depuis Telegram.
Pur client HTTP de l'[API](../api/README.md) — aucune logique métier
(garde-fou : [tests/architecture/test_bot_topology.py](../../tests/architecture/test_bot_topology.py)).

Vocabulaire : [CONTEXT.md](CONTEXT.md). Décision de surface : [ADR-0022](../../docs/adr/0022-surface-bot-domaines-hybrides.md).

## Surface de commandes

Cinq **domaines** métier + l'aide. Chaque domaine invoqué **sans argument ouvre un
clavier inline** (découvrable, zéro syntaxe) ; **avec arguments, il agit directement**
(raccourci power-user). Le menu natif Telegram (☰) est publié au démarrage et
adapté à l'instance.

| Domaine | Clavier | Raccourcis |
|---|---|---|
| `/ingestion` (alias `/etl`) | All · Test · Rebuild · Flux… · Resync · Statut | `/ingestion rebuild`, `/ingestion r151 c15`, `/ingestion statut` |
| `/flux` | tables avec descriptions, 📊 stats · 📥 export | `/flux stats c15`, `/flux export r151` |
| `/perimetre` | 📥 Entrées · 📤 Sorties (C15) | `/perimetre entrees`, `/perimetre sorties` |
| `/taxes` ⁽ᵉʳᵖ⁾ | Accise · CTA → trimestre (4 derniers + toutes périodes) | `/taxes accise 2025-T1`, `/taxes cta` |
| `/facturation` ⁽ᵉʳᵖ⁾ | Documents (choix du mois) · Check Odoo | `/facturation documents 2026-05-01`, `/facturation check` |
| `/start`, `/help` | — | annonce l'instance servie et la surface |

⁽ᵉʳᵖ⁾ masqué du menu et dégradé proprement quand aucun ERP n'est configuré
(`is_odoo_configured`, cf. ADR-0016).

Comportements notables :

- **`resync` exige une confirmation à deux taps** (purge l'état incrémental dlt et
  re-télécharge tout le SFTP) — y compris en raccourci texte.
- **Suivi de job par édition** : le message de lancement d'ingestion s'édite
  (`⏳ running` → `✅ completed` / `❌ failed` + sortie), pas de second message.
- **`reset` est retiré** de la surface (déprécié côté runner, alias de `resync`).
- Les exports partent en **documents XLSX** dans le chat.

## Alertes proactives

Si `TELEGRAM_NOTIFY_CHAT_ID` est défini, le bot surveille les jobs d'ingestion via l'API
et pousse une alerte 🚨 quand un job passe à `failed` — **y compris ceux lancés
par le scheduler** (supercronic). Pas d'alerte sur l'historique antérieur au
démarrage, pas de doublon. Vide = désactivé.

## Configuration

| variable | rôle |
|---|---|
| `TELEGRAM_BOT_TOKEN` | token BotFather ; vide = bot non démarré |
| `TELEGRAM_ALLOWED_USERS` | allowlist d'IDs Telegram (virgules) — obligatoire |
| `TELEGRAM_NOTIFY_CHAT_ID` | chat des alertes proactives ; vide = désactivé |
| `API_BASE_URL` | URL de l'API appelée par le bot (défaut `http://localhost:8001`) |
| `INSTANCE_SLUG` | slug de l'instance, annoncé par `/start` (ADR-0015) |

Convention de nommage : un bot par instance, `@<slug>_electricore_bot`
(ex. `@edn_electricore_bot`).

Le bot démarre dans le **process de l'API** (lifespan FastAPI) dès que
`TELEGRAM_BOT_TOKEN` est défini — en local :

```bash
uv run uvicorn electricore.api.main:app --port 8001
```

(le port doit correspondre à `API_BASE_URL`, sinon les commandes échouent en
« connexion refusée »).

## Architecture

```
bot/
├── app.py             # assemblage : build_application, menu natif, surveillance
├── auth.py            # @require_allowed (allowlist), @require_odoo (no-ERP)
├── client.py          # client HTTP async vers l'API (X-API-Key)
├── format.py          # rendu HTML (escape) — parse_mode=HTML partout
├── surveillance.py    # alertes proactives (polling /ingestion/jobs)
├── tasks.py           # tâches de fond (annulées au shutdown)
└── handlers/          # un module par domaine de commande
    ├── start.py       # /start, /help — COMMANDES = source de vérité de la surface
    ├── ingestion.py
    ├── flux.py
    ├── perimetre.py
    ├── taxes.py
    └── facturation.py
```

Un seul poller Telegram par token : pour tester en local avec le token de prod,
arrêter d'abord le conteneur `api` de la prod (`docker compose stop api`) — ou
utiliser un bot de dev dédié.
