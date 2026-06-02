# Bot Telegram comme UI opérationnelle

Le pilotage opérationnel (lancement ETL, consultation de stats, export de tables, calculs Accise/CTA, contrôles pré-facturation) passe par un bot Telegram ([electricore/bot/](../../electricore/bot/)) plutôt qu'une webapp, une CLI ou des rapports email. Trois raisons cumulées :

1. **Mobile + push + zéro frontend** — Telegram fournit gratuitement une UI mobile, des notifications push et une interaction bidirectionnelle, sans avoir à construire/héberger/maintenir un frontend.
2. **Adoption nulle** — les opérateurs sont déjà sur Telegram, pas de nouveau compte ni d'app à installer.
3. **Notifications asynchrones bon marché** — un ETL peut tourner ~10 min ; renvoyer le résultat (fichier ou texte) à la fin du job, sans polling côté client, est natif sur Telegram.

## Conséquences

- La **surface de commandes** (`/etl`, `/taxes`, `/facturation`, etc.) fait partie du contrat utilisateur ; changer un nom ou un argument casse l'usage quotidien.
- L'**autorisation** repose sur un allowlist d'IDs Telegram (`TELEGRAM_ALLOWED_USERS`). Pas de rôles ni de granularité — adapté à une équipe restreinte.
- Dépendance à l'**infrastructure Telegram** : pas de plan de repli si Telegram est indisponible. Acceptable au regard du SLA opérationnel.
