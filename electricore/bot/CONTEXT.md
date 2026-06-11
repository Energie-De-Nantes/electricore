# Contexte — bot (Telegram)

Vocabulaire spécifique au bot Telegram, client conversationnel de l'`api`. Voir [ADR-0010](../../docs/adr/0010-bot-telegram-ui-operationnelle.md).

## Bot

**Bot** :
Application [python-telegram-bot](https://docs.python-telegram-bot.org/) ([electricore/bot/](.)) qui expose des commandes Telegram appelant l'API ElectriCore. Pas de logique métier ; uniquement traduction commande Telegram → appel HTTP → formatage de la réponse.

**Domaine** :
Commande top-level regroupant les actions d'un même périmètre métier : `/etl` (ingestion), `/flux` (tables brutes Enedis), `/perimetre` (entrées/sorties C15), `/taxes` (accise, CTA), `/facturation` (documents, contrôles pré-facturation). Invoqué sans argument, un domaine ouvre un clavier inline ; avec arguments, il agit directement (raccourci).
_Éviter_ : commande plate, sous-commande.

**Raccourci** :
Invocation d'un domaine avec arguments texte qui court-circuite la navigation par boutons (ex : `/taxes accise 2025-T1`). Produit le même résultat que le parcours par clavier.

**Confirmation** :
Validation explicite en deux taps exigée avant une action coûteuse ou risquée (ex : `resync`, qui purge l'état incrémental et re-télécharge tout le SFTP).

**Menu natif** :
Liste des commandes publiée à Telegram au démarrage du bot, adaptée à l'instance : les domaines dépendant d'un ERP (`/taxes`, `/facturation`) sont masqués quand aucun ERP n'est configuré.

**Chat de notification** :
Chat Telegram destinataire des alertes proactives du bot (échec d'un job ETL, y compris ceux lancés par le scheduler), distinct des réponses aux commandes.

**Allowlist Telegram** :
Liste d'identifiants numériques Telegram autorisés à utiliser le bot, configurée via `TELEGRAM_ALLOWED_USERS`. Tout autre utilisateur reçoit `⛔ Accès refusé`. Pas de rôles ni de granularité.

**Client API** :
Wrapper `httpx.AsyncClient` ([client.py](client.py)) qui factorise les appels HTTP vers l'API : injection automatique de la clé `X-API-Key`, gestion des timeouts (10 s pour les requêtes courtes, 120 s pour les exports de tables, 300 s pour les livrables calculés). Le squelette httpx vit dans les primitives `_get_json` / `_get_bytes` / `_post_json` ; les méthodes publiques ne déclarent que chemin et budget de timeout.

**Livraison** :
Remise d'un *livrable* (cf. [core/CONTEXT.md](../core/CONTEXT.md)) dans le chat, avec suivi de progression (⏳ pendant le calcul, 📥 à l'envoi) et filet d'erreur (❌ édité sur le message de suivi — aucun chemin ne laisse le ⏳ figé). Deux primitives composables dans [livraison.py](livraison.py) : `etape()` (suivi + filet, retourne `None` quand l'échec est déjà signalé) et `envoyer_document()` (étape + document + confirmation). Tous les domaines passent par là ; le format des messages de suivi vit en un seul endroit (issue #174).
_Éviter_ : envoi (ne dit ni le suivi ni le filet), remise (collision avec le rabais commercial).

**Instance** :
Déploiement complet de la stack pour un opérateur donné (voir [ADR-0015](../../docs/adr/0015-deploiement-multi-instance.md)). Chaque instance a son propre bot Telegram, nommé `@<slug>_electricore_bot` ; `/start` annonce l'instance servie.
