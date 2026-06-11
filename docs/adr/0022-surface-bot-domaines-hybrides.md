# Surface de commandes du bot par domaines hybrides

La surface du bot Telegram passe de 11 commandes plates (`/etl`, `/status`, `/stats`, `/export`, `/flux`, `/entrees`, `/sorties`, `/taxes`, `/facturation`, `/check`, `/start`) à 5 domaines métier : `/etl` (ingestion), `/flux` (tables brutes Enedis), `/perimetre` (entrées/sorties C15), `/taxes` (accise, CTA), `/facturation` (documents + contrôles pré-facturation). Un domaine invoqué sans argument ouvre un clavier inline — découvrabilité, zéro syntaxe à retenir — ; avec arguments, il agit directement (raccourci power-user, ex : `/taxes accise 2025-T1`). Les actions coûteuses (`resync` : purge de l'état incrémental dlt + re-téléchargement SFTP complet) exigent une confirmation à deux taps. La bascule est big bang, sans alias de compatibilité.

## Alternatives rejetées

- **Surface plate cohérente** (`/etl_run`, `/taxes_accise`…) : autocomplete maximal dans le menu natif, mais le menu grossit linéairement avec chaque fonction.
- **Hub `/menu` unique** 100 % boutons : zéro syntaxe, mais perd l'autocomplete, le deep-linking par commande et la rapidité power-user.
- **Alias de transition** des anciennes commandes : code mort garanti pour une allowlist de quelques personnes joignables directement.

## Conséquences

- [ADR-0010](0010-bot-telegram-ui-operationnelle.md) reste valide : le choix Telegram et le statut de contrat de la surface ne changent pas. Le contrat est cassé une seule fois, annoncé dans le chat des opérateurs.
- Le menu natif (`setMyCommands`) est publié au démarrage et adapté à l'instance : les domaines ERP-dépendants (`/taxes`, `/facturation`) sont masqués quand aucun ERP n'est configuré, et répondent par un message explicite s'ils sont tapés quand même.
- Chaque instance a son bot, nommé `@<slug>_electricore_bot` ; `/start` annonce l'instance servie.
- Le mode déprécié `reset` disparaît de la surface ; `rebuild`, `resync` et la sélection de flux arbitraire (déjà supportés par l'API) y entrent.
