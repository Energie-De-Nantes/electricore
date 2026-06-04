# Déploiement multi-instance (un VPS par fournisseur)

Pour servir plusieurs fournisseurs (EDN, Enargia, …) sans réécrire ElectriCore en multi-tenant, chaque fournisseur reçoit une *instance* dédiée : un VPS, sa propre stack `docker-compose`, sa propre base DuckDB, ses clés AES, sa config Odoo, sa source SFTP, son bot Telegram, son sous-domaine. Approche assumée : on duplique le déploiement mono-tenant de l'[ADR-0011](0011-deploiement-vps-docker.md) plutôt que d'isoler logiquement dans le code.

## Identification d'une instance

Une variable `INSTANCE_SLUG` (ex : `edn`, `enargia`) figure dans le `.env` de chaque instance et est lue par l'app pour matérialiser l'identité :

- `/health` retourne `{"status": "ok", "instance": "edn"}` — utile pour le monitoring distant.
- Le titre OpenAPI affiche `ElectriCore API — EDN`.
- Les backups DuckDB sont nommés `backup_edn_<date>.duckdb` (évite les collisions si plusieurs instances poussent vers un store central).
- Au boot, l'API logue `Instance=edn connectée à odoo_db=<db>, sftp=<host>` — garde-fou minimal contre un `.env` mal copié (responsabilité humaine en dernier ressort, pas de blocage strict).

`INSTANCE_SLUG` n'apparaît **pas** dans les logs structurés ni dans les messages Telegram : un bot = une instance, le bot lui-même indique l'origine.

## DNS et certificats

Le domaine `electricore.fr` héberge un sous-domaine par instance (`edn.electricore.fr`, `enargia.electricore.fr`). Chaque VPS gère son propre certificat Let's Encrypt via le challenge HTTP-01 de Caddy ([deploy/docker/Caddyfile.example](../../deploy/docker/Caddyfile.example)) — pas de wildcard (incompatible avec l'isolation par VPS, exigerait DNS-01).

## Cycles de release indépendants

Chaque instance pin `ELECTRICORE_VERSION` dans son `.env`. Le déclenchement est manuel (SSH `docker compose pull && up -d` à 2-3 instances ; un script Ansible deviendra rentable au-delà). Conséquence assumée : roll-out progressif (tester sur une instance, valider, puis bumper la suivante), rollback indépendant, divergences temporaires de version acceptées.

## Alternatives écartées

- **Multi-tenant applicatif** (1 stack, `tenant_id` partout) : refactor profond (toutes les requêtes DuckDB, services API, scheduler, bot). Bénéfice nul à 2-3 fournisseurs, coût élevé.
- **Multi-stack sur 1 VPS** (N `docker-compose` isolés, Caddy mutualisé) : économise ~5 €/mois × N mais réintroduit une coordination opérationnelle (nommage des containers et volumes, partage des ressources, `down/up` croisés). Pas justifié au-delà de la simple économie marginale.
- **Watchtower (auto-pull `latest`)** : déploiement non maîtrisé, une régression toucherait toutes les instances simultanément. Incompatible avec le besoin de roll-out progressif.

## Limites

- **2-3 instances reste l'horizon assumé.** Au-delà de ~5, le provisioning manuel et la coordination des releases deviennent pénibles ; il faudra alors investir dans Ansible/Terraform ou reconsidérer le multi-tenant applicatif.
- **Pas de garde-fou strict** contre un `.env` mal configuré (mauvaise base Odoo). Le log de boot signale, mais la responsabilité finale reste humaine.
- **Le bot Telegram d'une instance ne voit que cette instance.** Pas de vue globale cross-instance ; le monitoring transverse repose sur les endpoints `/health` distincts.
