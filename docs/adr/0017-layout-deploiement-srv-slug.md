# Layout de déploiement : user système par instance dans `/srv/<slug>/`

## Contexte

[ADR-0011](0011-deploiement-vps-docker.md) a posé la stack Docker Compose mono-tenant, [ADR-0015](0015-deploiement-multi-instance.md) a tranché « 1 VPS = 1 fournisseur ». La procédure d'installation de [docs/deploiement.md](../deploiement.md) place la stack dans `/opt/electricore/` exécutée par `root` — défaut historique hérité de la première mise en prod EDN. Comme on automatise désormais l'installation via un script unique ([deploy/install.sh](../../deploy/install.sh)), il faut fixer **où vit une instance sur le VPS** et **qui en est propriétaire**. Le choix est gravé dans les chemins de backup, le crontab, le `docker-compose.yml` et la doc — donc difficile à revenir.

## Décision

Chaque instance est portée par un **user système dédié** dont le login est le slug de l'instance, avec son home dans `/srv/<slug>/` :

- **User** : `useradd -m -d /srv/<slug> -s /bin/bash <slug>`, ajouté au groupe `docker`, pas de sudo.
- **Home** : `/srv/<slug>/` contient `.env`, `deploy/docker/{docker-compose.yml, Caddyfile, crontab, backup_duckdb.sh}`, `backups/`.
- **Accès SSH** : direct sur le user (`ssh <slug>@<vps>`). Le script copie `~root/.ssh/authorized_keys` vers `~<slug>/.ssh/authorized_keys` par défaut, override possible via `--ssh-pubkey`.
- **Backups DuckDB** : bind-mount vers `/srv/<slug>/backups/` (remplace le volume Docker nommé `duckdb_backups` historique).
- **Ownership** : tous les artefacts sous `/srv/<slug>/` appartiennent à `<slug>:<slug>`, y compris les fichiers écrits par les conteneurs (uid alignée).

Le script `install.sh` traite mono-instance par VPS comme aujourd'hui. La convention `/srv/<slug>/` + user `<slug>` est néanmoins choisie pour rester **trivialement transférable** à un éventuel multi-instance futur (chaque instance = un user, un home, des volumes propres), sans coder ce multi-instance ici.

## Raison

1. **Préparer un éventuel multi-instance sans le faire.** ADR-0015 garde « 1 VPS = 1 instance » comme horizon assumé, mais le coût de poser dès maintenant la convention nominative `<slug>` (plutôt qu'un user générique `electricore` et un chemin `/opt/electricore/`) est nul. Si on revoit la décision ADR-0015 dans deux ans, monter une 2e instance sur le même VPS ne demandera pas de renommer l'existante.
2. **Sémantique FHS correcte.** `/srv` est défini comme « data servie par le système » (un VPS qui sert une instance ElectriCore). `/opt` est pour paquets tiers self-contained, `/home` pour des humains. `/srv/<slug>/` rend l'intention lisible.
3. **Isolation à coût zéro.** Un user dédié rend visible « qui possède quoi » sans la complexité de Docker rootless (bind-mounts, network, perf I/O DuckDB). Pas une vraie isolation sécurité (le user est dans `docker`, donc équivalent root via le daemon) — l'objectif est l'hygiène opérationnelle, pas un sandbox.
4. **Ops ssh-friendly.** `ssh <slug>@<vps>` pose directement l'opérateur dans `/srv/<slug>/` avec les bonnes perms et le bon `docker compose` (lit `.env` et `deploy/docker/` au bon endroit). Évite la danse `ssh root` → `cd /opt/electricore` → `sudo docker compose ...` à chaque intervention.

## Alternatives écartées

- **`/opt/electricore/` + exécution root (statu quo).** Convention de la doc historique. Suffit en mono-instance vraiment-mono, mais le passage futur à un multi-instance « doux » exigerait de renommer la prod existante. Évité parce que le coût de la convention aujourd'hui est nul.
- **`/opt/electricore/` + user `electricore` générique.** Isolation user OK mais le nom du projet n'est pas le nom de l'instance — friction structurelle si on monte une 2e instance.
- **`/home/<slug>/` au lieu de `/srv/<slug>/`.** Plus familier (`useradd -m` y dépose le home par défaut) mais sémantiquement faux (`<slug>` n'est pas un utilisateur humain). Marginal mais on prend le terme correct.
- **Docker rootless avec user `<slug>` sans accès au socket Docker host.** Isolation sécurité plus forte. Coût : bind-mounts moins ergonomiques, network plus contraint, perf I/O DuckDB dégradée. Gain marginal en mono-instance ; à reconsidérer si on bascule un jour vers du multi-instance prod sur même VPS.

## Conséquences

- **[docs/deploiement.md](../deploiement.md) réécrit** : chemin principal devient « lance le script », ancien flux manuel relégué en annexe « pour comprendre / dépanner ». Tous les `/opt/electricore/` → `/srv/<slug>/`.
- **[deploy/install.sh](../../deploy/install.sh) créé** : script bash unique, détecte OS (Ubuntu 22+/24+ ou Debian 12+), installe Docker si absent, crée le user `<slug>`, télécharge la config tag-pinned depuis GHCR, ouvre `$EDITOR` sur `.env`, valide (regex AES hex, URL parseable, slug), check DNS bloquant, démarre la stack, lance un ETL `mode=test`. Idempotent : à la relance, mode « reconfigure » (édition `.env`, restart) sans toucher à la DB.
- **[deploy/docker/docker-compose.yml](../../deploy/docker/docker-compose.yml) modifié** : volume nommé `duckdb_backups` remplacé par un bind-mount `/srv/<slug>/backups:/backups`. Le `<slug>` est injecté à l'install via substitution de variable d'environnement (`BACKUPS_PATH=/srv/${INSTANCE_SLUG}/backups`) ou via le compose lui-même résolvant `${INSTANCE_SLUG}`. Choix d'implémentation tranché à la rédaction du compose.
- **`deploy/docker/.env.example` enrichi** : marqueurs `# TODO: à remplir` sur les variables obligatoires + commentaires de validation (longueur, format) pour orienter le wizard d'édition. *(Fichier et wizard d'édition `.env` retirés au cutover secrets-as-code — cf. [ADR-0044](0044-secrets-as-code-sops-age.md) §8 / [ADR-0046](0046-convention-noms-env-par-domaine-identite-secrets.md) ; la config vit désormais dans `config.env`/`secrets.env` du dépôt de déploiement.)*
- **[ADR-0011](0011-deploiement-vps-docker.md) reste valide tel quel.** Il pose le choix de la stack (4 conteneurs sur VPS unique, pas de PaaS/K8s) sans spécifier le layout système hôte. ADR-0017 *complète* ADR-0011 en tranchant ce qui n'avait pas été explicité : qui possède les fichiers, où ils vivent. Aucune modification d'ADR-0011 nécessaire.
- **Instances déjà déployées** : pas d'auto-migration. Procédure manuelle documentée dans [docs/deploiement.md](../deploiement.md) (créer le user, `mv /opt/electricore /srv/<slug>`, chown, recréer la stack). Une instance EDN existante peut continuer à tourner sur l'ancien layout — la migration est opportuniste, pas urgente.

## Limites à connaître avant un changement d'échelle

- **Le script reste mono-instance par VPS.** Cette ADR pose la convention nominative qui rendra le multi-instance trivial à coder *si* on décide de le faire, mais ne le code pas. Si un jour on veut monter `enargia` à côté de `edn` sur le même VPS, il faudra trancher : Caddy mutualisé (option écartée par ADR-0015), préfixage `COMPOSE_PROJECT_NAME=<slug>` pour isoler les volumes/conteneurs Docker, conflit de ports 80/443. Toutes ces décisions restent ouvertes.
- **Le user `<slug>` est dans le groupe `docker`.** N'importe quel acteur capable de se loguer en `<slug>` peut `docker exec --user=root` sur n'importe quel conteneur du VPS. Le user *isole* mais ne *sécurise* pas — il sert à la lisibilité opérationnelle et à la transférabilité du layout, pas à un modèle de menace.
- **Pas de garantie d'unicité du slug à l'échelle du VPS.** Si l'opérateur lance le script avec un slug déjà utilisé comme nom de user système (ex : un compte humain qui s'appellerait `edn`), `useradd` échoue. Le script doit détecter et refuser, en demandant un autre slug ou en confirmant la reprise de l'existant.
