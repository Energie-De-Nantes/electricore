# Déploiement VPS

Guide opérationnel pour déployer ElectriCore sur un VPS via la stack Docker
([`deploy/docker/`](../deploy/docker/)).

Le script [`deploy/install.sh`](../deploy/install.sh) automatise la mise en place
de bout en bout. Ce document décrit son usage, puis ce qu'il fait sous le capot
(annexe « déploiement manuel »).

## Sommaire

1. [Quickstart](#quickstart)
2. [Prérequis](#prérequis)
3. [Variables `.env`](#variables-env)
4. [Provisionner une nouvelle instance](#provisionner-une-nouvelle-instance)
5. [Reconfigurer une instance existante](#reconfigurer-une-instance-existante)
6. [Durcissement du VPS](#durcissement-du-vps)
7. [Accès distant depuis un notebook Python](#accès-distant-depuis-un-notebook-python)
8. [Mode SFTP distant vs fichiers collocés](#mode-sftp-distant-vs-fichiers-collocés)
9. [Rotation des clés AES](#rotation-des-clés-aes)
10. [Sauvegarde et restauration](#sauvegarde-et-restauration)
11. [Mise à jour de version](#mise-à-jour-de-version)
12. [Fenêtre d'ingestion et concurrence DuckDB](#fenêtre-dingestion-et-concurrence-duckdb)
13. [Migration depuis l'ancien layout `/opt/electricore/`](#migration-depuis-lancien-layout-optelectricore)
14. [Annexe : déploiement manuel pas-à-pas](#annexe--déploiement-manuel-pas-à-pas)
15. [Dépannage](#dépannage)

---

## Quickstart

Sur un VPS Ubuntu 22.04+/24.04+ ou Debian 12+ fraîchement provisionné, en root :

```bash
curl -fsSL https://raw.githubusercontent.com/Energie-De-Nantes/electricore/main/deploy/install.sh -o install.sh
sudo bash install.sh \
    --slug <slug> --domain <slug>.electricore.fr --email ops@example.com
```

> **Note sur l'éditeur** : `nano` est l'éditeur par défaut (installé par le
> script s'il manque). Pour utiliser vim/vi : `EDITOR=vim sudo -E bash install.sh ...`
> (le `-E` est nécessaire car `sudo` strip les variables d'environnement par défaut).

Le script :

1. Détecte l'OS et installe les paquets requis (`curl`, `jq`, `cron`, `dnsutils`, `nano`).
2. Installe Docker si absent (via `get-docker.com`).
3. Configure UFW (OpenSSH + 80/tcp + 443/tcp + 443/udp).
4. Crée un user système `<slug>` (home `/srv/<slug>/`, groupe `docker`).
5. Télécharge la config (`docker-compose.yml`, `Caddyfile`, `crontab`, `.env`).
6. Substitue les valeurs (`INSTANCE_SLUG`, `BACKUPS_PATH`, domaine, email).
7. Ouvre `.env` dans `$EDITOR` (nano par défaut) — tu remplis les `# TODO:`.
8. Vérifie le DNS (`<domain>` doit pointer vers l'IP publique du VPS).
9. Démarre la stack `docker compose up -d`.
10. Lance une ingestion test (vérifie clés AES + SFTP + DuckDB).
11. Affiche un récap (URL, ssh, logs, backups).

Compte ~5-10 min selon la connexion. À la fin :

```bash
curl https://<slug>.electricore.fr/health
# → {"status":"ok","instance":"<slug>",…}
```

Cf. [ADR-0017](adr/0017-layout-deploiement-srv-slug.md) pour le rationale
du layout `/srv/<slug>/` + user dédié.

## Prérequis

- **VPS Linux** Ubuntu 22.04+/24.04+ ou Debian 12+, 2 vCPU et 4 Go RAM minimum,
  40 Go SSD recommandés.
- **Accès root SSH** sur le VPS — **uniquement pour la première installation**.
  Le script copie `~root/.ssh/authorized_keys` vers les users `ops` (admin) et
  `<slug>` (service), puis **désactive le SSH root** (durcissement par défaut,
  ADR-0031). Les opérations suivantes passent par `ssh ops@<vps>` — voir
  [Durcissement du VPS](#durcissement-du-vps).
- **Un nom de domaine** avec un A-record pointant vers l'IP publique du VPS.
- **Ports 80 et 443** ouverts dans le pare-feu cloud (ACME HTTP-01 + HTTPS).
- **Les clés AES Enedis** (clé + IV en hexadécimal) — fournies par Enedis au
  fournisseur.
- **Source SFTP Enedis** :
  - Soit identifiants SFTP distants (mode A).
  - Soit le dépôt Enedis collocé sur le VPS (mode B — recommandé si possible).

## Variables `.env`

Le script ouvre `.env` dans ton éditeur pour que tu remplisses les valeurs
marquées `# TODO:`. Voici la table de référence (cf. aussi
[`deploy/providers/example/secrets.env.example`](../deploy/providers/example/secrets.env.example)
et [`config.env.example`](../deploy/providers/example/config.env.example)) :

**Identité de l'instance**

| Variable | Exemple | Notes |
|---|---|---|
| `INSTANCE_SLUG` | `edn` | Slug court, [a-z0-9-]+, 2-32 chars. Doit matcher `--slug`. Substitué automatiquement par le script. |
| `ELECTRICORE_VERSION` | `1.7.0` | Tag GHCR à déployer. Pin explicite recommandé en prod. |
| `BACKUPS_PATH` | `/srv/edn/backups` | Chemin host des backups (bind-mount). Substitué automatiquement. |

**API**

| Variable | Notes |
|---|---|
| `API_KEY` | Clé principale (≥ 32 chars). Générer : `python -c "import secrets; print(secrets.token_urlsafe(32))"`. |
| `API_KEYS` | Optionnel — clés additionnelles séparées par virgules (notebooks, intégrations tierces). |

**SFTP Enedis**

| Variable | Notes |
|---|---|
| `SFTP__URL` | `sftp://user:pass@host:22/exports` (mode A) ou `file:///var/enedis/` (mode B). |

**Clés AES Enedis** (cf. [rotation](#rotation-des-clés-aes))

| Variable | Notes |
|---|---|
| `AES__TROUSSEAU__<label>__KEY` | Hex 32 (AES-128) ou 64 (AES-256) chars. `<label>` parlant (`aes256_2026`…). |
| `AES__TROUSSEAU__<label>__IV`  | Hex 32 chars. **Optionnel** : présent ⇒ schéma IV-fixe (AES-128) ; **absent ⇒ schéma IV-préfixé** (AES-256, l'IV est en tête de chaque fichier — ADR-0040). |
| (autres labels) | Conserver les anciennes clés dans le trousseau tant que des archives chiffrées avec elles peuvent être (re)téléchargées. |

**Bot Telegram** (optionnel)

| Variable | Notes |
|---|---|
| `TELEGRAM_BOT_TOKEN` | Via [@BotFather](https://t.me/BotFather). Convention : `<slug>_electricore_bot`. |
| `TELEGRAM_ALLOWED_USERS` | IDs Telegram autorisés (séparés par virgules). Via [@userinfobot](https://t.me/userinfobot). |

**Odoo** (optionnel — requis pour `/taxes/*` et `/facturation/check/odoo`)

| Variable | Notes |
|---|---|
| `ODOO_ENV` | `prod` ou `test`. |
| `ODOO_PROD_URL` / `ODOO_PROD_DB` / `ODOO_PROD_USERNAME` / `ODOO_PROD_PASSWORD` | Coordonnées Odoo du fournisseur. |

Le script valide chaque champ après la fermeture de l'éditeur (regex hex pour
AES, longueur API_KEY, URL parseable, slug). Si invalide, il ré-ouvre l'éditeur
avec les erreurs en tête en commentaire.

## Provisionner une nouvelle instance

ElectriCore est déployé en **multi-instance** : un VPS dédié par fournisseur
(EDN, Enargia, …), chacun avec sa propre stack, base DuckDB, clés AES, source
SFTP, bot Telegram, sous-domaine. Cf. [ADR-0015](adr/0015-deploiement-multi-instance.md).

### 1. Choisir le slug

Identifiant court de l'instance, minuscules + chiffres + tirets (`edn`, `enargia`,
`enargia-test`). Une fois choisi, il est difficile à changer (gravé dans DNS,
backups, user système). Choisir soigneusement.

### 2. Provisionner le VPS

Spécifications minimales : 2 vCPU, 4 Go RAM, 40 Go SSD, Ubuntu 22.04+/24.04+ ou
Debian 12+. Ouvrir les ports 80 et 443 au pare-feu cloud.

### 3. Créer le A-record DNS

```
<slug>.electricore.fr.   A   <IP-VPS>
```

Vérifier la propagation avant de lancer le script :

```bash
dig +short <slug>.electricore.fr
```

Pas de wildcard `*.electricore.fr` — chaque VPS gère son propre certificat via
HTTP-01 (cf. [ADR-0015](adr/0015-deploiement-multi-instance.md)).

### 4. Configurer l'accès SSH

Le script s'exécute via `ssh root@<vps>` (cf. [Prérequis](#prérequis)) — ta clé
publique doit donc déjà se trouver dans `~root/.ssh/authorized_keys` du VPS.

1. **Clé locale** — à générer une seule fois si tu n'en as pas
   (`ls ~/.ssh/id_ed25519.pub` pour vérifier) :

   ```bash
   ssh-keygen -t ed25519 -C "ops@example.com"
   ```

2. **Déposer la clé publique sur le VPS** — soit via le panneau du fournisseur
   cloud au moment du provisionning (champ « SSH key »), soit après coup si tu
   disposes d'un accès mot de passe root :

   ```bash
   ssh-copy-id -i ~/.ssh/id_ed25519.pub root@<vps>
   ```

3. **Tester la connexion** :

   ```bash
   ssh root@<vps>
   ```

4. **Alias local (optionnel)** — pour éviter de retaper l'hôte à chaque fois,
   ajouter une entrée dans `~/.ssh/config` sur ta machine :

   ```
   Host electricore-<slug>
       HostName <slug>.electricore.fr   # ou l'IP publique du VPS
       User ops                          # root pour la 1ʳᵉ install ; ops (admin) après durcissement
       IdentityFile ~/.ssh/id_ed25519
       IdentitiesOnly yes
   ```

   Ensuite : `ssh electricore-<slug>` suffit. (`User <slug>` pour un login sur le
   compte de service plutôt que l'admin.)

> Le script propage `~root/.ssh/authorized_keys` vers `ops` (admin) **et** vers
> `<slug>` (service), ce qui permet `ssh ops@<vps>` et `ssh <slug>@<vps>` après
> l'installation. Pour donner à `<slug>` ou à `ops` une clé dédiée plutôt
> qu'hériter de celles de root, passer `--ssh-pubkey "ssh-ed25519 ..."` (service)
> ou `--admin-pubkey "ssh-ed25519 ..."` (admin) au script
> (cf. [Lancer le script](#5-lancer-le-script)).

### 5. Lancer le script

```bash
ssh root@<vps>
curl -fsSL https://raw.githubusercontent.com/Energie-De-Nantes/electricore/main/deploy/install.sh -o install.sh
sudo bash install.sh \
    --slug <slug> \
    --domain <slug>.electricore.fr \
    --email ops@example.com \
    --version 1.7.0
```

> L'éditeur par défaut est `nano`. Pour vim/vi : préfixer par
> `EDITOR=vim sudo -E bash install.sh ...` (le `-E` passe l'env à sudo).

Options notables :
- `--version <tag>` — pin la version GHCR + écrit `ELECTRICORE_VERSION` dans `.env`
  (recommandé en prod, défaut `latest`). Accepte `1.7.0`, `1.8.0rc1`, etc.
- `--ssh-pubkey "ssh-ed25519 ..."` — clé SSH dédiée pour `<slug>`. Sans cette
  option, le script copie `~root/.ssh/authorized_keys`.
- `--env-from <fichier>` — charge un `.env` pré-rempli (utile pour les déploiements
  scriptés, court-circuite l'éditeur).
- `--skip-dns` — saute la vérification DNS (test local).

### 6. Vérifier le bon fonctionnement

Le script affiche un récap en fin d'exécution :

```
✓ Instance edn opérationnelle.

  URL              https://edn.electricore.fr
  /health          curl https://edn.electricore.fr/health
  SSH              ssh edn@edn.electricore.fr
  Logs             sudo -u edn docker compose -f /srv/edn/deploy/docker/docker-compose.yml logs -f
  Backups          /srv/edn/backups/ (rotation 14 snapshots, cron 03:30 Europe/Paris)
  Ingestion nocturne     02:00 Europe/Paris
```

Vérifs manuelles complémentaires :

- [ ] `curl https://<slug>.electricore.fr/health` retourne `{"status":"ok","instance":"<slug>"}`.
- [ ] `/docs` ouvre l'API et le titre contient `<slug>` (ex : `ElectriCore API — EDN`).
- [ ] Certificat Let's Encrypt valide (badge cadenas).
- [ ] Bot Telegram répond à `/start` pour un user dans `TELEGRAM_ALLOWED_USERS`.

### 7. Documenter et archiver

- Noter les coordonnées (slug, IP, contacts fournisseur, registrar DNS).
- Sauvegarder `/srv/<slug>/.env` dans un gestionnaire de secrets (1Password,
  Bitwarden, Vaultwarden auto-hébergé…).
- Ajouter le sous-domaine au monitoring distant (ping `/health` régulier).

## Reconfigurer une instance existante

Toutes les modifications post-install passent par le **mode reconfigure** : tu
relances `install.sh` avec les mêmes `--slug` et `--domain`, le script détecte
l'instance, backup `.env` vers `.env.bak.<timestamp>`, ouvre `$EDITOR`, valide,
restart la stack. **Jamais touche à la DB ni aux backups.**

```bash
# VPS durci (ADR-0031) : login admin via ops — le SSH root est désactivé
ssh ops@<vps>
sudo bash /srv/<slug>/deploy/install.sh --slug <slug> --domain <slug>.electricore.fr
```

> Sur un VPS pas encore durci (ancienne instance, ou install lancée avec
> `--no-harden`), c'est encore `ssh root@<vps>`. Le durcissement est idempotent :
> le reconfigure le (re)pose au passage.

Couvre les cas :

- Rotation des clés AES (cf. [section dédiée](#rotation-des-clés-aes))
- Bump de version (`--version 1.8.0`)
- Changement de domaine (`--domain nouveau.electricore.fr`)
- Ajout/retrait de Telegram, Odoo, etc.

> **Helpers `lib/` toujours frais** : si le `lib/` co-localisé avec `install.sh`
> est absent ou incomplet (un `install.sh` à jour à côté d'un `lib/` figé d'un run
> antérieur — le piège stale-lib, #62), le script télécharge une copie **fraîche
> dans `/tmp`** plutôt que de réutiliser/laisser pourrir un `lib/` à côté du script.
> En mode reconfigure, un `lib/` co-localisé complet est en plus re-téléchargé.
> Override possible via `INSTALL_BASE_URL` pour pinner sur un tag spécifique en dev.

## Secrets-as-code (SOPS + age, ADR-0044)

Depuis [ADR-0044](adr/0044-secrets-as-code-sops-age.md), les secrets ne vivent plus
en clair dans `/srv/<slug>/.env` : ils sont **versionnés chiffrés** (SOPS + age) dans
un **dépôt de déploiement privé**, et **déchiffrés dans le process** par l'entrypoint
de l'image (`electricore-entrypoint`) au démarrage — jamais de fichier clair sur disque.

Le `.env` se scinde en deux :

| Fichier | Contenu | Versionné | Chiffré |
|---|---|---|---|
| `config.env` | config NON secrète + substitutions compose (`INSTANCE_SLUG`, `ELECTRICORE_VERSION`, `BACKUPS_PATH`, `ODOO_ENV`…) | oui | non (clair) |
| `secrets.env` | **uniquement** des credentials (`SFTP__URL`, trousseau `AES__TROUSSEAU__*`, `API_KEY`/`API_KEYS`, `TELEGRAM_BOT_TOKEN`, bloc `ODOO_*`) | oui | **oui** (SOPS + age) |

`docker compose` résout ses substitutions `${...}` **côté hôte avant tout conteneur**
→ il lui faut du clair, d'où `config.env` séparé. Les credentials, eux, sont déchiffrés
**dans** le conteneur par l'entrypoint.

### Deux identités, générées sur la box

Chaque box génère **deux** paires de clés à l'install, **via l'image** (zéro dépendance
hôte : `age` et `ssh-keygen` sont embarqués) :

- une paire **age** (déchiffrement des secrets) — privée dans `/srv/<slug>/age.key` ;
- une paire **SSH** (lecture du dépôt de déploiement) — privée dans `/srv/<slug>/ssh_deploy_key`.

Les **clés privées naissent sur la box et ne la quittent jamais** (`600`, montées RO
dans les conteneurs). L'opérateur enregistre les deux **publiques** : la age pub comme
**destinataire `.sops.yaml`**, la SSH pub comme **deploy key en lecture seule** du dépôt.

La deploy key SSH est de **faible valeur** : elle ne donne accès qu'à du **ciphertext**,
inutile sans la clé age. Toute la sécurité repose sur la clé age, jamais sur le transport.

> **Mort de la box** ⇒ on **forge une identité neuve** (`add-provider.sh` + `sops updatekeys`),
> pas de récupération de clé (forger est déjà bon marché). La clé privée d'une box ne
> transite jamais.

### Onboarding en deux temps

La box ne peut **pas déchiffrer** tant que sa clé age publique n'est pas destinataire
`.sops.yaml`. L'install se fait donc **en deux temps** :

```bash
# 1er temps — la box génère ses identités et IMPRIME les deux clés publiques, puis s'arrête.
ssh ops@<vps>
sudo bash /srv/<slug>/deploy/install.sh \
    --slug <slug> --domain <slug>.electricore.fr \
    --deploy-repo git@github.com:Energie-De-Nantes/electricore-secrets.git
# → AGE_PUBLIC_KEY=age1…   (à enregistrer comme destinataire .sops.yaml)
# → SSH_DEPLOY_PUBKEY=ssh-ed25519 …   (à enregistrer comme deploy key RO du dépôt)
```

Côté **machine admin**, dans le dépôt de déploiement privé :

1. ajouter la **age pub** aux destinataires de `providers/<slug>/.sops.yaml` (cf.
   [`add-provider.sh`](../deploy/add-provider.sh)), puis `sops updatekeys providers/<slug>/secrets.env` ;
2. enregistrer la **SSH pub** comme **deploy key RO** du dépôt (réglages GitHub/GitLab) ;
3. commit + push.

```bash
# 2e temps — reconfigure : la box pull le ciphertext, déchiffre, démarre la stack.
sudo bash /srv/<slug>/deploy/install.sh \
    --slug <slug> --domain <slug>.electricore.fr \
    --deploy-repo git@github.com:Energie-De-Nantes/electricore-secrets.git
```

À ce 2e temps, le script : pull `providers/<slug>/{config.env,secrets.env}` via la
deploy key, **valide le split** (config claire sans secret ; secrets déchiffrables et
valides), puis démarre la stack. Si la box ne déchiffre pas encore (age pub pas encore
destinataire), il **réaffiche** les pubs et **échoue proprement** sans rien démarrer.

### Runbook de migration EDN (bascule forcée)

Migration de l'instance vivante (EDN) du `.env` en clair vers secrets-as-code, dans une
**fenêtre planifiée** (pas de double chemin maintenu, ADR-0044 §8). L'unique manipulation
de secrets en clair se fait **sur la machine admin** et **n'est jamais committée**.

1. **Box** — générer les identités (1er temps ci-dessus) avec `--deploy-repo`. Noter les
   deux pubs. (La box garde son `.env` vivant intact pour l'instant.)
2. **Machine admin** — scinder le `.env` vivant récupéré de la box en deux :
   - `config.env` ← `INSTANCE_SLUG`, `ELECTRICORE_VERSION`, `BACKUPS_PATH`, config non-secrète ;
   - `secrets.env.clair` ← `SFTP__URL`, `AES__TROUSSEAU__*`, `API_KEY`/`API_KEYS`,
     `TELEGRAM_BOT_TOKEN`, `ODOO_*`.
3. **Machine admin** — chiffrer **sur place**, jamais committer le clair :
   ```bash
   sops encrypt --input-type dotenv --output-type dotenv \
       secrets.env.clair > providers/<slug>/secrets.env
   shred -u secrets.env.clair    # détruire le clair
   ```
   (`.sops.yaml` du provider porte déjà l'admin + la box comme destinataires.)
4. **Machine admin** — committer `providers/<slug>/{config.env,secrets.env}` + pousser ;
   enregistrer la SSH pub comme deploy key RO.
5. **Box** — `reconfigure` (2e temps) : pull + déchiffre + démarre.
6. **Vérifier** : `curl https://<slug>.electricore.fr/health` OK, une ingestion test OK.
7. **Puis seulement** : supprimer le `.env` en clair de la box (`shred -u /srv/<slug>/.env`).
   La bascule est **forcée** : tant que `.env` traîne, on n'a pas fini.

## Durcissement du VPS

Depuis [ADR-0031](adr/0031-durcissement-ssh-vps-utilisateur-ops.md), `install.sh`
durcit le VPS **par défaut** (étape 6, juste après la création du user de service).
Trois rôles distincts par construction :

| Rôle | Compte | Accès | Usage |
|---|---|---|---|
| **admin** | `ops` | SSH par clé, **sudo NOPASSWD** | login humain, lance `install.sh` (install + reconfigure) |
| **service** | `<slug>` | SSH par clé, groupe `docker`, **pas de sudo** | possède `/srv/<slug>/`, fait tourner la stack (ADR-0017) |
| ~~root~~ | `root` | **SSH désactivé**, local + sudo depuis `ops` | — |

Pour sauter entièrement le durcissement : `--no-harden`. Pour rétro-durcir un VPS
déjà déployé sans reconfigure complet, voir le script autonome
[`deploy/harden.sh`](#rétro-durcir-un-vps-existant).

### SSH (root-off, clé uniquement)

Le durcissement pose un drop-in `/etc/ssh/sshd_config.d/50-electricore-harden.conf` :

- `PermitRootLogin no` — plus de login root en SSH.
- `PasswordAuthentication no` + `KbdInteractiveAuthentication no` — clé uniquement.
- `PubkeyAuthentication yes`, `X11Forwarding no`, `MaxAuthTries 3`.

La config est validée par `sshd -t` **avant** un `systemctl reload ssh` (jamais
`restart`) : la session root en cours **survit**, seuls les nouveaux logins
root/mot-de-passe échouent. La connexion suivante se fait en `ops`.

**Garde-fou anti-verrouillage** : la bascule root-off est refusée tant que `ops`
n'a pas de `authorized_keys` exploitable — impossible de se verrouiller dehors.
La clé de `ops` est amorcée depuis `~root/.ssh/authorized_keys` (override
`--admin-pubkey "ssh-ed25519 …"`).

> ⚠️ Après le premier durcissement, mets à jour ton `~/.ssh/config` :
> `User root` → `User ops`. Ne le fais pas avant — `ops` n'existe pas tant que
> l'install n'a pas tourné.

### fail2ban (force brute SSH)

Le durcissement installe `fail2ban` et active le jail `sshd` via
`/etc/fail2ban/jail.d/electricore.conf`, avec **`backend = systemd`** (journald).
C'est le piège Debian/Ubuntu moderne : le défaut historique lit
`/var/log/auth.log`, vide sur ces images — il faut lire le journal systemd.
Paramètres : `maxretry = 3`, `findtime = 10m`, `bantime = 1h`.

```bash
sudo fail2ban-client status sshd   # IP bannies, compteurs
```

fail2ban est marginal une fois le mot de passe SSH coupé (plus rien à brute-forcer) ;
il sert surtout à réduire le bruit des scanners dans les logs.

### Mises à jour automatiques (unattended-upgrades)

Le durcissement installe `unattended-upgrades` et active les **correctifs de
sécurité** automatiques (`/etc/apt/apt.conf.d/20auto-upgrades`), avec un
**redémarrage automatique à 04:30** Europe/Paris
(`/etc/apt/apt.conf.d/52electricore-unattended`,
`Automatic-Reboot "true"` + `Automatic-Reboot-Time "04:30"`).

Pourquoi 04:30 et pourquoi rebooter :

- **Après le backup de 03:30** (cf. [Sauvegarde](#sauvegarde-et-restauration)) :
  on ne reboote jamais au milieu d'un snapshot.
- **Sans reboot, les patchs kernel/openssl restent dormants.** Le redémarrage
  nocturne les applique réellement.
- **Risque faible** : la stack est `restart: unless-stopped` et Docker démarre au
  boot — elle revient seule en ~1 min. Micro-coupure non planifiée seulement les
  nuits où un reboot est en attente (VPS sans HA, ADR-0011).

### Rétro-durcir un VPS existant

Pour durcir une instance **déjà déployée** sans relancer un reconfigure complet,
le script autonome [`deploy/harden.sh`](../deploy/harden.sh) source la même
logique (`harden_vps`) et l'applique seule. **Aucune hypothèse de layout** : la
clé de `ops` est amorcée depuis `~root/.ssh/authorized_keys` quel que soit
l'emplacement de la stack (`/srv/<slug>/` comme l'ancien `/opt/electricore/`).

```bash
# Instance au layout courant (arbre deploy/ présent) :
ssh root@<vps>          # ou ssh ops@<vps> si déjà partiellement durci
sudo bash /srv/<slug>/deploy/harden.sh

# Ancien layout /opt/electricore/ (root-run), ou box sans notre arbre deploy/ :
curl -fsSL https://raw.githubusercontent.com/Energie-De-Nantes/electricore/main/deploy/harden.sh -o harden.sh
sudo bash harden.sh
```

Le même **garde-fou anti-verrouillage** s'applique (refus de couper root SSH si
`ops` n'a pas de clé). Options : `--admin-pubkey "ssh-ed25519 …"`, et les `--no-*`
(`--no-sshd`, `--no-fail2ban`, `--no-unattended-upgrades`) pour durcir par morceaux.

### Réverser le durcissement (désinstallation)

Pour revenir en arrière — repasser sur un accès root classique, ou annuler en cas
de souci — [`deploy/unharden.sh`](../deploy/unharden.sh) défait ce que le
durcissement a posé :

- retire le drop-in sshd → **SSH root rétabli** (défaut de l'image) ;
- retire la jail fail2ban (laisse le paquet installé) ;
- retire la conf unattended-upgrades (auto-reboot 04:30 désactivé).

Le user admin `ops` est **conservé** par défaut ; `--purge-ops` le supprime aussi
(sudoers + compte + home).

```bash
# Instance au layout courant :
ssh ops@<vps>          # ou root si tu y as encore accès
sudo bash /srv/<slug>/deploy/unharden.sh               # garde ops
sudo bash /srv/<slug>/deploy/unharden.sh --purge-ops   # supprime aussi ops

# Standalone (legacy /opt, ou box sans notre arbre deploy/) :
curl -fsSL https://raw.githubusercontent.com/Energie-De-Nantes/electricore/main/deploy/unharden.sh -o unharden.sh
sudo bash unharden.sh
```

La réversion rétablit le SSH root **en premier**, donc même un `--purge-ops` ne
peut pas te verrouiller dehors. Pense ensuite à remettre `User root` dans ton
`~/.ssh/config` si tu l'avais basculé sur `ops`.

> La réversion retire entièrement `/etc/apt/apt.conf.d/20auto-upgrades`. Si ton
> image l'avait déjà avant durcissement, les mises à jour auto repassent au défaut
> du paquet (généralement inactif sans ce fichier).

## Accès distant depuis un notebook Python

Depuis la v1.5, l'API expose les résultats des pipelines opérationnels en flux
**Arrow IPC**, consommables sans rapatrier la base DuckDB. Un notebook local
peut piloter le calcul côté serveur tout en restant maître de la chaîne
d'écriture vers Odoo (cf. [ADR-0012](adr/0012-api-read-only-odoo.md)).

### Endpoints Arrow IPC

| Endpoint | Sortie | Paramètre |
|---|---|---|
| `GET /facturation/arrow` | `lignes_facture_rapprochees` (rapprochement Odoo ↔ Enedis du mois) | `mois=YYYY-MM-DD` (défaut : dernier mois) |
| `GET /taxes/accise/detail.arrow` | Détail Accise TICFE | `trimestre=YYYY-TX` |
| `GET /taxes/cta/arrow` | Détail CTA mensuel | idem |

Les endpoints `xlsx` existants restent inchangés.

### Client Python

```bash
# Client Arrow (DataFrames polars) — paquet séparé electricore-client, extra [arrow]
pip install "electricore-client[arrow]"
```

```python
from electricore_client.arrow import ElectricoreArrowClient

client = ElectricoreArrowClient(
    url="https://<slug>.electricore.fr",
    api_key="votre_cle_api",
)

df = client.facturation()                          # mois=None → dernier mois
df_accise = client.accise(trimestre="2025-T1")
df_cta = client.cta(trimestre="2025-T1")
```

### TLS local (cert auto-signé)

```python
import httpx
from electricore_client.arrow import ElectricoreArrowClient

http = httpx.Client(verify=False, timeout=httpx.Timeout(30.0, read=120.0))
client = ElectricoreArrowClient(url="https://electricore.localhost", api_key="…", http_client=http)
```

## Mode SFTP distant vs fichiers collocés

### Mode A — SFTP distant (par défaut)

Le scheduler d'ingestion se connecte au serveur SFTP Enedis pour télécharger les fichiers
chiffrés, puis les déchiffre et les ingère.

`.env` :

```
SFTP__URL=sftp://user:pass@host.enedis.fr:22/exports
```

Rien à changer dans `docker-compose.yml`.

### Mode B — Fichiers collocés sur le VPS

Si le serveur SFTP Enedis tourne sur **le même VPS**, on évite un téléchargement
inutile et un transfert réseau supplémentaire de données sensibles : l'API lit
directement les fichiers chiffrés depuis le système de fichiers lors de chaque
déclenchement d'ingestion.

`.env` :

```
SFTP__URL=file:///var/enedis/
```

Dans `/srv/<slug>/deploy/docker/docker-compose.yml`, décommenter le bind-mount
du service **`api`** :

```yaml
services:
  api:
    volumes:
      - duckdb_data:/data
      - /var/enedis:/var/enedis:ro     # ← cette ligne
```

> **⚠️ Important** : les fichiers Enedis restent chiffrés en AES sur disque
> (même en mode collocé). Les clés `AES__*` sont toujours obligatoires.
>
> Le service `ingestion-scheduler` n'a **pas** besoin de ce bind-mount — il appelle
> `POST /ingestion/run` via HTTP, c'est `api` qui exécute le pipeline.

Puis :

```bash
sudo -u <slug> docker compose -f /srv/<slug>/deploy/docker/docker-compose.yml \
    --env-file /srv/<slug>/.env up -d api
```

## Rotation des clés AES (trousseau N-clés, ADR-0037)

Enedis rote périodiquement ses clés (et en change la longueur : AES-128 → AES-256).
Le **trousseau** porte un nombre arbitraire de clés labellisées ; la bonne est
sélectionnée par essai (aucune date, aucun protocole).

### Procédure (secrets-as-code, ADR-0044)

Depuis [ADR-0044](adr/0044-secrets-as-code-sops-age.md), le trousseau AES vit dans le
`secrets.env` **chiffré** du provider — plus dans un `.env` édité sur la box. La rotation
se fait donc **sur la machine admin**, par édition chiffrée in-place :

1. Recevoir la nouvelle clé Enedis.
2. Sur la machine admin (dépôt de déploiement privé), éditer **in-place** le secrets
   chiffré (SOPS ouvre l'éditeur sur le clair, re-chiffre à la fermeture) :

   ```bash
   sops providers/<slug>/secrets.env
   ```

   **Ajouter** la nouvelle clé au trousseau sous un nouveau label, sans retirer les anciennes :

   ```
   # AES-256 (depuis juin 2026) : Enedis ne fournit QUE la clé, sans IV → pas de __IV.
   # Schéma IV-préfixé (ADR-0040) : l'IV est en tête de chaque fichier. NE PAS mettre d'__IV.
   AES__TROUSSEAU__aes256_2026__KEY=nouvelle_cle_hex
   # AES-128 (archives chiffrées avant juin 2026) : clé + IV fournis (schéma IV-fixe).
   AES__TROUSSEAU__aes128_2024__KEY=ancienne_cle_hex
   AES__TROUSSEAU__aes128_2024__IV=ancien_iv_hex
   ```

3. Commit + push le `secrets.env` (re-chiffré).
4. Sur la box, `reconfigure` (pull + déchiffre + restart) :

   ```bash
   ssh ops@<vps>
   sudo bash /srv/<slug>/deploy/install.sh --slug <slug> --domain <slug>.electricore.fr \
       --deploy-repo git@github.com:Energie-De-Nantes/electricore-secrets.git
   ```

Les logs `[<label>]` indiquent quelle clé a déchiffré quel fichier. Si un flux a des
fichiers mais **0** déchiffrement réussi, le job passe à `failed` et le bot alerte
(escalade per-flux) : signe qu'une clé manque encore au trousseau.

Retirer une vieille clé seulement quand plus aucune archive chiffrée avec elle
n'est (re)téléchargeable depuis le SFTP.

> **`sops updatekeys` ≠ rotation de secret** (ADR-0044) : `updatekeys` ne rote que
> l'**enveloppe** (re-wrap vers le jeu de destinataires, ex. ajout d'une box via
> [`add-provider.sh`](../deploy/add-provider.sh)). Si une **clé AES a pu fuiter**, il
> faut la changer **à la source** (nouvelle clé Enedis) — `updatekeys` ne la protège pas.

## Sauvegarde et restauration

### Sauvegarde automatique

Le scheduler crée un snapshot complet chaque nuit à 03:30 (Europe/Paris) — voir
[`deploy/docker/crontab.example`](../deploy/docker/crontab.example) et
[`deploy/docker/backup_duckdb.sh`](../deploy/docker/backup_duckdb.sh).

- Format : `EXPORT DATABASE` (SQL + parquet), compressé en `tar.gz`.
- Emplacement : `/srv/<slug>/backups/` (bind-mount, lisible directement côté host).
- Nommage : `snapshot_<slug>_<TS>.tar.gz` (préfixe par slug, cf. [ADR-0015](adr/0015-deploiement-multi-instance.md)).
- Rétention : 14 snapshots les plus récents (variable `RETAIN_DAYS`).

```bash
ssh <slug>@<vps>
ls -lh /srv/<slug>/backups/
```

### Copie offsite (recommandée)

Le snapshot reste sur le VPS. Ajouter une copie hors-site via
[rclone](https://rclone.org/) (à configurer côté user `<slug>`) :

```bash
# Dans la crontab du user <slug>
45 3 * * * rclone copy /srv/<slug>/backups remote:electricore-backups --max-age 24h
```

### Restauration

```bash
ssh <slug>@<vps>
cd /srv/<slug>/

# 1. Choisir un snapshot
ls -lh backups/

# 2. Décompresser
tar -xzf backups/snapshot_<slug>_20260601T013000Z.tar.gz -C /tmp/

# 3. Stopper la stack
sudo -u <slug> docker compose -f deploy/docker/docker-compose.yml --env-file .env down

# 4. Restaurer dans une nouvelle base
duckdb /tmp/restored.duckdb "IMPORT DATABASE '/tmp/snapshot_<slug>_20260601T013000Z/'"

# 5. Remplacer la base courante (DuckDB est dans un volume Docker nommé)
docker run --rm -v <slug>_duckdb_data:/data -v /tmp:/host alpine \
    cp /host/restored.duckdb /data/flux_enedis_pipeline.duckdb

# 6. Redémarrer
sudo -u <slug> docker compose -f deploy/docker/docker-compose.yml --env-file .env up -d
```

## Mise à jour de version

### Mise à jour standard

Via le mode reconfigure du script, avec `--version` ou en éditant
`ELECTRICORE_VERSION` dans `.env` :

```bash
sudo bash /srv/<slug>/deploy/install.sh \
    --slug <slug> --domain <slug>.electricore.fr --version 1.8.0
```

Le script :

1. Détecte l'instance existante (mode reconfigure).
2. Re-télécharge `docker-compose.yml`, `Caddyfile`, `crontab` au tag demandé
   (utile si la stack a évolué).
3. Backup `.env`, écrit `ELECTRICORE_VERSION=<--version>` automatiquement, ouvre
   `$EDITOR` (nano par défaut) pour les ajustements éventuels.
4. `docker compose pull` puis `up -d` (recrée les conteneurs avec la nouvelle image).
5. ingestion test.

### Rollback

```bash
sudo bash /srv/<slug>/deploy/install.sh \
    --slug <slug> --domain <slug>.electricore.fr --version 1.7.0
```

> ⚠️ Une rétrogradation **majeure** peut nécessiter une restauration de la base si
> le schéma a évolué — vérifier le CHANGELOG.

## Fenêtre d'ingestion et concurrence DuckDB

DuckDB autorise plusieurs lecteurs en parallèle, mais le writer (ici le scheduler
d'ingestion) prend un verrou exclusif sur le fichier pendant l'écriture. Concrètement :

- L'API reste accessible pendant la lecture (`SELECT`) tant que l'ingestion n'écrit pas.
- Si une requête API arrive **pendant** un `pipeline.run()`, l'API retry jusqu'à
  3 fois (1 s d'écart) avant de renvoyer une erreur. La plupart des écritures DLT
  sont courtes (checkpoints).
- Si une requête tombe pile sur un long checkpoint, le client reçoit un `500` —
  relancer après 30 s. C'est pour cela qu'on planifie l'ingestion à 02:00.

Pour ajuster la fenêtre d'ingestion, éditer l'horaire dans
`/srv/<slug>/deploy/docker/crontab` (`0 2 * * *` = 02:00 Europe/Paris, le TZ du
conteneur est fixé via `docker-compose.yml`).

## Migration depuis l'ancien layout `/opt/electricore/`

Les instances déployées avant l'introduction du script (ADR-0017) tournent dans
`/opt/electricore/` en root. La migration vers le nouveau layout
`/srv/<slug>/` + user dédié est **opportuniste**, pas urgente : l'ancien layout
continue à tourner. Quand tu veux migrer une instance existante :

### Procédure

```bash
ssh root@<vps>
SLUG=edn   # à adapter

# 1. Stopper l'ancienne stack
cd /opt/electricore/deploy/docker
docker compose down

# 2. Créer le user système
useradd --create-home --home-dir /srv/${SLUG} --shell /bin/bash ${SLUG}
usermod -aG docker ${SLUG}
install -d -m 700 -o ${SLUG} -g ${SLUG} /srv/${SLUG}/.ssh
cp /root/.ssh/authorized_keys /srv/${SLUG}/.ssh/authorized_keys
chown ${SLUG}:${SLUG} /srv/${SLUG}/.ssh/authorized_keys
chmod 600 /srv/${SLUG}/.ssh/authorized_keys

# 3. Déplacer les fichiers
mv /opt/electricore/.env /srv/${SLUG}/.env
mv /opt/electricore/deploy /srv/${SLUG}/deploy

# 4. Ajouter les nouvelles variables (INSTANCE_SLUG, BACKUPS_PATH)
cat >> /srv/${SLUG}/.env <<EOF
INSTANCE_SLUG=${SLUG}
BACKUPS_PATH=/srv/${SLUG}/backups
EOF

# 5. Migrer les backups du volume Docker vers le bind-mount
install -d -o 1000 -g 1000 /srv/${SLUG}/backups
docker run --rm \
    -v electricore_duckdb_backups:/old \
    -v /srv/${SLUG}/backups:/new \
    alpine sh -c "cp -a /old/. /new/"
docker volume rm electricore_duckdb_backups

# 6. Mettre <slug> dans le groupe docker (gid 1000 = uid conteneur)
DOCKER_UID=1000
getent group ${DOCKER_UID} >/dev/null || groupadd -g ${DOCKER_UID} electricore-uid
usermod -aG ${DOCKER_UID} ${SLUG}

# 7. Récupérer la dernière version du compose (avec bind-mount BACKUPS_PATH)
cd /srv/${SLUG}/deploy/docker
curl -fsSL -o docker-compose.yml \
    https://raw.githubusercontent.com/Energie-De-Nantes/electricore/main/deploy/docker/docker-compose.yml

# 8. chown final
chown -R ${SLUG}:${SLUG} /srv/${SLUG}

# 9. Démarrer la nouvelle stack
sudo -u ${SLUG} -- bash -c \
    "cd /srv/${SLUG}/deploy/docker && docker compose --env-file ../../.env up -d"

# 10. Vérifier
curl https://<slug>.electricore.fr/health
# {"status":"ok","instance":"<slug>",…}
```

Une fois la migration validée et observée stable pendant quelques jours,
supprimer `/opt/electricore/` :

```bash
rm -rf /opt/electricore
```

> La migration ci-dessus ne durcit pas le VPS. Pour appliquer le durcissement
> SSH (ADR-0031) sur la box migrée — ou directement sur l'ancien layout avant
> migration — lancer le script autonome
> [`deploy/harden.sh`](#rétro-durcir-un-vps-existant).

## Annexe : déploiement manuel pas-à-pas

Cette section décrit ce que fait `install.sh` étape par étape. Utile pour
comprendre, dépanner, ou installer sans le script.

### Layout cible

```
/srv/<slug>/
├── .env                           ← secrets (source unique)
├── backups/                       ← bind-mount des snapshots DuckDB
└── deploy/docker/
    ├── docker-compose.yml
    ├── Caddyfile
    ├── crontab
    └── backup_duckdb.sh
```

### 1. Provisionning OS

```bash
apt-get update
apt-get install -y curl jq cron dnsutils

# Docker via get-docker.com
curl -fsSL https://get.docker.com | sh

# UFW
apt-get install -y ufw
ufw allow OpenSSH
ufw allow 80/tcp
ufw allow 443/tcp
ufw allow 443/udp
ufw --force enable
```

### 2. User système

```bash
SLUG=edn
useradd --create-home --home-dir /srv/${SLUG} --shell /bin/bash ${SLUG}
usermod -aG docker ${SLUG}
install -d -m 700 -o ${SLUG} -g ${SLUG} /srv/${SLUG}/.ssh
cp /root/.ssh/authorized_keys /srv/${SLUG}/.ssh/authorized_keys
chown ${SLUG}:${SLUG} /srv/${SLUG}/.ssh/authorized_keys
chmod 600 /srv/${SLUG}/.ssh/authorized_keys
```

### 3. Téléchargement de la config

```bash
TAG=1.7.0
BASE=https://raw.githubusercontent.com/Energie-De-Nantes/electricore/${TAG}/deploy/docker
DEST=/srv/${SLUG}/deploy/docker
install -d ${DEST}
for f in docker-compose.yml Caddyfile.example crontab.example backup_duckdb.sh; do
    curl -fsSL -o "${DEST}/${f}" "${BASE}/${f}"
done
mv ${DEST}/Caddyfile.example ${DEST}/Caddyfile
mv ${DEST}/crontab.example   ${DEST}/crontab
chmod +x ${DEST}/backup_duckdb.sh
```

### 4. Configuration `.env`

Éditer `/srv/${SLUG}/.env` :

- Remplacer `INSTANCE_SLUG=` par `INSTANCE_SLUG=${SLUG}`.
- Remplacer `BACKUPS_PATH=...` par `BACKUPS_PATH=/srv/${SLUG}/backups`.
- Remplir tous les `# TODO: à remplir` (cf. [Variables `.env`](#variables-env)).

### 5. Configuration Caddy

Éditer `${DEST}/Caddyfile` :

- Remplacer `electricore.exemple.fr` par `${SLUG}.electricore.fr`.
- Remplacer `votre-email@example.com` par un email valide.

### 6. Ownership

```bash
install -d -o 1000 -g 1000 /srv/${SLUG}/backups
chown -R ${SLUG}:${SLUG} /srv/${SLUG}
```

### 7. Démarrer

```bash
sudo -u ${SLUG} -- bash -c \
    "cd /srv/${SLUG}/deploy/docker && docker compose --env-file ../../.env up -d"
```

### 8. ingestion test

```bash
API_KEY=$(grep ^API_KEY= /srv/${SLUG}/.env | cut -d= -f2)
sudo -u ${SLUG} -- bash -c \
    "cd /srv/${SLUG}/deploy/docker && docker compose exec -T ingestion-scheduler \
     curl -X POST -H 'X-API-Key:${API_KEY}' -H 'Content-Type: application/json' \
     -d '{\"mode\":\"test\"}' http://api:8001/ingestion/run"
```

## Dépannage

### `/health` retourne `database.accessible: false`

- Si `error` mentionne `Fichier DuckDB introuvable` : aucune ingestion n'a encore été
  lancée. Lancer manuellement via la commande ingestion test ci-dessus.
- Si `error` mentionne `Could not set lock` : l'ingestion est en cours d'écriture.
  Réessayer dans quelques secondes.
- Sinon : `sudo -u <slug> docker compose -f /srv/<slug>/deploy/docker/docker-compose.yml logs api`.

### `ingestion-scheduler` redémarre en boucle

```bash
sudo -u <slug> docker compose -f /srv/<slug>/deploy/docker/docker-compose.yml logs ingestion-scheduler
```

Causes typiques :

- `API_KEY` absente ou ne correspond pas à `API_KEYS`.
- `api` n'est pas encore *healthy* (le scheduler attend `condition: service_healthy`).
- Erreur de syntaxe dans `crontab`.

### Certificat Caddy bloqué

```bash
sudo -u <slug> docker compose -f /srv/<slug>/deploy/docker/docker-compose.yml logs caddy | grep -i acme
```

- Vérifier que les ports 80 et 443 sont exposés et atteignables depuis l'extérieur.
- Vérifier que le domaine résout vers l'IP du VPS (`dig +short <domain>`).
- Pendant les tests, activer l'`acme_ca` staging dans `Caddyfile` pour éviter
  les rate-limits Let's Encrypt.

### L'ingestion échoue avec `Échec déchiffrement avec X clé(s)`

Aucune clé du trousseau n'a déchiffré ce flux (clé manquante ou fichier corrompu).
Comparer les `AES__TROUSSEAU__<label>__*` dans `.env` avec ce qu'Enedis a fourni ;
en période de rotation, vérifier que **l'ancienne clé** est toujours dans le trousseau
(elle reste nécessaire pour les archives historiques). Relancer le script en mode
reconfigure pour ré-éditer `.env` proprement.

### Le bot Telegram ne répond pas

- Vérifier que `bot.running: true` dans `/health`.
- Vérifier que votre ID Telegram est listé dans `TELEGRAM_ALLOWED_USERS`.
- Logs : `sudo -u <slug> docker compose -f /srv/<slug>/deploy/docker/docker-compose.yml logs api | grep -i telegram`.

### Erreur de permissions sur `/srv/<slug>/backups/`

Le bind-mount est owned uid 1000 (user `electricore` du conteneur). Pour que
`<slug>` puisse `ls /srv/<slug>/backups/`, il doit être dans le même groupe :

```bash
usermod -aG 1000 <slug>
# se reconnecter en ssh pour que le groupe s'applique
```

### Le script `install.sh` s'arrête sur "OS non supporté"

Seuls Ubuntu 22.04+/24.04+ et Debian 12+ sont supportés. Pour une autre distro,
suivre l'[annexe déploiement manuel](#annexe--déploiement-manuel-pas-à-pas) en
adaptant les commandes apt.
