# Durcissement SSH du VPS : utilisateur admin `ops`, root sans SSH

## Contexte

[ADR-0011](0011-deploiement-vps-docker.md) pose la stack Docker Compose sur VPS unique ; [ADR-0017](0017-layout-deploiement-srv-slug.md) place chaque instance dans `/srv/<slug>/`, portée par un user système `<slug>` (groupe `docker`, **pas de sudo**), joignable en `ssh <slug>@<vps>`. Tout le flux d'installation et de reconfiguration de [docs/deploiement.md](../deploiement.md) repose en revanche sur **`root` en SSH** (`ssh root@<vps>` puis `sudo bash install.sh`), et le script copie `~root/.ssh/authorized_keys` vers le user `<slug>`.

Le VPS expose 22/80/443. À l'état actuel, rien ne durcit l'accès SSH : `root` accepte les connexions, l'authentification par mot de passe dépend du défaut (souvent activé) de l'image cloud, aucun bannissement de force brute (`fail2ban`), aucune application automatique des correctifs de sécurité. On veut un VPS **durci par défaut** dès l'installation, sans transformer l'opération quotidienne en parcours du combattant.

La tension est réelle : la règle de durcissement canonique est « désactiver le login root », mais elle entre en collision frontale avec le flux reconfigure, le contrôle `EUID == 0` de `install.sh`, et l'entrée `~/.ssh/config` de l'opérateur (`User root`). Le choix est gravé dans l'accès SSH lui-même — se tromper, c'est se verrouiller dehors à distance. Difficile à revenir.

## Décision

On applique la règle canonique (`PermitRootLogin no`) **en introduisant un utilisateur admin dédié** plutôt qu'en chargeant le user de service. Trois rôles, distincts par construction :

| Rôle | Compte | Privilèges | Usage |
|---|---|---|---|
| **admin** | `ops` | sudo **NOPASSWD**, SSH par clé uniquement | login humain ; lance `sudo bash install.sh` (install + reconfigure) |
| **service** | `<slug>` | groupe `docker`, **pas de sudo** | possède `/srv/<slug>/`, fait tourner la stack (inchangé, ADR-0017) |
| ~~root~~ | `root` | local seulement | **SSH désactivé** ; atteignable via `sudo` depuis `ops` |

- **`ops`** : créé idempotemment (une fois par VPS), `sudo` sans mot de passe via `/etc/sudoers.d/ops`. Sa clé est amorcée en copiant `~root/.ssh/authorized_keys` au moment du durcissement (override `--admin-pubkey`). NOPASSWD est **imposé** par le choix « clé uniquement » : `ops` n'a pas de mot de passe, donc un `sudo` interactif serait inutilisable.
- **sshd** : drop-in `/etc/ssh/sshd_config.d/50-electricore-harden.conf` — `PermitRootLogin no`, `PasswordAuthentication no`, `KbdInteractiveAuthentication no`, `PubkeyAuthentication yes`, `X11Forwarding no`, `MaxAuthTries 3`. Pas d'`AllowUsers`. Validé par `sshd -t` **avant** `systemctl reload ssh` (jamais `restart`).
- **fail2ban** : jail `sshd` via `/etc/fail2ban/jail.d/electricore.conf` avec `backend=systemd` (le piège Debian/Ubuntu moderne : journal, pas `/var/log/auth.log`).
- **unattended-upgrades** : correctifs de sécurité activés ; `Automatic-Reboot "true"`, `Automatic-Reboot-Time "04:30"` (après le backup de 03:30 ; la stack revient seule grâce à `restart: unless-stopped`).
- **Livraison** : logique dans `deploy/lib/harden.sh` (`harden_vps()` orchestrant `ensure_admin_user` → `seed_admin_key` → `grant_nopasswd_sudo` → **garde-fou : clé de `ops` non vide** → `harden_sshd` → `setup_fail2ban` → `setup_unattended_upgrades`). Câblé dans `install.sh` comme étape **active par défaut**, placée *après* la confirmation user+clé, avec un échappatoire `--no-harden`. Wrapper autonome `deploy/harden.sh` pour durcir un VPS déjà déployé (y compris l'ancien layout `/opt/electricore/`).

**Garde-fou anti-verrouillage (ordre impératif)** : `install.sh` crée `ops` + sudo + clé, **vérifie que `ops` a un `authorized_keys` non vide**, et seulement ensuite bascule `PermitRootLogin no` / `PasswordAuthentication no`. La session root en cours survit (sshd ne tue pas les sessions ouvertes) ; les nouveaux logins root échouent. La prochaine connexion se fait en `ops`.

## Raison

1. **Moindre privilège sans renoncer au confort.** Désactiver root est le geste canonique ; le user dédié `ops` évite l'alternative coûteuse (donner sudo au user de service `<slug>`, ce qui mélangerait « qui fait tourner la stack » et « qui administre la machine »). On garde la séparation de rôles d'ADR-0017 en la rendant explicite.
2. **NOPASSWD découle du modèle clé-uniquement.** Avec `PasswordAuthentication no`, `ops` n'a aucune raison d'avoir un mot de passe ; en exiger un pour `sudo` casserait les reconfigure scriptés (`--env-from`) et les tests e2e. La frontière de sécurité est déjà la clé SSH — un porteur de clé peut de toute façon piloter toute la stack en `<slug>`.
3. **Durci par défaut.** L'intérêt d'un installeur unattended est qu'il fasse le bon geste sans qu'on y pense ; le durcissement opt-in serait oublié. Le garde-fou rend l'activation par défaut sûre.
4. **Auto-reboot pour que les correctifs s'appliquent.** Installer les patches kernel/openssl sans jamais redémarrer les laisse dormants. Le `restart: unless-stopped` de la stack rend le redémarrage nocturne quasi indolore.

## Alternatives écartées

- **Root en clé uniquement (`PermitRootLogin prohibit-password`).** Le « 90 % » pragmatique : root reste mais sans mot de passe, l'entrée `~/.ssh/config` et le flux reconfigure survivent sans rien changer. Écarté au profit du geste canonique complet, le user `ops` neutralisant le coût.
- **Désactiver root + donner sudo au user `<slug>`.** Évite un 3ᵉ compte, mais conflate service et administration : le compte qui fait tourner la stack deviendrait aussi le compte d'admin de la machine. Mauvaise séparation de rôles.
- **`sudo` avec mot de passe pour `ops`.** Vrai second facteur pour l'escalade *si* le mot de passe est stocké séparément de la clé. Écarté : casse les déploiements scriptés/unattended, et il faudrait gérer ce mot de passe dans un coffre.
- **`AllowUsers ops <slug>`.** Durcissement supplémentaire, mais ajoute un piège (ajouter un user plus tard sans le whitelister = verrouillage) et oblige le script autonome à connaître `<slug>`. Marginal une fois root coupé et mots de passe désactivés.
- **Durcissement opt-in (`--harden`).** Zéro risque de surprise, mais les instances partent non durcies par défaut — ça vide l'intérêt.
- **fail2ban sur backend `/var/log/auth.log`.** Le défaut historique ne lit rien sur les Debian/Ubuntu récents (journald). `backend=systemd` est requis.

## Conséquences

- **[deploy/lib/harden.sh](../../deploy/lib/harden.sh) créé** + câblage d'une étape dans [deploy/install.sh](../../deploy/install.sh) (active par défaut, après l'étape user/clé) ; [deploy/lib/cli.sh](../../deploy/lib/cli.sh) gagne `--no-harden` (+ `--admin-pubkey` optionnel).
- **[deploy/harden.sh](../../deploy/harden.sh) créé** : wrapper autonome pour rétro-durcir un VPS existant ou un ancien layout `/opt/electricore/`.
- **Récap `install.sh`** : affiche `ssh ops@<domain>` (admin) **et** `ssh <slug>@<domain>` (service).
- **[docs/deploiement.md](../deploiement.md)** : Prérequis (root SSH requis *seulement pour la première install*), flux reconfigure (`ssh root` → `ssh ops`), l'étape « Configurer l'accès SSH », la section migration, + une nouvelle section « Durcissement ».
- **`~/.ssh/config` de l'opérateur** : `User root` → `User ops` une fois le VPS durci (pas avant — `ops` n'existe pas encore sur la machine).
- **Tests** : unitaires `deploy/tests/unit.sh` sur les parties pures (génération du drop-in, parsing `--no-harden`) ; e2e multipass exerce le chemin durci (en gardant `--no-harden` pour les runs rapides). Le multipass pilote via `multipass exec`, pas SSH — couper root/mot de passe ne casse pas le harnais.
- **Pas dans un `CONTEXT.md`** : le vocabulaire ops/déploiement n'est pas du langage métier ; la distinction admin/service vit ici et dans la doc de déploiement, pas dans la CONTEXT-MAP (scopée aux modules Python).

## Limites à connaître

- **`<slug>` reste dans le groupe `docker`** (ADR-0017) : quiconque se logue en `<slug>` est root-équivalent via le daemon Docker. Ce durcissement réduit la **surface d'attaque distante** (root SSH, force brute mot de passe), pas l'escalade locale.
- **NOPASSWD : clé volée = root immédiat.** Acceptable car la clé SSH est déjà la frontière unique, mais ça impose une hygiène stricte de la clé privée de `ops`.
- **Auto-reboot = micro-coupure non planifiée** sur un VPS sans HA (ADR-0011) les nuits où un reboot est en attente. Mitigé par l'horaire (04:30) et `restart: unless-stopped`.
- **fail2ban est marginal** une fois l'authentification par mot de passe coupée : il sert surtout à réduire le bruit des scanners dans les logs.
- **La première install reste amorcée par `root`** (clé du panel cloud) : c'est l'ancre de confiance initiale, non éliminée par cette ADR — seulement refermée derrière soi une fois `ops` en place.
