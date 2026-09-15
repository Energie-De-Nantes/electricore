# shellcheck shell=bash
# Création du user système <slug>, home /srv/<slug>/, groupe docker, SSH key.
# Cf. ADR-0017.
#
# Toutes les fonctions dérivent le home d'instance via ${SRV_BASE:-/srv} : défaut
# /srv en prod, surchargeable par les tests pour opérer sur un home jetable sans
# root. chown_instance_home et ensure_backups_dir doivent ainsi viser le MÊME
# chemin (l'un écrase l'exception backups/ de l'autre, #459).

# create_instance_user <slug>
# Crée le user s'il n'existe pas, met son home à /srv/<slug>/, l'ajoute à docker.
# Garde-fou : refuse si le user existe mais avec un home différent (cas tordu).
create_instance_user() {
    local slug="$1"
    local home="${SRV_BASE:-/srv}/${slug}"
    if id "$slug" >/dev/null 2>&1; then
        local existing_home
        existing_home=$(getent passwd "$slug" | cut -d: -f6)
        if [[ "$existing_home" != "$home" ]]; then
            die "user $slug existe avec un home différent ($existing_home), refus de réutilisation." \
                "Choisir un autre slug ou supprimer le user existant à la main."
        fi
        log_skip "user $slug déjà présent (home $home)"
    else
        useradd --create-home --home-dir "$home" --shell /bin/bash "$slug"
        log_ok "user $slug créé (home $home)"
    fi
    if ! id -nG "$slug" | tr ' ' '\n' | grep -qx docker; then
        usermod -aG docker "$slug"
        log_ok "user $slug ajouté au groupe docker"
    else
        log_skip "user $slug déjà dans le groupe docker"
    fi
}

# setup_ssh_authorized_keys <slug> [<pubkey>]
# Si pubkey fournie : l'écrit dans authorized_keys (override).
# Sinon : copie ~root/.ssh/authorized_keys s'il existe.
setup_ssh_authorized_keys() {
    local slug="$1"
    local pubkey="${2:-}"
    local home="${SRV_BASE:-/srv}/${slug}"
    local ssh_dir="${home}/.ssh"
    local auth_file="${ssh_dir}/authorized_keys"
    install -d -m 700 -o "$slug" -g "$slug" "$ssh_dir"
    if [[ -n "$pubkey" ]]; then
        printf '%s\n' "$pubkey" > "$auth_file"
        log_ok "clé SSH installée pour $slug (depuis --ssh-pubkey)"
    elif [[ -r /root/.ssh/authorized_keys ]]; then
        cp /root/.ssh/authorized_keys "$auth_file"
        log_ok "clé SSH copiée depuis /root/.ssh/authorized_keys"
    else
        log_warn "aucune clé SSH disponible — $slug ne pourra pas se connecter en ssh." \
                 "Relancer plus tard avec --ssh-pubkey si besoin."
        return 0
    fi
    chown "$slug:$slug" "$auth_file"
    chmod 600 "$auth_file"
}

# chown_instance_home <slug>
# S'assure que tout sous /srv/<slug>/ est owned par le user.
# NB : ce chown -R aveugle écrase les exceptions uid conteneur → elles doivent être
# (ré)appliquées APRÈS lui, ICI MÊME, pour backups/ et les clés lues par le conteneur :
#   - backups/ (le dossier seul, non récursif) — #459 : ensure_backups_dir le
#     ré-assertait chez l'appelant, mais l'appel a sauté dans 284aed1 sans qu'aucun
#     test ne le voie (box Enargia sans sauvegarde, #734) ;
#   - age.key — fix #672 : generate_box_identities la chowne à CONTAINER_UID à
#     l'étape 7 du chemin relais, AVANT ce balayage (étape 10) qui l'écrasait à
#     CHAQUE reconfigure (constaté box Enargia, 28/07 : entrypoint SOPS
#     « permission denied » de retour) ;
#   - relais_ssh_key — même bug (constaté box Enargia, 26/08 : relais aveugle,
#     « permission denied /app/.ssh/id_ed25519 » sur chaque push) :
#     check_relais_ssh_key (étape 11 relais) la re-chowne, mais un reconfigure
#     SANS chemin relais balaie le home sans jamais y repasser.
# Auto-correctrice plutôt qu'un ré-assert chez chaque appelant : tout futur chemin
# qui balaie le home garde ces exceptions au conteneur. Le mode 2750 de backups/
# reste l'affaire d'ensure_backups_dir (chemin stack).
chown_instance_home() {
    local slug="$1"
    local home="${SRV_BASE:-/srv}/${slug}"
    chown -R "$slug:$slug" "$home"
    local cible
    for cible in age.key relais_ssh_key backups; do
        if [[ -e "${home}/${cible}" ]]; then
            chown "${CONTAINER_UID:-1000}:${CONTAINER_GID:-1000}" "${home}/${cible}" 2>/dev/null || true
        fi
    done
}

# Identité du user du conteneur (Dockerfile : `USER electricore`, uid:gid 1000:1000).
# Le bind-mount host des backups doit lui appartenir pour être writable. Overridable
# pour les tests (chown vers soi-même, sans root).
CONTAINER_UID="${CONTAINER_UID:-1000}"
CONTAINER_GID="${CONTAINER_GID:-1000}"

# ensure_backups_dir <slug>
# Crée /srv/<slug>/backups et le donne au user du conteneur (uid:gid 1000), en
# EXCEPTION du chown global de chown_instance_home (qui le donnerait à <slug>).
# Sans ça, le conteneur — qui tourne en uid 1000 — ne peut pas y écrire et
# backup_duckdb.sh plante au mkdir du snapshot (« Permission denied », #459) :
# aucune sauvegarde n'est produite.
#
# Doit être appelé APRÈS chown_instance_home pour écraser son `chown -R`. setgid
# (2750) : les snapshots créés par le conteneur héritent du groupe 1000. Idempotent
# (ré-asserte à chaque reconfigure).
#
# <slug> n'est PAS ajouté au groupe 1000 (#734) : sur l'hôte ce gid n'est pas forcément
# libre — box Enargia : `sftpusers`, chrooté par un `Match Group` sshd, y ajouter <slug>
# enfermerait ses sessions SSH. Lecture des backups côté host : root (`sudo ls`, offsite
# en `sudo rclone`). Les membres du groupe 1000 ont la lecture du dossier mais n'y
# arrivent pas tant que /srv/<slug> reste en 750 <slug>:<slug>.
#
# Le groupe n'a PAS le write (2750 = rwxr-s---), et c'est suffisant : backup_duckdb.sh
# — écriture du snapshot ET purge de rétention — tourne DANS le conteneur (uid 1000 =
# owner).
ensure_backups_dir() {
    local slug="$1"
    local backups="${SRV_BASE:-/srv}/${slug}/backups"
    install -d -m 2750 -o "$CONTAINER_UID" -g "$CONTAINER_GID" "$backups"
    log_ok "backups ${backups} → uid:gid ${CONTAINER_UID}:${CONTAINER_GID} (writable conteneur)"
}
