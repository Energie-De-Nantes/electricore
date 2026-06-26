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
# NB : ce chown -R aveugle écrase aussi backups/ → l'exception uid 1000 doit être
# (ré)appliquée APRÈS lui via ensure_backups_dir (cf. #459, même classe que le seam
# safe.directory du deploy-repo).
chown_instance_home() {
    local slug="$1"
    local home="${SRV_BASE:-/srv}/${slug}"
    chown -R "$slug:$slug" "$home"
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
# (2750) : les snapshots créés par le conteneur héritent du groupe 1000, donc
# <slug> — ajouté à ce groupe par ensure_slug_in_container_group — peut les lire
# et les pousser en offsite (rclone). Idempotent (ré-asserte à chaque reconfigure).
ensure_backups_dir() {
    local slug="$1"
    local backups="${SRV_BASE:-/srv}/${slug}/backups"
    install -d -m 2750 -o "$CONTAINER_UID" -g "$CONTAINER_GID" "$backups"
    log_ok "backups ${backups} → uid:gid ${CONTAINER_UID}:${CONTAINER_GID} (writable conteneur)"
}

# ensure_slug_in_container_group <slug>
# Ajoute <slug> au groupe gid CONTAINER_GID (celui du user conteneur) pour qu'il
# puisse lire les backups écrits en gid 1000 (`ls`, offsite rclone). Crée le groupe
# s'il n'existe pas encore sur l'hôte (un host sans user uid 1000 n'a pas forcément
# de groupe gid 1000). Idempotent.
ensure_slug_in_container_group() {
    local slug="$1"
    local gid="$CONTAINER_GID"
    local grp
    grp=$(getent group "$gid" | cut -d: -f1)
    if [[ -z "$grp" ]]; then
        grp="electricore-data"
        if ! groupadd -g "$gid" "$grp" 2>/dev/null; then
            log_warn "création du groupe gid ${gid} échouée — lecture des backups par ${slug} non garantie."
            return 0
        fi
        log_ok "groupe ${grp} (gid ${gid}) créé"
    fi
    if id -nG "$slug" 2>/dev/null | tr ' ' '\n' | grep -qx "$grp"; then
        log_skip "user $slug déjà dans le groupe $grp (gid $gid)"
    else
        usermod -aG "$grp" "$slug"
        log_ok "user $slug ajouté au groupe $grp (gid $gid) — lecture des backups"
    fi
}
