# shellcheck shell=bash
# Création du user système <slug>, home /srv/<slug>/, groupe docker, SSH key.
# Cf. ADR-0017.

# create_instance_user <slug>
# Crée le user s'il n'existe pas, met son home à /srv/<slug>/, l'ajoute à docker.
# Garde-fou : refuse si le user existe mais avec un home différent (cas tordu).
create_instance_user() {
    local slug="$1"
    local home="/srv/${slug}"
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
    local home="/srv/${slug}"
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
chown_instance_home() {
    local slug="$1"
    chown -R "$slug:$slug" "/srv/${slug}"
}
