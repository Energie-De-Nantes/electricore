# shellcheck shell=bash
# Durcissement OS/SSH du VPS (ADR-0031). Sourcé par install.sh (étape par défaut)
# et par le wrapper autonome deploy/harden.sh.
#
# Trois rôles, distincts par construction :
#   - ops      : admin, sudo NOPASSWD, login SSH par clé uniquement
#   - <slug>   : service, groupe docker, pas de sudo (inchangé, ADR-0017)
#   - root     : SSH désactivé ; atteignable via sudo depuis ops
#
# Toutes les fonctions sont idempotentes et requièrent root (l'orchestrateur
# install.sh garantit EUID 0 ; le wrapper deploy/harden.sh re-vérifie).

# Utilisateur admin dédié. Override possible via env (tests, exotique).
HARDEN_ADMIN_USER="${HARDEN_ADMIN_USER:-ops}"

# ─── Utilisateur admin ──────────────────────────────────────────────────────

# ensure_admin_user <user>
# Crée l'utilisateur admin (home + shell) s'il n'existe pas. Idempotent :
# une fois par VPS, un reconfigure réutilise l'existant sans erreur.
ensure_admin_user() {
    local user="$1"
    if id "$user" >/dev/null 2>&1; then
        log_skip "user admin $user déjà présent"
    else
        useradd --create-home --shell /bin/bash "$user"
        log_ok "user admin $user créé"
    fi
}

# grant_nopasswd_sudo <user>
# Octroie sudo sans mot de passe via /etc/sudoers.d/<user>. NOPASSWD est imposé
# par le modèle clé-uniquement (ADR-0031) : l'admin n'a pas de mot de passe, un
# sudo interactif serait inutilisable. Validé par `visudo -cf` avant installation
# (une règle sudoers cassée verrouille tout escalade).
grant_nopasswd_sudo() {
    local user="$1"
    local file="/etc/sudoers.d/${user}"
    local tmp
    tmp="$(mktemp)"
    printf '%s ALL=(ALL) NOPASSWD:ALL\n' "$user" > "$tmp"
    if visudo -cf "$tmp" >/dev/null 2>&1; then
        install -m 0440 -o root -g root "$tmp" "$file"
        rm -f "$tmp"
        log_ok "sudo NOPASSWD pour $user (${file})"
    else
        rm -f "$tmp"
        die "règle sudoers invalide pour $user — abandon (sécurité)."
    fi
}

# seed_admin_key <user> [<pubkey>]
# Amorce le authorized_keys de l'admin. Si <pubkey> fournie : l'écrit (override
# --admin-pubkey). Sinon : copie ~root/.ssh/authorized_keys (l'ancre de confiance
# de la première install). Idempotent : réécrit le fichier à chaque appel.
seed_admin_key() {
    local user="$1"
    local pubkey="${2:-}"
    local home
    home="$(getent passwd "$user" | cut -d: -f6)"
    [[ -n "$home" ]] || die "impossible de résoudre le home de $user"
    local ssh_dir="${home}/.ssh"
    local auth_file="${ssh_dir}/authorized_keys"
    install -d -m 700 -o "$user" -g "$user" "$ssh_dir"
    if [[ -n "$pubkey" ]]; then
        printf '%s\n' "$pubkey" > "$auth_file"
        log_ok "clé SSH installée pour $user (depuis --admin-pubkey)"
    elif [[ -s /root/.ssh/authorized_keys ]]; then
        cp /root/.ssh/authorized_keys "$auth_file"
        log_ok "clé SSH de $user copiée depuis /root/.ssh/authorized_keys"
    else
        log_warn "aucune clé SSH disponible pour $user." \
                 "Fournir --admin-pubkey avant de couper le SSH root (garde-fou anti-verrouillage)."
        return 0
    fi
    chown "$user:$user" "$auth_file"
    chmod 600 "$auth_file"
}

# ─── Garde-fou anti-verrouillage ────────────────────────────────────────────

# authorized_keys_present <file>
# 0 si le fichier existe et contient au moins une entrée de clé (ligne non vide
# et non commentaire). Pur, sans side-effect — testable hors VPS.
authorized_keys_present() {
    local file="$1"
    [[ -f "$file" ]] || return 1
    grep -qE '^[[:space:]]*[^#[:space:]]' "$file"
}

# admin_has_authorized_key <user>
# Applique authorized_keys_present au home de l'utilisateur. C'est le verrou du
# garde-fou : on refuse de couper root/mot-de-passe SSH tant que l'admin n'a pas
# de clé exploitable (sinon : verrouillage à distance définitif).
admin_has_authorized_key() {
    local user="$1"
    local home
    home="$(getent passwd "$user" | cut -d: -f6)"
    [[ -n "$home" ]] || return 1
    authorized_keys_present "${home}/.ssh/authorized_keys"
}

# ─── Orchestrateur ──────────────────────────────────────────────────────────

# harden_vps
# Orchestre le durcissement (ADR-0031). Lit les globals OPT_* posés par cli.sh :
#   OPT_ADMIN_PUBKEY   clé SSH override pour l'admin (sinon copie root)
#
# Cette tranche ne pose que la partie SÛRE : utilisateur admin + sudo + clé.
# Aucune modification sshd ici → le SSH root reste actif, aucun verrouillage
# possible. Le verrouillage sshd, fail2ban et unattended-upgrades arrivent dans
# les tranches suivantes.
harden_vps() {
    local user="${HARDEN_ADMIN_USER}"
    local pubkey="${OPT_ADMIN_PUBKEY:-}"

    ensure_admin_user "$user"
    seed_admin_key "$user" "$pubkey"
    grant_nopasswd_sudo "$user"
}
