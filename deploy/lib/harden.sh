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

# ─── Verrouillage sshd ──────────────────────────────────────────────────────

# Drop-in de durcissement. Le répertoire sshd_config.d/ est inclus par défaut
# sur Debian 12 / Ubuntu 22.04+ (cf. is_supported_os). Override pour les tests.
SSHD_HARDEN_DROPIN="${SSHD_HARDEN_DROPIN:-/etc/ssh/sshd_config.d/50-electricore-harden.conf}"

# render_sshd_hardening
# Émet le contenu du drop-in sshd sur stdout. Pur, sans side-effect — testable.
render_sshd_hardening() {
    cat <<'EOF'
# Durcissement SSH ElectriCore (ADR-0031) — généré par deploy/lib/harden.sh.
# Ne pas éditer à la main : régénéré à chaque durcissement. Rechargé via
# `systemctl reload ssh` après validation `sshd -t`.
PermitRootLogin no
PasswordAuthentication no
KbdInteractiveAuthentication no
PubkeyAuthentication yes
X11Forwarding no
MaxAuthTries 3
EOF
}

# harden_sshd
# Pose le drop-in (root-off, clé uniquement), valide par `sshd -t`, puis
# `reload` (jamais `restart` — les sessions ouvertes survivent). Précédé du
# garde-fou anti-verrouillage : refuse de basculer si l'admin n'a pas de clé.
harden_sshd() {
    local user="${HARDEN_ADMIN_USER}"
    # ── Garde-fou anti-verrouillage (ordre impératif, ADR-0031) ──
    if ! admin_has_authorized_key "$user"; then
        die "garde-fou anti-verrouillage : $user n'a pas de clé SSH exploitable." \
            "Refus de couper le SSH root. Fournir --admin-pubkey puis relancer."
    fi
    install -d -m 755 "$(dirname "$SSHD_HARDEN_DROPIN")"
    render_sshd_hardening > "$SSHD_HARDEN_DROPIN"
    chmod 0644 "$SSHD_HARDEN_DROPIN"
    # Valider AVANT de recharger : une conf cassée empêcherait sshd de démarrer.
    if ! sshd -t 2>/dev/null; then
        rm -f "$SSHD_HARDEN_DROPIN"
        die "sshd -t a rejeté le durcissement — drop-in retiré, sshd inchangé."
    fi
    # reload, jamais restart : ne tue pas les sessions en cours (dont la session
    # root d'installation). Les nouveaux logins root/mot-de-passe échouent.
    if systemctl reload ssh 2>/dev/null || systemctl reload sshd 2>/dev/null; then
        :
    else
        die "échec du reload sshd — vérifier 'systemctl status ssh'."
    fi
    log_ok "sshd durci : root-off, clé uniquement, MaxAuthTries 3 (${SSHD_HARDEN_DROPIN})"
}

# ─── fail2ban ───────────────────────────────────────────────────────────────

# Jail fail2ban. Override pour les tests.
FAIL2BAN_JAIL="${FAIL2BAN_JAIL:-/etc/fail2ban/jail.d/electricore.conf}"

# render_fail2ban_jail
# Émet la conf du jail sshd sur stdout. Pur, sans side-effect — testable.
# `backend = systemd` est REQUIS : sur Debian/Ubuntu récents les logins SSH vont
# dans le journal systemd, pas dans /var/log/auth.log (le défaut historique ne
# lirait rien). Cf. ADR-0031, alternative écartée « backend auth.log ».
render_fail2ban_jail() {
    cat <<'EOF'
# Jail fail2ban ElectriCore (ADR-0031) — généré par deploy/lib/harden.sh.
[sshd]
enabled  = true
backend  = systemd
port     = ssh
maxretry = 3
findtime = 10m
bantime  = 1h
EOF
}

# setup_fail2ban
# Installe fail2ban et active le jail sshd (backend systemd). Idempotent :
# ensure_packages saute si déjà là, la conf est réécrite, le service redémarré
# pour recharger le jail. Marginal une fois le mot de passe coupé — sert surtout
# à réduire le bruit des scanners dans les logs.
setup_fail2ban() {
    ensure_packages fail2ban
    install -d -m 755 "$(dirname "$FAIL2BAN_JAIL")"
    render_fail2ban_jail > "$FAIL2BAN_JAIL"
    chmod 0644 "$FAIL2BAN_JAIL"
    systemctl enable fail2ban >/dev/null 2>&1 || true
    if systemctl restart fail2ban 2>/dev/null || systemctl start fail2ban 2>/dev/null; then
        log_ok "fail2ban actif : jail sshd, backend=systemd (${FAIL2BAN_JAIL})"
    else
        die "échec du (re)démarrage de fail2ban — vérifier 'systemctl status fail2ban'."
    fi
}

# ─── Mises à jour automatiques ──────────────────────────────────────────────

# Fichiers apt.conf.d. Override pour les tests.
UNATTENDED_PERIODIC="${UNATTENDED_PERIODIC:-/etc/apt/apt.conf.d/20auto-upgrades}"
UNATTENDED_OVERRIDE="${UNATTENDED_OVERRIDE:-/etc/apt/apt.conf.d/52electricore-unattended}"
# Après le backup de 03:30 (cf. crontab) : un patch kernel/openssl en attente
# s'applique vraiment, et la stack revient seule (restart: unless-stopped).
UNATTENDED_REBOOT_TIME="${UNATTENDED_REBOOT_TIME:-04:30}"

# render_unattended_periodic
# Active la maj des listes de paquets + l'application unattended. Pur, testable.
render_unattended_periodic() {
    cat <<'EOF'
// ElectriCore (ADR-0031) — active les mises à jour de sécurité automatiques.
APT::Periodic::Update-Package-Lists "1";
APT::Periodic::Unattended-Upgrade "1";
EOF
}

# render_unattended_override
# Redémarrage auto après application des correctifs, à l'heure configurée. Pur.
# Les origines de sécurité sont déjà activées par défaut dans
# /etc/apt/apt.conf.d/50unattended-upgrades (Debian & Ubuntu) — on ne touche
# qu'au comportement de reboot pour éviter les patterns d'origine distro-spécifiques.
render_unattended_override() {
    cat <<EOF
// ElectriCore (ADR-0031) — redémarrage auto après mise à jour, après le backup.
Unattended-Upgrade::Automatic-Reboot "true";
Unattended-Upgrade::Automatic-Reboot-Time "${UNATTENDED_REBOOT_TIME}";
EOF
}

# setup_unattended_upgrades
# Installe unattended-upgrades, active les maj de sécurité + l'auto-reboot.
# Idempotent. Risque faible : la stack est `restart: unless-stopped` et Docker
# démarre au boot → auto-rétablissement en ~1 min après le reboot.
setup_unattended_upgrades() {
    ensure_packages unattended-upgrades
    render_unattended_periodic > "$UNATTENDED_PERIODIC"
    chmod 0644 "$UNATTENDED_PERIODIC"
    render_unattended_override > "$UNATTENDED_OVERRIDE"
    chmod 0644 "$UNATTENDED_OVERRIDE"
    systemctl enable apt-daily-upgrade.timer >/dev/null 2>&1 || true
    log_ok "unattended-upgrades : maj sécurité + reboot auto ${UNATTENDED_REBOOT_TIME} (après backup 03:30)"
}

# ─── Orchestrateur ──────────────────────────────────────────────────────────

# harden_vps
# Orchestre le durcissement (ADR-0031). Lit les globals OPT_* (cli.sh côté
# install.sh, parse_harden_args côté deploy/harden.sh) :
#   OPT_ADMIN_PUBKEY   clé SSH override pour l'admin (sinon copie root)
#   OPT_NO_SSHD        saute le verrouillage sshd (garde root SSH actif)
#   OPT_NO_FAIL2BAN    saute fail2ban
#   OPT_NO_UNATTENDED  saute unattended-upgrades
#
# Ordre impératif (ADR-0031) : on amorce d'abord l'admin (user + sudo + clé),
# le garde-fou anti-verrouillage (au seuil de harden_sshd) vérifie que ops a une
# clé exploitable, et SEULEMENT ensuite on coupe le SSH root. La session root en
# cours survit au `reload` ; la prochaine connexion se fait en ops.
harden_vps() {
    local user="${HARDEN_ADMIN_USER}"
    local pubkey="${OPT_ADMIN_PUBKEY:-}"

    ensure_admin_user "$user"
    seed_admin_key "$user" "$pubkey"
    grant_nopasswd_sudo "$user"

    if [[ "${OPT_NO_SSHD:-0}" -eq 1 ]]; then
        log_skip "verrouillage sshd ignoré (--no-sshd) — SSH root inchangé"
    else
        harden_sshd
    fi
    if [[ "${OPT_NO_FAIL2BAN:-0}" -eq 1 ]]; then
        log_skip "fail2ban ignoré (--no-fail2ban)"
    else
        setup_fail2ban
    fi
    if [[ "${OPT_NO_UNATTENDED:-0}" -eq 1 ]]; then
        log_skip "unattended-upgrades ignoré (--no-unattended-upgrades)"
    else
        setup_unattended_upgrades
    fi
}
