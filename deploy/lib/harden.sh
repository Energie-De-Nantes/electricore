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

# ─── Préflight sshd non-vierge (#656) ───────────────────────────────────────
# Avant de poser le drop-in global (PasswordAuthentication no), audite les
# comptes EXISTANTS du système : un compte au mot de passe usable, sans clé SSH,
# et sans bloc Match qui le protège serait coupé silencieusement par le drop-in
# global. Complémentaire au garde-fou anti-verrouillage ci-dessus (qui protège
# seulement `ops`) — même précédent structurel : refus AVANT tout changement,
# rien n'est posé, sshd n'est pas rechargé.
#
# `root` est explicitement HORS AUDIT (revue #656) : couper root SSH est la
# juridiction du garde-fou anti-verrouillage ci-dessus, pas de ce préflight — et
# la remédiation « Match User <compte> + PasswordAuthentication yes » proposée
# aux autres comptes ne s'applique PAS à root : `PermitRootLogin no` (posé par
# le même drop-in, cf. render_sshd_hardening) coupe root indépendamment de
# PasswordAuthentication, un Match ne le rendrait pas.
#
# Diff avant/après (revue #656) : chaque compte candidat est sondé DEUX fois —
# sur la config réelle SEULE (avant) et sur la config fusionnée avec le drop-in
# (après) — et seule la bascule yes→no est signalée. Sur une box déjà durcie
# (reconfigure), avant=no déjà (le drop-in existant coupe déjà le compte) →
# silencieux quel que soit après ; une box vierge est inchangée (avant=yes,
# après=yes|no selon Match).

# sshd_preflight_at_risk_accounts
# Lit sur stdin des enregistrements
# "user:passwd_state:has_key:effective_avant:effective_apres" (un par ligne) :
#   passwd_state      2e champ de `passwd -S` (P=utilisable, L=verrouillé, NP=sans mdp)
#   has_key           1 si authorized_keys_present pour ce compte, 0 sinon
#   effective_avant   PasswordAuthentication effectif SANS le drop-in (config réelle seule)
#   effective_apres   PasswordAuthentication effectif AVEC le drop-in fusionné
# Émet (stdout) les comptes à risque : mot de passe utilisable + pas de clé + qui
# BASCULENT yes→no (revue #656 — ni no→no « déjà coupé » ni yes→yes « toujours
# protégé » ne sont signalés). `root` est explicitement exclu — cf. commentaire
# de section (juridiction du garde-fou anti-verrouillage). Pur, sans side-effect
# — testable sur des fixtures, sans VM ni root. Seul critère d'exclusion sur le
# shell : aucun — passwd_state≠P (verrouillé L, ou sans mot de passe NP) est le
# seul filtre ; un compte SFTP-only via ForceCommand internal-sftp a un shell
# nologin mais authentifie normalement (cf. #656).
sshd_preflight_at_risk_accounts() {
    local user passwd_state has_key avant apres
    while IFS=: read -r user passwd_state has_key avant apres; do
        [[ -n "$user" ]] || continue
        [[ "$user" != "root" ]] || continue        # juridiction du garde-fou anti-verrouillage
        [[ "$passwd_state" == "P" ]] || continue   # verrouillé/sans mdp : rien à couper
        [[ "$has_key" == "0" ]] || continue         # une clé → déjà migré, pas à risque
        [[ "$avant" == "yes" && "$apres" == "no" ]] || continue   # seule la bascule yes→no est signalée
        printf '%s\n' "$user"
    done
}

# sshd_preflight_oracle_failed_accounts
# Même flux d'enregistrements que sshd_preflight_at_risk_accounts. Émet les
# comptes CANDIDATS (mot de passe utilisable, pas de clé, hors root) pour
# lesquels effective_avant ou effective_apres est vide/imparsable (ni "yes" ni
# "no") — l'oracle sshd -T n'a pas su conclure (config invalide, permissions,
# bloc Match mal résolu…). Fail-closed (revue #656) : un tel compte doit
# REFUSER le durcissement, jamais passer silencieusement — l'absence de valeur
# ne doit JAMAIS être lue comme "yes" implicite. Pur, testable.
sshd_preflight_oracle_failed_accounts() {
    local user passwd_state has_key avant apres
    while IFS=: read -r user passwd_state has_key avant apres; do
        [[ -n "$user" ]] || continue
        [[ "$user" != "root" ]] || continue
        [[ "$passwd_state" == "P" ]] || continue
        [[ "$has_key" == "0" ]] || continue
        if [[ ! "$avant" =~ ^(yes|no)$ ]] || [[ ! "$apres" =~ ^(yes|no)$ ]]; then
            printf '%s\n' "$user"
        fi
    done
}

# sshd_preflight_last_password_login <user>
# Dernier login par mot de passe de <user> observé dans le journal systemd sshd
# (`Accepted password for <user> ` — c'est la MÉTHODE D'AUTH qui compte, pas le
# dernier login toutes méthodes : lastlog/wtmp ne distinguent pas mot de passe vs
# clé, demande de Virgile #656). AIDE À LA DÉCISION affichée dans le message de
# refus — jamais un critère : la logique refuse/passe n'en dépend pas (pas
# d'auto-pass pour un compte "mort"). Bornée à la fenêtre observable : si rien
# trouvé, annonce depuis quand le journal sshd est observable — JAMAIS "jamais
# utilisé" (le journal peut avoir tourné/été purgé avant). Impur (journalctl),
# dégradé propre si absent/inaccessible. Appelée UNIQUEMENT sur le chemin de
# refus (comptes déjà signalés) par sshd_preflight_login_hints, jamais sur le
# balayage nominal (perf — cf. sshd_preflight_collect). Surchargeable en test
# (même seam que sshd_preflight_collect).
sshd_preflight_last_password_login() {
    local user="$1"
    command -v journalctl >/dev/null 2>&1 || { printf 'indisponible (journalctl absent)'; return; }
    # Un seul appel journalctl ; filtré aux lignes horodatées réelles (écarte les
    # en-têtes/pseudo-lignes type "-- No entries --" ou "-- Journal begins at … --",
    # qui n'ont pas de timestamp ISO en tête et fausseraient "depuis <date>").
    local log
    log="$(journalctl -u ssh -u sshd -o short-iso 2>/dev/null | grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2}T')"
    local last
    last="$(grep -F "Accepted password for ${user} " <<<"$log" | tail -1 | awk '{print $1}')"
    if [[ -n "$last" ]]; then
        printf '%s' "$last"
        return
    fi
    local oldest
    oldest="$(head -1 <<<"$log" | awk '{print $1}')"
    if [[ -n "$oldest" ]]; then
        printf 'aucun login mdp sur la fenêtre du journal (depuis %s)' "$oldest"
    else
        printf 'indisponible (journal sshd vide ou inaccessible)'
    fi
}

# sshd_preflight_login_hints <users>
# Formatte, pour chaque utilisateur de <users> (un par ligne), le dernier login
# par mot de passe observé — annexé aux messages de refus (fail-closed ET
# comptes à risque). Aide à la décision uniquement (cf.
# sshd_preflight_last_password_login).
sshd_preflight_login_hints() {
    local users="$1" user
    while IFS= read -r user; do
        [[ -n "$user" ]] || continue
        printf '\n  - %s : dernier login mdp — %s' "$user" "$(sshd_preflight_last_password_login "$user")"
    done <<<"$users"
}

# sshd_preflight_merged_config [<sshd_config>]
# Construit un sshd_config FUSIONNÉ temporaire : le drop-in de durcissement (PAS
# ENCORE posé sur disque) en tête, suivi du contenu RÉEL de <sshd_config> (jamais
# modifié — lecture seule vis-à-vis de /etc/ssh). Émet le chemin du fichier
# temporaire sur stdout ; à charge de l'appelant de le supprimer. Sur Debian/Ubuntu,
# le drop-in réel serait inclus tout en haut du fichier (`Include sshd_config.d/*.conf`,
# cf. is_supported_os) — le préfixer ici reproduit cette précédence à l'identique ;
# les blocs Match existants (généralement en fin de fichier, ou dans un drop-in déjà
# posé — relu via l'Include du fichier réel) continuent de primer sur nos globales
# pour les connexions qu'ils couvrent.
sshd_preflight_merged_config() {
    local real_conf="${1:-/etc/ssh/sshd_config}"
    local tmp
    tmp="$(mktemp)"
    { render_sshd_hardening; echo; cat "$real_conf"; } > "$tmp"
    printf '%s\n' "$tmp"
}

# sshd_preflight_parse_passwordauth
# Extrait la valeur de PasswordAuthentication d'un dump `sshd -T` lu sur stdin.
# Pur — isole la couture awk du binaire sshd, testable avec un vrai dump en
# fixture (#656 AC5).
sshd_preflight_parse_passwordauth() {
    awk '$1 == "passwordauthentication" { print $2; exit }'
}

# sshd_preflight_effective_passwordauth <conf> <user>
# Valeur effective de PasswordAuthentication pour <user> sur <conf> — résolution
# des blocs Match déléguée à sshd lui-même (`sshd -T -C …`), pas de parsing
# regex maison (la couture awk est isolée dans sshd_preflight_parse_passwordauth).
# La spec passée à -C est TOUJOURS complète (user, host, addr, laddr, lport) :
# sur les OpenSSH 9.x (Debian 12 / Ubuntu 24.04), un -C incomplet en présence
# d'un bloc Match Address/Host/LocalAddress/LocalPort peut faire fatal() sshd -T
# au lieu de conclure (revue #656) — host/addr/laddr/lport sont figés (connexion
# locale simulée), seul user varie, ce qui suffit à résoudre les Match User/Group
# visés par ce préflight.
sshd_preflight_effective_passwordauth() {
    local conf="$1" user="$2"
    sshd -T -f "$conf" \
        -C "user=${user},host=localhost,addr=127.0.0.1,laddr=127.0.0.1,lport=22" 2>/dev/null \
        | sshd_preflight_parse_passwordauth
}

# sshd_preflight_collect
# Énumère les comptes du système (getent passwd, root exclu — cf. commentaire de
# section), sonde passwd -S + authorized_keys, et pour les seuls comptes
# CANDIDATS (mot de passe utilisable, pas de clé) sonde les deux valeurs
# effectives (avant/après drop-in). Émet les enregistrements consommés par
# sshd_preflight_at_risk_accounts / sshd_preflight_oracle_failed_accounts.
# Perf (revue #656) : les sondes sshd -T (un fork sshd par appel, deux par
# candidat) ne sont lancées QUE pour les candidats — un compte verrouillé, sans
# mot de passe, déjà en clé, ou root n'a besoin d'aucun oracle, la décision les
# exclut de toute façon. Impur : nécessite root (passwd -S lit /etc/shadow) et
# le binaire sshd — non testé en unitaire par construction (couvert par l'e2e
# multipass, cf. deploy/tests/e2e/).
sshd_preflight_collect() {
    local real_conf="/etc/ssh/sshd_config"
    local merged_conf u home passwd_state has_key avant apres
    merged_conf="$(sshd_preflight_merged_config "$real_conf")"
    while IFS=: read -r u _ _ _ _ home _; do
        [[ "$u" != "root" ]] || continue   # hors audit (juridiction du garde-fou anti-verrouillage)
        passwd_state="$(passwd -S "$u" 2>/dev/null | awk '{print $2}')" || true
        [[ -n "$passwd_state" ]] || continue   # pas d'entrée shadow (rare) → ignoré
        has_key=0
        authorized_keys_present "${home}/.ssh/authorized_keys" && has_key=1
        if [[ "$passwd_state" == "P" && "$has_key" == "0" ]]; then
            avant="$(sshd_preflight_effective_passwordauth "$real_conf" "$u")" || true
            apres="$(sshd_preflight_effective_passwordauth "$merged_conf" "$u")" || true
        else
            avant=""; apres=""   # pas candidat : ni la décision ni le fail-closed ne les regardent
        fi
        printf '%s:%s:%s:%s:%s\n' "$u" "$passwd_state" "$has_key" "$avant" "$apres"
    done < <(getent passwd)
    rm -f "$merged_conf"
}

# sshd_preflight_refuse_if_at_risk
# Refuse (die) AVANT tout changement si un compte existant BASCULERAIT yes→no
# sous le durcissement — rien n'est posé, sshd n'est pas rechargé. Fail-closed
# (revue #656) : si l'oracle sshd -T n'a pas conclu pour un compte candidat,
# refuse aussi, avec un message DÉDIÉ (les remédiations "comptes à risque" ne
# s'appliquent pas à un oracle en panne). Silencieux (log_ok) sur une box
# vierge, déjà durcie (no→no) ou couverte (Match/clé). Surchargeable en test en redéfinissant
# sshd_preflight_collect (même précédent que poll_ingestion_job /
# _ingestion_call_get_job) et, pour les dates de dernier login, en redéfinissant
# sshd_preflight_last_password_login.
sshd_preflight_refuse_if_at_risk() {
    local records
    records="$(sshd_preflight_collect)"

    local failed
    failed="$(sshd_preflight_oracle_failed_accounts <<<"$records")"
    if [[ -n "$failed" ]]; then
        die "préflight sshd : impossible de conclure pour $(paste -sd' ' - <<<"$failed") — l'oracle sshd -T n'a pas répondu (config sshd invalide, bloc Match mal résolu, ou permissions insuffisantes)." \
            "Refus de durcir par prudence (fail-closed) : le préflight ne peut pas garantir qu'aucun compte ne serait coupé. Diagnostiquer à la main : sshd -T -f /etc/ssh/sshd_config -C user=<compte>,host=localhost,addr=127.0.0.1,laddr=127.0.0.1,lport=22 ; puis relancer.$(sshd_preflight_login_hints "$failed")"
    fi

    local at_risk
    at_risk="$(sshd_preflight_at_risk_accounts <<<"$records")"
    if [[ -n "$at_risk" ]]; then
        die "préflight sshd : compte(s) au mot de passe qui serai(en)t coupé(s) — $(paste -sd' ' - <<<"$at_risk")." \
            "Remédier : migrer le(s) compte(s) en clé SSH (authorized_keys), ou poser une exception 'Match User <compte>' + 'PasswordAuthentication yes' dans un drop-in sshd_config.d/ (numéroté APRÈS ${SSHD_HARDEN_DROPIN##*/}) ; puis relancer.$(sshd_preflight_login_hints "$at_risk")"
    fi

    log_ok "préflight sshd : aucun compte existant ne serait coupé par le durcissement."
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
# `reload` (jamais `restart` — les sessions ouvertes survivent). Précédé de deux
# refus possibles (rien n'est posé, sshd n'est pas rechargé si l'un des deux tombe) :
#   1. garde-fou anti-verrouillage : l'admin (ops) n'a pas de clé exploitable.
#   2. préflight non-vierge (#656) : un compte EXISTANT serait coupé (mot de passe
#      usable, pas de clé, aucun Match ne le protège).
harden_sshd() {
    local user="${HARDEN_ADMIN_USER}"
    # ── Garde-fou anti-verrouillage (ordre impératif, ADR-0031) ──
    if ! admin_has_authorized_key "$user"; then
        die "garde-fou anti-verrouillage : $user n'a pas de clé SSH exploitable." \
            "Refus de couper le SSH root. Fournir --admin-pubkey puis relancer."
    fi
    # ── Préflight non-vierge (#656) : refuse si un compte existant serait coupé ──
    sshd_preflight_refuse_if_at_risk
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

# ─── Réversibilité (désinstallation) ────────────────────────────────────────
# Inverse de harden_vps : retire ce que le durcissement a posé. Idempotent.
# Pensé pour un retour arrière sûr — rétablir l'accès root AVANT tout nettoyage.

# unharden_sshd
# Retire le drop-in de durcissement et recharge sshd → restaure le comportement
# par défaut de l'image (root SSH ré-autorisé, auth selon le défaut cloud). C'est
# LA réversion critique : exécutée en premier pour regagner l'accès root.
unharden_sshd() {
    if [[ ! -f "$SSHD_HARDEN_DROPIN" ]]; then
        log_skip "drop-in sshd absent — sshd déjà au défaut, rien à retirer"
        return 0
    fi
    rm -f "$SSHD_HARDEN_DROPIN"
    if ! sshd -t 2>/dev/null; then
        die "sshd -t a échoué après retrait du drop-in — vérifier la conf sshd à la main."
    fi
    if systemctl reload ssh 2>/dev/null || systemctl reload sshd 2>/dev/null; then
        log_ok "drop-in sshd retiré, sshd rechargé — accès root rétabli (défaut image)"
    else
        die "échec du reload sshd après retrait du drop-in — vérifier 'systemctl status ssh'."
    fi
}

# remove_fail2ban_jail
# Retire la conf de jail ElectriCore et recharge fail2ban. Laisse le paquet
# installé (on ne désinstalle pas : d'autres jails peuvent en dépendre).
remove_fail2ban_jail() {
    if [[ ! -f "$FAIL2BAN_JAIL" ]]; then
        log_skip "jail fail2ban ElectriCore absente — rien à retirer"
        return 0
    fi
    rm -f "$FAIL2BAN_JAIL"
    systemctl restart fail2ban 2>/dev/null || true
    log_ok "jail fail2ban ElectriCore retirée (${FAIL2BAN_JAIL})"
}

# remove_unattended_config
# Retire les fichiers apt.conf.d posés par le durcissement (auto-reboot + maj
# auto). N'efface pas la conf distro par défaut (50unattended-upgrades) ni le
# paquet : on revient simplement au comportement d'origine de l'image.
remove_unattended_config() {
    local removed=0
    [[ -f "$UNATTENDED_OVERRIDE" ]] && { rm -f "$UNATTENDED_OVERRIDE"; removed=1; }
    [[ -f "$UNATTENDED_PERIODIC" ]] && { rm -f "$UNATTENDED_PERIODIC"; removed=1; }
    if [[ "$removed" -eq 1 ]]; then
        log_ok "conf unattended-upgrades ElectriCore retirée (auto-reboot 04:30 désactivé)"
    else
        log_skip "conf unattended-upgrades ElectriCore absente — rien à retirer"
    fi
}

# remove_admin_user <user>
# Retrait OPT-IN de l'admin (sudoers + compte + home). Destructif → réservé à
# --purge-ops. À n'exécuter qu'après unharden_sshd (root SSH déjà rétabli),
# sinon on se prive du seul accès non-root.
remove_admin_user() {
    local user="$1"
    rm -f "/etc/sudoers.d/${user}"
    if id "$user" >/dev/null 2>&1; then
        if userdel -r "$user" 2>/dev/null; then
            log_ok "user admin $user retiré (sudoers + compte + home)"
        else
            log_warn "échec userdel $user (session active ?) — sudoers retiré, compte à supprimer à la main."
        fi
    else
        rm -f "/etc/sudoers.d/${user}"
        log_skip "user admin $user absent — sudoers nettoyé"
    fi
}

# unharden_vps
# Inverse de harden_vps. Lit les globals OPT_* :
#   OPT_PURGE_OPS   supprime aussi le user ops (destructif ; défaut: conservé)
#
# Ordre impératif : on rétablit le SSH root EN PREMIER (regagner l'accès), puis
# on nettoie fail2ban + unattended ; le retrait de ops (opt-in) vient en dernier,
# une fois root réaccessible.
unharden_vps() {
    local user="${HARDEN_ADMIN_USER}"

    unharden_sshd
    remove_fail2ban_jail
    remove_unattended_config
    if [[ "${OPT_PURGE_OPS:-0}" -eq 1 ]]; then
        remove_admin_user "$user"
    else
        log_info "user admin $user conservé (le supprimer : --purge-ops ; re-durcir : deploy/harden.sh)"
    fi
}
