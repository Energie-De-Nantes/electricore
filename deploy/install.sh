#!/usr/bin/env bash
# ElectriCore installer — provisionne une instance mono-tenant sur VPS frais.
# Cf. ADR-0017 (layout /srv/<slug>/), ADR-0011 (stack docker compose),
#     ADR-0015 (multi-instance par VPS).
#
# Étapes implémentées (chemin nominal, issue #48) :
#   1. Détection OS + check root
#   2. apt packages + Docker via get-docker.com
#   3. UFW : OpenSSH + 80 + 443
#   4. Création user système <slug> + SSH key
#   5. Téléchargement config tag-pinné
#   6. Substitutions (slug, domaine, email)
#   7. Édition .env + validation (loop)
#   8. DNS check bloquant
#   9. docker compose up + wait_for_health
#
# ETL test + mode reconfigure + récap final : issue #49.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/log.sh
source "${SCRIPT_DIR}/lib/log.sh"
# shellcheck source=lib/cli.sh
source "${SCRIPT_DIR}/lib/cli.sh"
# shellcheck source=lib/validate.sh
source "${SCRIPT_DIR}/lib/validate.sh"
# shellcheck source=lib/os.sh
source "${SCRIPT_DIR}/lib/os.sh"
# shellcheck source=lib/system.sh
source "${SCRIPT_DIR}/lib/system.sh"
# shellcheck source=lib/user.sh
source "${SCRIPT_DIR}/lib/user.sh"
# shellcheck source=lib/config.sh
source "${SCRIPT_DIR}/lib/config.sh"
# shellcheck source=lib/env_validate.sh
source "${SCRIPT_DIR}/lib/env_validate.sh"
# shellcheck source=lib/dns.sh
source "${SCRIPT_DIR}/lib/dns.sh"
# shellcheck source=lib/stack.sh
source "${SCRIPT_DIR}/lib/stack.sh"

LOG_TOTAL_STEPS=9

parse_args "$@" || { usage; exit 2; }

validate_slug "$OPT_SLUG" || die "slug invalide : '$OPT_SLUG' (attendu [a-z0-9-]+, 2-32 chars)"
validate_domain "$OPT_DOMAIN" || die "domaine invalide : '$OPT_DOMAIN'"
[[ -z "$OPT_EMAIL" || "$OPT_EMAIL" =~ ^[^@[:space:]]+@[^@[:space:]]+\.[^@[:space:]]+$ ]] \
    || die "email invalide : '$OPT_EMAIL'"

HOME_DIR="/srv/${OPT_SLUG}"
ENV_FILE="${HOME_DIR}/.env"
DOCKER_DIR="${HOME_DIR}/deploy/docker"
CADDYFILE="${DOCKER_DIR}/Caddyfile"

# ─── Étape 1 : OS + root ────────────────────────────────────────────────
log_step "Détection OS + privilèges"
[[ $EUID -eq 0 ]] || die "Le script doit être lancé en root (sudo bash $0 ...)"
is_supported_os || die "OS non supporté : $(detect_os)" \
    "Supporté : Ubuntu 22.04+/24.04+ ou Debian 12+."
log_ok "OS : $(detect_os)"

# ─── Étape 2 : paquets ──────────────────────────────────────────────────
log_step "Paquets système"
ensure_packages curl jq cron dnsutils

# ─── Étape 3 : Docker + UFW ─────────────────────────────────────────────
log_step "Docker"
install_docker_if_missing
log_step "UFW (ports 80/443 + SSH)"
setup_ufw

# ─── Étape 4 : user système + SSH ───────────────────────────────────────
log_step "User système ${OPT_SLUG} (home ${HOME_DIR})"
create_instance_user "$OPT_SLUG"
setup_ssh_authorized_keys "$OPT_SLUG" "$OPT_SSH_PUBKEY"

# ─── Étape 5 : config files ─────────────────────────────────────────────
log_step "Téléchargement config (tag ${OPT_VERSION})"
download_config_files "$OPT_VERSION" "$HOME_DIR"
chown_instance_home "$OPT_SLUG"

# ─── Étape 6 : substitutions ────────────────────────────────────────────
log_step "Patch des templates (slug + domaine + email)"
substitute_env "$ENV_FILE" "$OPT_SLUG"
substitute_caddyfile "$CADDYFILE" "$OPT_DOMAIN" "$OPT_EMAIL"
if [[ -z "$OPT_EMAIL" ]]; then
    log_warn "email Let's Encrypt non fourni — placeholder conservé dans ${CADDYFILE}." \
             "Éditer à la main avant de mettre en prod."
fi

# ─── Étape 7 : édition .env + validation ────────────────────────────────
log_step "Configuration .env (édition + validation)"
if [[ -n "$OPT_ENV_FROM" ]]; then
    [[ -r "$OPT_ENV_FROM" ]] || die "--env-from : fichier illisible : $OPT_ENV_FROM"
    cp "$OPT_ENV_FROM" "$ENV_FILE"
    chown "$OPT_SLUG:$OPT_SLUG" "$ENV_FILE"
    chmod 600 "$ENV_FILE"
    log_info "chargement depuis $OPT_ENV_FROM"
else
    log_info "ouverture de $ENV_FILE dans \${EDITOR:-vi}…"
fi
while true; do
    if [[ -z "$OPT_ENV_FROM" ]]; then
        sudo -u "$OPT_SLUG" "${EDITOR:-vi}" "$ENV_FILE"
    fi
    if errs=$(validate_env_file "$ENV_FILE" "$OPT_SLUG"); then
        log_ok ".env valide"
        chmod 600 "$ENV_FILE"
        break
    fi
    log_err ".env invalide :"
    printf '%s\n' "$errs" | sed 's/^/     - /'
    if [[ -n "$OPT_ENV_FROM" ]]; then
        die ".env fourni invalide, abandon."
    fi
    prepend_errors_to_env "$ENV_FILE" "$errs"
    log_warn "ré-ouverture de l'éditeur dans 2s…"
    sleep 2
done

# ─── Étape 8 : DNS ──────────────────────────────────────────────────────
log_step "Vérification DNS"
if [[ "$OPT_SKIP_DNS" -eq 1 ]]; then
    log_skip "DNS check ignoré (--skip-dns)"
else
    public_ip=$(get_public_ip) || die "Impossible de déterminer l'IP publique du VPS."
    log_info "IP publique du VPS : ${public_ip}"
    log_info "Attente que ${OPT_DOMAIN} pointe vers ${public_ip} (jusqu'à 5 min)…"
    wait_for_dns "$OPT_DOMAIN" "$public_ip" || die \
        "DNS non propagé après 5 minutes." \
        "Vérifier le A-record de ${OPT_DOMAIN}. Relancer le script quand c'est propre."
fi

# ─── Étape 9 : stack ────────────────────────────────────────────────────
log_step "Démarrage de la stack docker compose"
compose_up "$OPT_SLUG"
log_info "Attente du healthcheck API (jusqu'à 60s)…"
wait_for_health "$OPT_SLUG" || die \
    "API non healthy après 60s." \
    "Voir les logs : sudo -u $OPT_SLUG docker compose -f $DOCKER_DIR/docker-compose.yml logs"

log_ok "Instance ${OPT_SLUG} opérationnelle."
log_info "URL : https://${OPT_DOMAIN}"
log_info "SSH : ssh ${OPT_SLUG}@${OPT_DOMAIN}"
log_info ""
log_info "Étapes suivantes (issue #49) : ETL test + mode reconfigure + récap détaillé."
