#!/usr/bin/env bash
# ElectriCore installer — provisionne une instance mono-tenant sur VPS frais.
# Cf. ADR-0017 (layout /srv/<slug>/), ADR-0011 (stack docker compose),
#     ADR-0015 (multi-instance par VPS).
#
# Étapes (chemin nominal + reconfigure, issues #48 et #49) :
#   1. Détection OS + check root
#   2. apt packages
#   3. Docker (idempotent)
#   4. UFW : OpenSSH + 80 + 443
#   5. Création user système <slug> + SSH key
#   6. Téléchargement config tag-pinné
#   7. Substitutions (slug, domaine, email)
#   8. Édition .env + validation (loop)
#   9. DNS check bloquant
#  10. docker compose up + wait_for_health
#  11. ETL test (mode test, ~3s)
#  12. Récap final
#
# Mode reconfigure : si /srv/<slug>/.env existe déjà, on backup le .env,
# on saute la création user/Docker/UFW (idempotents de toute façon), on
# ne télécharge pas le .env (mais on rafraîchit compose/Caddy/crontab pour
# bump de version), et on ne touche jamais à la DB DuckDB.
#
# Sourçage : le script est protégé par un guard `main` (en fin de fichier).
# Sourcer install.sh expose `fetch_lib_files` sans déclencher l'installation.
# Utile pour les tests unitaires de `deploy/tests/unit.sh`.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_BASE_URL_DEFAULT="${INSTALL_BASE_URL:-https://raw.githubusercontent.com/Energie-De-Nantes/electricore/main/deploy}"
LIB_FILES=(log cli validate os system user config env_validate dns stack ingestion)

# fetch_lib_files <base_url> <target_dir>
# Télécharge les helpers `${LIB_FILES[@]}` depuis <base_url> vers <target_dir>.
# Utilisée par le bootstrap initial (`lib/` absent) ET le refresh reconfigure
# (cf. issue #62 : éviter qu'un lib/ figé sur une version antérieure du script
# ne court-circuite les corrections embarquées dans une nouvelle install.sh).
fetch_lib_files() {
    local base="$1"
    local target="$2"
    command -v curl >/dev/null || { echo "curl introuvable, install curl puis relance." >&2; return 1; }
    install -d "$target"
    local f
    for f in "${LIB_FILES[@]}"; do
        if ! curl -fsSL "${base}/${f}.sh" -o "${target}/${f}.sh"; then
            echo "✗ Échec téléchargement ${f}.sh depuis ${base}/" >&2
            return 1
        fi
    done
}

# _source_lib <dir> : (re-)source tous les helpers depuis <dir>.
# Idempotent en bash : les fonctions sont redéfinies, les globals (CONFIG_BASE_URL,
# codes couleur, etc.) sont écrasées proprement.
_source_lib() {
    local dir="$1"
    local f
    for f in "${LIB_FILES[@]}"; do
        # shellcheck source=/dev/null
        source "${dir}/${f}.sh"
    done
}

# Bootstrap initial : si lib/ est absent (quickstart `curl install.sh | bash`),
# on télécharge les helpers depuis main (ou INSTALL_BASE_URL si surchargé).
if [[ ! -d "${SCRIPT_DIR}/lib" ]]; then
    echo "→ Bootstrap : téléchargement des helpers depuis ${INSTALL_BASE_URL_DEFAULT}/lib/…"
    fetch_lib_files "${INSTALL_BASE_URL_DEFAULT}/lib" "${SCRIPT_DIR}/lib" || exit 1
    echo "✓ Helpers téléchargés dans ${SCRIPT_DIR}/lib/"
fi

_source_lib "${SCRIPT_DIR}/lib"


main() {
    set -euo pipefail

    LOG_TOTAL_STEPS=12

    # Trap propre : Ctrl+C, kill, exit non-zero. Nettoie le script get-docker.sh
    # temporaire et signale clairement l'abandon. Ne touche jamais à la DB ni au .env.
    cleanup() {
        local rc=$?
        [[ -f /tmp/get-docker.sh ]] && rm -f /tmp/get-docker.sh
        if [[ $rc -ne 0 ]] && [[ "${_CLEAN_EXIT:-0}" -ne 1 ]]; then
            echo
            printf '%s\n' "Script interrompu (exit $rc). Aucune modification destructive faite." >&2
            if [[ -n "${OPT_SLUG:-}" && -n "${OPT_DOMAIN:-}" ]]; then
                printf '%s\n' "Pour relancer (mode reconfigure si déjà partiellement installé) :" >&2
                printf '   sudo bash %s --slug %s --domain %s\n' "$0" "$OPT_SLUG" "$OPT_DOMAIN" >&2
            fi
        fi
    }
    trap cleanup EXIT INT TERM

    parse_args "$@" || { usage; exit 2; }

    validate_slug "$OPT_SLUG" || die "slug invalide : '$OPT_SLUG' (attendu [a-z0-9-]+, 2-32 chars)"
    validate_domain "$OPT_DOMAIN" || die "domaine invalide : '$OPT_DOMAIN'"
    [[ -z "$OPT_EMAIL" || "$OPT_EMAIL" =~ ^[^@[:space:]]+@[^@[:space:]]+\.[^@[:space:]]+$ ]] \
        || die "email invalide : '$OPT_EMAIL'"

    HOME_DIR="/srv/${OPT_SLUG}"
    ENV_FILE="${HOME_DIR}/.env"
    DOCKER_DIR="${HOME_DIR}/deploy/docker"
    CADDYFILE="${DOCKER_DIR}/Caddyfile"

    # ─── Détection mode reconfigure ─────────────────────────────────────────
    MODE_RECONFIGURE=0
    if [[ -f "$ENV_FILE" ]]; then
        MODE_RECONFIGURE=1
    fi
    # Garde-fou ADR-0017 : si /srv/<slug> existe sans .env, on est dans un état
    # inconnu (peut-être un user système qui s'appelle aussi <slug>). Refus poli.
    if [[ -d "$HOME_DIR" && "$MODE_RECONFIGURE" -eq 0 ]]; then
        die "Le dossier ${HOME_DIR} existe mais ne contient pas de .env." \
            "État ambigu — choisir un autre slug ou supprimer ${HOME_DIR} à la main."
    fi

    # En mode reconfigure : refresh inconditionnel des helpers lib/ pour éviter
    # qu'un lib/ figé sur une version antérieure du script ne court-circuite
    # les fixes embarqués dans cette nouvelle install.sh (#62).
    if [[ "$MODE_RECONFIGURE" -eq 1 ]]; then
        log_info "Mode reconfigure : refresh des helpers lib/ depuis ${INSTALL_BASE_URL_DEFAULT}/lib/…"
        if fetch_lib_files "${INSTALL_BASE_URL_DEFAULT}/lib" "${SCRIPT_DIR}/lib"; then
            _source_lib "${SCRIPT_DIR}/lib"
            log_ok "Helpers lib/ rafraîchis."
        else
            log_warn "Refresh des helpers échoué — on continue avec la version locale (potentiellement stale)."
        fi
    fi

    # ─── Étape 1 : OS + root ────────────────────────────────────────────────
    log_step "Détection OS + privilèges"
    [[ $EUID -eq 0 ]] || die "Le script doit être lancé en root (sudo bash $0 ...)"
    is_supported_os || die "OS non supporté : $(detect_os)" \
        "Supporté : Ubuntu 22.04+/24.04+ ou Debian 12+."
    log_ok "OS : $(detect_os)"
    if [[ "$MODE_RECONFIGURE" -eq 1 ]]; then
        log_info "Instance ${OPT_SLUG} déjà installée — MODE RECONFIGURE activé."
    fi

    # ─── Étape 2 : paquets ──────────────────────────────────────────────────
    log_step "Paquets système"
    ensure_packages curl jq cron dnsutils nano

    # ─── Étape 3 : Docker ───────────────────────────────────────────────────
    log_step "Docker"
    install_docker_if_missing

    # ─── Étape 4 : UFW ──────────────────────────────────────────────────────
    log_step "UFW (ports 80/443 + SSH)"
    setup_ufw

    # ─── Étape 5 : user système + SSH ───────────────────────────────────────
    log_step "User système ${OPT_SLUG} (home ${HOME_DIR})"
    create_instance_user "$OPT_SLUG"
    setup_ssh_authorized_keys "$OPT_SLUG" "$OPT_SSH_PUBKEY"

    # ─── Étape 6 : config files ─────────────────────────────────────────────
    log_step "Téléchargement config (tag ${OPT_VERSION})"
    download_config_files "$OPT_VERSION" "$HOME_DIR"
    chown_instance_home "$OPT_SLUG"

    # ─── Étape 7 : substitutions ────────────────────────────────────────────
    log_step "Patch des templates (slug + domaine + email)"
    substitute_env "$ENV_FILE" "$OPT_SLUG" "$OPT_VERSION"
    substitute_caddyfile "$CADDYFILE" "$OPT_DOMAIN" "$OPT_EMAIL"
    if [[ -z "$OPT_EMAIL" ]]; then
        log_warn "email Let's Encrypt non fourni — placeholder conservé dans ${CADDYFILE}." \
                 "Éditer à la main avant de mettre en prod."
    fi

    # ─── Étape 8 : édition .env + validation ────────────────────────────────
    log_step "Configuration .env (édition + validation)"
    if [[ "$MODE_RECONFIGURE" -eq 1 ]]; then
        backup="${ENV_FILE}.bak.$(date +%Y%m%dT%H%M%SZ)"
        cp -p "$ENV_FILE" "$backup"
        chown "$OPT_SLUG:$OPT_SLUG" "$backup"
        log_info "backup du .env existant → ${backup}"
    fi
    if [[ -n "$OPT_ENV_FROM" ]]; then
        [[ -r "$OPT_ENV_FROM" ]] || die "--env-from : fichier illisible : $OPT_ENV_FROM"
        cp "$OPT_ENV_FROM" "$ENV_FILE"
        chown "$OPT_SLUG:$OPT_SLUG" "$ENV_FILE"
        chmod 600 "$ENV_FILE"
        log_info "chargement depuis $OPT_ENV_FROM"
    else
        log_info "ouverture de $ENV_FILE dans \${EDITOR:-nano}…"
    fi
    while true; do
        if [[ -z "$OPT_ENV_FROM" ]]; then
            sudo -u "$OPT_SLUG" "${EDITOR:-nano}" "$ENV_FILE"
        fi
        if errs=$(validate_env_file "$ENV_FILE" "$OPT_SLUG"); then
            log_ok ".env valide"
            strip_validation_error_block "$ENV_FILE"
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

    # ─── Étape 9 : DNS ──────────────────────────────────────────────────────
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

    # ─── Étape 10 : stack ───────────────────────────────────────────────────
    log_step "Démarrage de la stack docker compose"
    compose_up "$OPT_SLUG"
    log_info "Attente du healthcheck API (jusqu'à 60s)…"
    wait_for_health "$OPT_SLUG" || die \
        "API non healthy après 60s." \
        "Voir les logs : sudo -u $OPT_SLUG docker compose -f $DOCKER_DIR/docker-compose.yml logs"

    # ─── Étape 11 : ETL test ────────────────────────────────────────────────
    log_step "ETL test (mode test, ~3s)"
    if run_ingestion_test "$OPT_SLUG"; then
        log_ok "ETL test réussi — clés AES OK, SFTP accessible, DuckDB écrit."
    else
        log_err "ETL test échoué — la stack tourne mais la chaîne ETL ne valide pas."
        show_ingestion_failure_hints "$OPT_SLUG"
        log_warn "Stack laissée en place. Corrige .env et réessaye (commande ci-dessus)."
    fi

    # ─── Étape 12 : récap ───────────────────────────────────────────────────
    log_step "Récapitulatif"
    cat <<EOF

  ${_C_GREEN}${_C_BOLD}✓ Instance ${OPT_SLUG} opérationnelle.${_C_RESET}

  URL              https://${OPT_DOMAIN}
  /health          curl https://${OPT_DOMAIN}/health
  SSH              ssh ${OPT_SLUG}@${OPT_DOMAIN}
  Logs             sudo -u ${OPT_SLUG} docker compose -f ${DOCKER_DIR}/docker-compose.yml logs -f
  Stop/Start       sudo -u ${OPT_SLUG} docker compose -f ${DOCKER_DIR}/docker-compose.yml {down,up -d}
  Backups          ${HOME_DIR}/backups/ (rotation 14 snapshots, cron 03:30 Europe/Paris)
  ETL nocturne     02:00 Europe/Paris (cf. ${DOCKER_DIR}/crontab)

  Étapes suivantes recommandées :
    - Configurer un offsite des backups (rclone vers un cloud — cf. docs/deploiement.md)
    - Ajouter https://${OPT_DOMAIN}/health à votre monitoring distant
    - Sauvegarder ${ENV_FILE} dans un gestionnaire de secrets

  Pour reconfigurer plus tard (rotation clés AES, bump version, etc.) :
    sudo bash $0 --slug ${OPT_SLUG} --domain ${OPT_DOMAIN}

EOF

    _CLEAN_EXIT=1
}


# Guard : n'exécute `main` que si le script est exécuté (`bash install.sh ...`),
# pas s'il est sourcé (cas des tests unitaires qui veulent juste les fonctions).
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
