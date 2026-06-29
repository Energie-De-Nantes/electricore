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
#   6. Durcissement VPS (admin ops + sshd root-off + fail2ban + maj auto, ADR-0031 ; --no-harden)
#   7. Téléchargement config tag-pinné
#   8. Substitutions (slug, domaine, email)
#   9. Identité box + pull dépôt secrets + validation config.env + box_can_decrypt (ADR-0044)
#  10. DNS check bloquant
#  11. docker compose up + wait_for_health
#  12. Test ingestion (mode test, ~3s) — valide le CONTENU des secrets via le vrai conteneur
#  13. Récap final
#
# Mode reconfigure : si /srv/<slug>/ porte déjà une clé age (ou un legacy .env),
# on saute la création user/Docker/UFW (idempotents de toute façon), on rafraîchit
# compose/Caddy/crontab pour le bump de version, et on ne touche jamais à la DB DuckDB.
#
# Sourçage : le script est protégé par un guard `main` (en fin de fichier).
# Sourcer install.sh expose `fetch_lib_files` sans déclencher l'installation.
# Utile pour les tests unitaires de `deploy/tests/unit.sh`.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_BASE_URL_DEFAULT="${INSTALL_BASE_URL:-https://raw.githubusercontent.com/Energie-De-Nantes/electricore/main/deploy}"
LIB_FILES=(log cli validate os system user harden config env_validate secrets dns stack ingestion)

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

# lib_dir_complete <dir> : vrai si <dir> existe ET contient TOUS les helpers
# attendus (`${LIB_FILES[@]}`). Un lib/ figé sur une version antérieure du script
# ne contient pas les nouveaux helpers (ex. harden.sh ajouté pour ADR-0031) :
# `_source_lib` échouerait et le vieux cli.sh rejetterait les nouvelles options
# (#62, trap stale-lib).
lib_dir_complete() {
    local dir="$1" f
    [[ -d "$dir" ]] || return 1
    for f in "${LIB_FILES[@]}"; do
        [[ -f "${dir}/${f}.sh" ]] || return 1
    done
}

# Résolution du répertoire des helpers :
#   - lib/ co-localisé ET complet (checkout repo, /srv/<slug>/deploy/, tests,
#     run offline/pinné) → on l'utilise tel quel.
#   - sinon (quickstart `curl install.sh`, OU lib/ figé d'un run antérieur) → on
#     télécharge une copie FRAÎCHE dans un dossier temporaire, jamais réutilisée
#     ni laissée à pourrir à côté du script. Élimine le trap stale-lib à la racine
#     (pas de lib/ persistant dans /root) plutôt que de le rattraper après coup.
HELPERS_DIR="${SCRIPT_DIR}/lib"
HELPERS_DIR_TEMP=""
if ! lib_dir_complete "$HELPERS_DIR"; then
    HELPERS_DIR_TEMP="$(mktemp -d "${TMPDIR:-/tmp}/electricore-lib.XXXXXX")"
    HELPERS_DIR="$HELPERS_DIR_TEMP"
    echo "→ Bootstrap : téléchargement des helpers (copie fraîche) dans ${HELPERS_DIR}…"
    fetch_lib_files "${INSTALL_BASE_URL_DEFAULT}/lib" "$HELPERS_DIR" || exit 1
    echo "✓ Helpers téléchargés (temporaire, nettoyé en fin de run)."
fi

_source_lib "$HELPERS_DIR"


main() {
    set -euo pipefail

    LOG_TOTAL_STEPS=13

    # Trap propre : Ctrl+C, kill, exit non-zero. Nettoie le script get-docker.sh
    # temporaire et signale clairement l'abandon. Ne touche jamais à la DB ni au .env.
    cleanup() {
        local rc=$?
        [[ -f /tmp/get-docker.sh ]] && rm -f /tmp/get-docker.sh
        # Copie temporaire des helpers (bootstrap /tmp) : on nettoie en sortie.
        [[ -n "${HELPERS_DIR_TEMP:-}" ]] && rm -rf "$HELPERS_DIR_TEMP"
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

    # Secrets-as-code (ADR-0044) ajoute 2 étapes (identité box + pull) à la place de
    # l'édition .env → le compteur d'étapes s'ajuste pour rester juste.
    [[ -n "${OPT_DEPLOY_REPO:-}" ]] && LOG_TOTAL_STEPS=15

    validate_slug "$OPT_SLUG" || die "slug invalide : '$OPT_SLUG' (attendu [a-z0-9-]+, 2-32 chars)"
    validate_domain "$OPT_DOMAIN" || die "domaine invalide : '$OPT_DOMAIN'"
    [[ -z "$OPT_EMAIL" || "$OPT_EMAIL" =~ ^[^@[:space:]]+@[^@[:space:]]+\.[^@[:space:]]+$ ]] \
        || die "email invalide : '$OPT_EMAIL'"

    HOME_DIR="/srv/${OPT_SLUG}"
    ENV_FILE="${HOME_DIR}/.env"
    DOCKER_DIR="${HOME_DIR}/deploy/docker"
    CADDYFILE="${DOCKER_DIR}/Caddyfile"

    # ─── Détection mode reconfigure ─────────────────────────────────────────
    # En secrets-as-code (ADR-0044), la box ne porte plus de .env mais une clé age :
    # sa présence atteste un install antérieur (2e temps de l'onboarding = reconfigure).
    AGE_KEY_FILE="${HOME_DIR}/age.key"
    MODE_RECONFIGURE=0
    if [[ -f "$ENV_FILE" || -f "$AGE_KEY_FILE" ]]; then
        MODE_RECONFIGURE=1
    fi
    # Garde-fou ADR-0017 : si /srv/<slug> existe sans .env NI clé age, on est dans un
    # état inconnu (peut-être un user système qui s'appelle aussi <slug>). Refus poli.
    if [[ -d "$HOME_DIR" && "$MODE_RECONFIGURE" -eq 0 ]]; then
        die "Le dossier ${HOME_DIR} existe mais ne contient ni .env ni clé age (age.key)." \
            "État ambigu — choisir un autre slug ou supprimer ${HOME_DIR} à la main."
    fi

    # En mode reconfigure avec des helpers CO-LOCALISÉS : refresh pour éviter
    # qu'un lib/ figé sur une version antérieure ne court-circuite les fixes de
    # cette install.sh (#62). Inutile si le bootstrap a déjà tiré une copie fraîche
    # en /tmp (HELPERS_DIR_TEMP non vide).
    if [[ "$MODE_RECONFIGURE" -eq 1 && -z "$HELPERS_DIR_TEMP" ]]; then
        log_info "Mode reconfigure : refresh des helpers depuis ${INSTALL_BASE_URL_DEFAULT}/lib/…"
        if fetch_lib_files "${INSTALL_BASE_URL_DEFAULT}/lib" "$HELPERS_DIR"; then
            _source_lib "$HELPERS_DIR"
            log_ok "Helpers rafraîchis."
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

    # ─── Étape 6 : durcissement VPS (ADR-0031) ──────────────────────────────
    # Actif par défaut, juste après l'étape user/clé. Le garde-fou anti-verrouillage
    # (clé de ops non vide) protège la bascule root-off ; --no-harden pour sauter.
    log_step "Durcissement VPS — admin ops + sshd + fail2ban + maj auto"
    if [[ "${OPT_NO_HARDEN:-0}" -eq 1 ]]; then
        log_skip "durcissement ignoré (--no-harden)"
    else
        harden_vps
    fi

    # ─── Étape 7 : config files ─────────────────────────────────────────────
    log_step "Téléchargement config (tag ${OPT_VERSION})"
    # En secrets-as-code (--deploy-repo) : pas de .env téléchargé, la config claire
    # arrive par le dépôt (providers/<slug>/config.env, ADR-0044).
    skip_env=0; [[ -n "$OPT_DEPLOY_REPO" ]] && skip_env=1
    download_config_files "$OPT_VERSION" "$HOME_DIR" "$skip_env"
    chown_instance_home "$OPT_SLUG"

    # ─── Étape 8 : substitutions ────────────────────────────────────────────
    log_step "Patch des templates (slug + domaine + email)"
    substitute_caddyfile "$CADDYFILE" "$OPT_DOMAIN" "$OPT_EMAIL"
    if [[ -z "$OPT_EMAIL" ]]; then
        log_warn "email Let's Encrypt non fourni — placeholder conservé dans ${CADDYFILE}." \
                 "Éditer à la main avant de mettre en prod."
    fi

    # ─── Étape 9 : secrets-as-code (ADR-0044) ──────────────────────────────
    # --deploy-repo est obligatoire depuis le cutover (cli.sh) : on génère l'identité
    # de la box, on tire le dépôt, on valide la POLITIQUE de split (config.env : présence
    # des substitutions compose + anti-leak) et on teste que la box DÉCHIFFRE (box_can_decrypt).
    # On ne valide PLUS le contenu de secrets.env par un preflight bash : le vrai conteneur le
    # fait verbatim via `sops exec-env` aux étapes 11-12 (cf. ADR-0049, classe de bug rc12).
    log_step "Identité de la box (age + SSH deploy key)"
    if pubs=$(generate_box_identities "$OPT_SLUG"); then
        age_pub=$(printf '%s\n' "$pubs" | sed -n 's/^AGE_PUBLIC_KEY=//p')
        ssh_pub=$(printf '%s\n' "$pubs" | sed -n 's/^SSH_DEPLOY_PUBKEY=//p')
    else
        die "Génération des identités de la box échouée (image ${SECRETS_IMAGE} indisponible ?)."
    fi

    log_step "Pull du dépôt de déploiement privé"
    if ! pull_deploy_repo "$OPT_DEPLOY_REPO" "$OPT_SLUG"; then
        # Onboarding EN DEUX TEMPS (ADR-0044 §4) : la deploy key SSH doit être
        # enregistrée comme deploy key RO avant que le pull réussisse.
        print_onboarding_pending "$OPT_SLUG" "$OPT_DOMAIN" "$age_pub" "$ssh_pub"
        _CLEAN_EXIT=1
        return 0
    fi

    log_step "Validation du split config + test de déchiffrement"
    config_env="${HOME_DIR}/config.env"
    if errs=$(validate_config_env "$config_env" "$OPT_SLUG"); then
        log_ok "config.env valide"
    else
        log_err "config.env invalide :"; printf '%s\n' "$errs" | sed 's/^/     - /'
        die "Corrige providers/${OPT_SLUG}/config.env dans le dépôt, pousse, puis relance reconfigure."
    fi

    # Override LOCAL de la version (#460) : `--version` pilote le tag d'image déployé même
    # en secrets-as-code, SANS toucher au dépôt secrets (la version n'est pas un secret,
    # c'est un paramètre de déploiement — ADR-0044). Appliqué APRÈS le pull : le config.env
    # du dépôt reste la baseline GitOps (reconfigure sans --version = version pinée). Le pull
    # ré-écrit config.env à chaque reconfigure → l'override est ré-appliqué à chaque fois.
    # (Régression #299 corrigée : l'appel avait été supprimé → --version devenait inerte.)
    if [[ "${OPT_VERSION_EXPLICIT:-0}" -eq 1 ]]; then
        repo_version=$(read_env_var "$config_env" ELECTRICORE_VERSION)
        override_config_version "$config_env" "$OPT_VERSION"
        log_warn "ELECTRICORE_VERSION overridé localement : ${OPT_VERSION} (dépôt : ${repo_version}) — non versionné, propre à cette box."
    fi

    if ! box_can_decrypt "$OPT_SLUG"; then
        # La box a sa clé + le ciphertext, mais ne déchiffre pas → sa clé age publique
        # n'est pas (encore) destinataire dans .sops.yaml. 2e temps de l'onboarding.
        print_onboarding_pending "$OPT_SLUG" "$OPT_DOMAIN" "$age_pub" "$ssh_pub"
        die "La box ne déchiffre pas encore : enregistre sa clé age comme destinataire (.sops.yaml + sops updatekeys), pousse, puis relance reconfigure."
    fi
    log_ok "secrets.env déchiffrable (contenu validé par le conteneur, étapes 11-12)"

    # ─── Étape 10 : DNS ─────────────────────────────────────────────────────
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

    # ─── Étape 11 : stack ───────────────────────────────────────────────────
    log_step "Démarrage de la stack docker compose"
    compose_up "$OPT_SLUG"
    log_info "Attente du healthcheck API (jusqu'à 60s)…"
    wait_for_health "$OPT_SLUG" || die \
        "API non healthy après 60s." \
        "Voir les logs : sudo -u $OPT_SLUG docker compose -f $DOCKER_DIR/docker-compose.yml logs"

    # ─── Étape 12 : test ingestion ──────────────────────────────────────────
    log_step "Test ingestion (mode test, ~3s)"
    if run_ingestion_test "$OPT_SLUG"; then
        log_warn "Échantillon non daté → ne garantit PAS la couverture du trousseau AES ; lancer un resync pour valider la clé courante."
    else
        log_err "Test ingestion échoué — la stack tourne mais la chaîne ingestion ne valide pas."
        show_ingestion_failure_hints "$OPT_SLUG"
        log_warn "Stack laissée en place. Corrige secrets.env et réessaye (commande ci-dessus)."
    fi

    # ─── Étape 13 : récap ───────────────────────────────────────────────────
    log_step "Récapitulatif"
    # Récap SSH : une fois durci (ADR-0031), l'admin passe par ops (root SSH coupé) ;
    # le user de service <slug> reste joignable. Sans durcissement, seul <slug>.
    if [[ "${OPT_NO_HARDEN:-0}" -eq 1 ]]; then
        ssh_recap="  SSH (service)    ssh ${OPT_SLUG}@${OPT_DOMAIN}"
    else
        ssh_recap="  SSH (admin)      ssh ops@${OPT_DOMAIN}   ← root SSH désactivé
  SSH (service)    ssh ${OPT_SLUG}@${OPT_DOMAIN}"
    fi
    # Image RÉELLEMENT déployée : valeur effective de ELECTRICORE_VERSION dans config.env (qui
    # a pu être overridée localement par --version, #460), pas l'option brute. Fallback sur
    # OPT_VERSION pour le chemin legacy .env (pas de config.env).
    effective_version=$(read_env_var "${HOME_DIR}/config.env" ELECTRICORE_VERSION 2>/dev/null || true)
    [[ -n "$effective_version" ]] || effective_version="$OPT_VERSION"
    cat <<EOF

  ${_C_GREEN}${_C_BOLD}✓ Instance ${OPT_SLUG} opérationnelle.${_C_RESET}

  URL              https://${OPT_DOMAIN}
  /health          curl https://${OPT_DOMAIN}/health
  Image            ghcr.io/energie-de-nantes/electricore:${effective_version}
${ssh_recap}
  Logs             sudo -u ${OPT_SLUG} docker compose -f ${DOCKER_DIR}/docker-compose.yml logs -f
  Stop/Start       sudo -u ${OPT_SLUG} docker compose -f ${DOCKER_DIR}/docker-compose.yml {down,up -d}
  Backups          ${HOME_DIR}/backups/ (rotation 14 snapshots, cron 03:30 Europe/Paris)
  Ingestion nocturne  02:00 Europe/Paris (cf. ${DOCKER_DIR}/crontab)

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
