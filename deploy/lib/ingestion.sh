# shellcheck shell=bash
# Lancement d'un ETL test (mode test = échantillon de 2 fichiers/flux) pour vérifier
# que la chaîne SFTP/file → déchiffrement AES → DuckDB répond. ATTENTION : l'échantillon
# n'est pas trié par date → il ne valide PAS la couverture du trousseau AES (une clé
# courante manquante peut passer inaperçue). Validation réelle = resync (cf. install.sh).

# _ingestion_parse_job_id <json>
# Extrait le job_id d'une réponse JSON /ingestion/run.
_ingestion_parse_job_id() {
    printf '%s\n' "$1" | sed -n 's/.*"job_id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -1
}

# _ingestion_parse_status <json>
# Extrait le champ status d'une réponse JSON de job.
_ingestion_parse_status() {
    printf '%s\n' "$1" | sed -n 's/.*"status"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -1
}

# _ingestion_call_post <slug> <api_key>
# POST /ingestion/run mode=test via le conteneur ingestion-scheduler.
# Imprime la réponse JSON sur stdout ; retourne le code curl.
_ingestion_call_post() {
    local slug="$1" api_key="$2" home="/srv/${1}"
    sudo -u "$slug" -- bash -c \
        "cd '${home}/deploy/docker' && \
         docker compose exec -T ingestion-scheduler curl -fsS \
             --max-time 30 \
             -X POST -H 'X-API-Key:${api_key}' -H 'Content-Type: application/json' \
             -d '{\"mode\":\"test\"}' http://api:8001/ingestion/run" \
        2>/dev/null
}

# _ingestion_call_get_job <slug> <api_key> <job_id>
# GET /ingestion/jobs/<job_id> via le conteneur.
# Imprime la réponse JSON sur stdout ; retourne le code curl.
# Surchargeable dans les tests unitaires sans docker/sudo.
_ingestion_call_get_job() {
    local slug="$1" api_key="$2" job_id="$3" home="/srv/${1}"
    sudo -u "$slug" -- bash -c \
        "cd '${home}/deploy/docker' && \
         docker compose exec -T ingestion-scheduler curl -fsS \
             --max-time 10 \
             -H 'X-API-Key:${api_key}' http://api:8001/ingestion/jobs/${job_id}" \
        2>/dev/null
}

# poll_ingestion_job <slug> <api_key> <job_id> [<max_retries>] [<delay_seconds>]
# Poll GET /ingestion/jobs/<job_id> jusqu'à status=completed (→0) ou failed/timeout (→1).
# Défauts : 30 retries × 4s = 2 min max.
poll_ingestion_job() {
    local slug="$1" api_key="$2" job_id="$3"
    local max="${4:-30}" delay="${5:-4}"
    local i=1 resp status
    while (( i <= max )); do
        resp=$(_ingestion_call_get_job "$slug" "$api_key" "$job_id") || true
        status=$(_ingestion_parse_status "$resp")
        case "$status" in
            completed) return 0 ;;
            failed)    return 1 ;;
        esac
        (( i < max )) && sleep "$delay"
        i=$((i+1))
    done
    log_warn "ETL test : job ${job_id} toujours en cours après ${max} tentatives — timeout."
    return 1
}

# run_ingestion_test <slug>
# Lance le test ETL (POST /ingestion/run mode=test), attend la complétion par poll,
# et logue le résultat. Retourne 0 si le job est completed, 1 sinon.
run_ingestion_test() {
    local slug="$1"
    local home="/srv/${slug}"
    local api_key
    api_key=$(read_env_var "${home}/.env" API_KEY)
    [[ -n "$api_key" ]] || { log_err "API_KEY introuvable dans ${home}/.env"; return 1; }

    local resp job_id
    resp=$(_ingestion_call_post "$slug" "$api_key") || {
        log_err "Échec du POST /ingestion/run (API inaccessible ou erreur HTTP)."
        return 1
    }
    job_id=$(_ingestion_parse_job_id "$resp")
    [[ -n "$job_id" ]] || {
        log_err "Pas de job_id dans la réponse POST /ingestion/run."
        return 1
    }

    log_info "Job ETL démarré : ${job_id} — attente de la complétion…"
    if poll_ingestion_job "$slug" "$api_key" "$job_id"; then
        log_ok "ETL test réussi — chaîne SFTP→déchiffrement→DuckDB OK sur un échantillon (2 fichiers)."
        return 0
    else
        return 1
    fi
}

# show_ingestion_failure_hints <slug>
# Affiche les 50 dernières lignes des logs ingestion-scheduler + les causes typiques.
show_ingestion_failure_hints() {
    local slug="$1"
    local home="/srv/${slug}"
    log_warn "Causes typiques :"
    log_info "  - Clé manquante au trousseau AES__TROUSSEAU__<label>__{KEY,IV} (flux non déchiffré → job failed)"
    log_info "  - SFTP__URL inaccessible (credentials ou réseau)"
    log_info "  - file://… pointe vers un dossier vide/inexistant (mode B)"
    log_info ""
    log_info "Logs ingestion-scheduler (50 dernières lignes) :"
    sudo -u "$slug" -- bash -c \
        "cd '${home}/deploy/docker' && docker compose logs --tail=50 ingestion-scheduler" \
        2>&1 | sed 's/^/     | /'
    log_info ""
    log_info "Pour réessayer après correction :"
    log_info "  sudo -u $slug docker compose -f ${home}/deploy/docker/docker-compose.yml \\"
    log_info "      exec ingestion-scheduler curl -X POST -H \"X-API-Key:\$API_KEY\" \\"
    log_info "      -H 'Content-Type: application/json' -d '{\"mode\":\"test\"}' \\"
    log_info "      http://api:8001/ingestion/run"
}
