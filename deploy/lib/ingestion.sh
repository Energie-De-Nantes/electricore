# shellcheck shell=bash
# Lancement d'un test ingestion (mode test = échantillon de 2 fichiers/flux) pour vérifier
# que la chaîne SFTP/file → déchiffrement AES → DuckDB répond. ATTENTION : l'échantillon
# n'est pas trié par date → il ne valide PAS la couverture du trousseau AES (une clé
# courante manquante peut passer inaperçue). Validation réelle = resync (cf. install.sh).

# _ingestion_parse_job_id <json>
# Extrait l'identifiant de job de la réponse POST /ingestion/run. L'API le sérialise sous
# la clé "id" (IngestionJobResponse.id) — PAS "job_id", qui n'est qu'un nom de paramètre de
# route et de prose. Au POST (202), "output" est null → aucune autre occurrence de "id" ne
# parasite l'extraction.
_ingestion_parse_job_id() {
    printf '%s\n' "$1" | sed -n 's/.*"id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -1
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

# _ingestion_read_scheduler_key <slug>
# Imprime la clé du consommateur "scheduler" du trousseau API (ADR-0046 §4) en déchiffrant
# secrets.env sur le HOST (sops + clé age — même motif que box_can_decrypt dans secrets.sh).
# `docker compose exec` ne convient PAS : il hérite de l'env de BASE du conteneur, pas des
# secrets injectés par `sops exec-env` dans uvicorn (entrypoint, ADR-0044). C'est la MÊME
# clé que le cron nocturne (crontab : API__TROUSSEAU__scheduler__KEY).
# Surchargeable dans les tests ; SRV_BASE = racine des homes d'instance (/srv en prod).
_ingestion_read_scheduler_key() {
    local slug="$1" home="${SRV_BASE:-/srv}/${1}"
    local secrets="${home}/providers/${slug}/secrets.env"
    SOPS_AGE_KEY_FILE="${home}/age.key" \
        sops decrypt --input-type dotenv --output-type dotenv "$secrets" 2>/dev/null \
        | sed -n 's/^API__TROUSSEAU__scheduler__KEY=//p' | head -1
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
    log_warn "Test ingestion : job ${job_id} toujours en cours après ${max} tentatives — timeout."
    return 1
}

# run_ingestion_test <slug>
# Lance le test ingestion (POST /ingestion/run mode=test), attend la complétion par poll,
# et logue le résultat. Retourne 0 si le job est completed, 1 sinon.
run_ingestion_test() {
    local slug="$1"
    local api_key
    # La clé d'appel vit dans secrets.env CHIFFRÉ (ADR-0044) sous le label "scheduler"
    # (ADR-0046 §4) — on la déchiffre sur le host (cf. _ingestion_read_scheduler_key).
    api_key=$(_ingestion_read_scheduler_key "$slug")
    [[ -n "$api_key" ]] || { log_err "Clé scheduler introuvable (API__TROUSSEAU__scheduler__KEY ; sops decrypt a échoué ? secrets.env/age.key absents ?)"; return 1; }

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

    log_info "Job ingestion démarré : ${job_id} — attente de la complétion…"
    if poll_ingestion_job "$slug" "$api_key" "$job_id"; then
        log_ok "Test ingestion réussi — chaîne SFTP→déchiffrement→DuckDB OK sur un échantillon (2 fichiers)."
        return 0
    else
        return 1
    fi
}

# show_ingestion_failure_hints <slug>
# Remonte un `ConfigurationManquante` éventuel (secret malformé, ADR-0049), puis les 50
# dernières lignes des logs ingestion-scheduler + les causes typiques.
show_ingestion_failure_hints() {
    local slug="$1"
    local home="/srv/${slug}"

    # Motif le plus probant en premier : si l'entrypoint a fail-fast sur un secret mal formé
    # (clé AES non-hex, URL SFTP sans schéma…), le conteneur a loggé `ConfigurationManquante`
    # (runtime.py, valider(...)) — on l'extrait des logs pour ne pas le noyer dans le dump brut.
    # C'est ici que surface désormais un secret invalide, le preflight bash ayant disparu (ADR-0049).
    local conf_err
    conf_err=$(sudo -u "$slug" -- bash -c \
        "cd '${home}/deploy/docker' && docker compose logs --tail=200 ingestion-scheduler" 2>/dev/null \
        | grep -iE 'configuration manquante|ConfigurationManquante' | tail -3)
    if [[ -n "$conf_err" ]]; then
        log_err "Secret malformé détecté par le conteneur (corrige secrets.env dans le dépôt) :"
        printf '%s\n' "$conf_err" | sed 's/^/     ! /'
        log_info ""
    fi

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
    log_info "  # La clé du scheduler vit dans secrets.env chiffré — la déchiffrer sur le host (sops) :"
    log_info "  API_KEY=\$(SOPS_AGE_KEY_FILE=${home}/age.key sops decrypt --input-type dotenv --output-type dotenv ${home}/providers/${slug}/secrets.env | sed -n 's/^API__TROUSSEAU__scheduler__KEY=//p' | head -1)"
    log_info "  sudo -u $slug docker compose -f ${home}/deploy/docker/docker-compose.yml \\"
    log_info "      exec -T ingestion-scheduler curl -X POST -H \"X-API-Key:\$API_KEY\" \\"
    log_info "      -H 'Content-Type: application/json' -d '{\"mode\":\"test\"}' \\"
    log_info "      http://api:8001/ingestion/run"
}
