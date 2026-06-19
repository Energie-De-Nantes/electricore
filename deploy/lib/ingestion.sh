# shellcheck shell=bash
# Lancement d'un ETL test (mode test = échantillon de 2 fichiers/flux) pour vérifier
# que la chaîne SFTP/file → déchiffrement AES → DuckDB répond. ATTENTION : l'échantillon
# n'est pas trié par date → il ne valide PAS la couverture du trousseau AES (une clé
# courante manquante peut passer inaperçue). Validation réelle = resync (cf. install.sh).

# run_ingestion_test <slug>
# Appelle POST /ingestion/run mode=test via le scheduler. Renvoie 0 si succès,
# 1 sinon (et imprime des indications sur les causes typiques).
run_ingestion_test() {
    local slug="$1"
    local home="/srv/${slug}"
    # On lit l'API_KEY depuis le .env pour authentifier l'appel.
    local api_key
    api_key=$(read_env_var "${home}/.env" API_KEY)
    [[ -n "$api_key" ]] || { log_err "API_KEY introuvable dans ${home}/.env"; return 1; }

    sudo -u "$slug" -- bash -c \
        "cd '${home}/deploy/docker' && \
         docker compose exec -T ingestion-scheduler curl -fsS \
             --max-time 120 \
             -X POST -H 'X-API-Key:${api_key}' -H 'Content-Type: application/json' \
             -d '{\"mode\":\"test\"}' http://api:8001/ingestion/run" \
        >/dev/null 2>&1
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
