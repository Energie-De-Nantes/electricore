# shellcheck shell=bash
# Téléchargement et patch des fichiers de configuration de l'instance.
# Source: https://raw.githubusercontent.com/Energie-De-Nantes/electricore/<tag>/deploy/docker/...

CONFIG_BASE_URL="${CONFIG_BASE_URL:-https://raw.githubusercontent.com/Energie-De-Nantes/electricore}"

# download_config_files <version> <home_dir>
# Télécharge depuis le tag <version> vers <home_dir>/{.env,deploy/docker/*}.
# Idempotent : si .env existe déjà, on ne l'écrase pas (mode reconfigure).
download_config_files() {
    local version="$1"
    local home="$2"
    local docker_dir="${home}/deploy/docker"
    # `latest` est un alias Docker mais pas un ref Git → bascule vers `main`
    # pour récupérer les configs depuis raw.githubusercontent.
    local ref="$version"
    [[ "$ref" == "latest" ]] && ref="main"
    local base="${CONFIG_BASE_URL}/${ref}/deploy/docker"
    install -d "$docker_dir"
    # .env : ne pas écraser si présent (rerun)
    if [[ ! -f "${home}/.env" ]]; then
        curl -fsSL "${base}/.env.example" -o "${home}/.env"
        log_ok ".env téléchargé depuis ${version}"
    else
        log_skip ".env déjà présent (conservé)"
    fi
    for f in docker-compose.yml Caddyfile.example crontab.example backup_duckdb.sh; do
        local dest="${docker_dir}/${f}"
        local dest_final="${dest}"
        # On utilise des .example pour Caddyfile/crontab dans le repo, mais les noms
        # définitifs côté instance sont sans .example
        case "$f" in
            Caddyfile.example) dest_final="${docker_dir}/Caddyfile" ;;
            crontab.example)   dest_final="${docker_dir}/crontab"   ;;
        esac
        curl -fsSL "${base}/${f}" -o "$dest_final"
    done
    chmod +x "${docker_dir}/backup_duckdb.sh"
    log_ok "fichiers compose téléchargés dans ${docker_dir}/"
}

# substitute_env <env_file> <slug>
# Remplace INSTANCE_SLUG= et BACKUPS_PATH= par les valeurs réelles.
# Idempotent : peut être appelé plusieurs fois sans dégrader.
substitute_env() {
    local env_file="$1"
    local slug="$2"
    sed -i \
        -e "s|^INSTANCE_SLUG=.*|INSTANCE_SLUG=${slug}|" \
        -e "s|^BACKUPS_PATH=.*|BACKUPS_PATH=/srv/${slug}/backups|" \
        "$env_file"
}

# substitute_caddyfile <file> <domain> [<email>]
# Remplace le placeholder de domaine et optionnellement l'email.
substitute_caddyfile() {
    local file="$1"
    local domain="$2"
    local email="${3:-}"
    sed -i "s|electricore\.exemple\.fr|${domain}|g" "$file"
    if [[ -n "$email" ]]; then
        sed -i "s|votre-email@example\.com|${email}|g" "$file"
    fi
}
