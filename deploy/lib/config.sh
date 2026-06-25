# shellcheck shell=bash
# Téléchargement et patch des fichiers de configuration de l'instance.
# Source: https://raw.githubusercontent.com/Energie-De-Nantes/electricore/<tag>/deploy/docker/...

CONFIG_BASE_URL="${CONFIG_BASE_URL:-https://raw.githubusercontent.com/Energie-De-Nantes/electricore}"

# map_version_to_git_ref <version>
# Convertit l'option `--version` (qui désigne un tag d'image Docker) vers le ref
# Git correspondant pour récupérer les configs depuis raw.githubusercontent :
#   latest                  → main          (alias Docker, pas un ref Git)
#   1.7.0, 1.8.0rc1, 2.0.0a1 → v<version>   (les tags Git portent un préfixe `v`)
#   main, dev, abc1234      → inchangé       (branche ou SHA)
map_version_to_git_ref() {
    case "$1" in
        latest)                 echo "main" ;;
        [0-9]*.[0-9]*.[0-9]*)   echo "v$1" ;;
        *)                      echo "$1" ;;
    esac
}

# download_config_files <version> <home_dir>
# Télécharge depuis le tag <version> vers <home_dir>/deploy/docker/*. En secrets-as-code
# (ADR-0044) on ne télécharge JAMAIS de .env : la config claire (config.env) et les secrets
# (secrets.env chiffré) arrivent par le dépôt de déploiement privé (providers/<slug>/).
download_config_files() {
    local version="$1"
    local home="$2"
    local docker_dir="${home}/deploy/docker"
    local ref; ref=$(map_version_to_git_ref "$version")
    local base="${CONFIG_BASE_URL}/${ref}/deploy/docker"
    install -d "$docker_dir"
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

# substitute_caddyfile <file> <domain> [<email>]
# Remplace le placeholder de domaine et optionnellement l'email.
substitute_caddyfile() {
    local file="$1"
    local domain="$2"
    local email="${3:-}"
    local owner; owner=$(stat -c '%u:%g' "$file" 2>/dev/null || echo "")
    sed -i "s|electricore\.exemple\.fr|${domain}|g" "$file"
    if [[ -n "$email" ]]; then
        sed -i "s|votre-email@example\.com|${email}|g" "$file"
    fi
    [[ -n "$owner" ]] && chown "$owner" "$file" 2>/dev/null || true
}
