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

# override_config_version <config_file> <version>
# Réécrit UNIQUEMENT ELECTRICORE_VERSION dans <config_file> (config.env), en laissant
# tout le reste intact (INSTANCE_SLUG, BACKUPS_PATH, ODOO_ENV…). C'est l'override LOCAL
# du tag d'image en secrets-as-code (#460) : `--version` pilote l'image effectivement
# déployée sans muter la baseline GitOps tirée du dépôt — la version est un PARAMÈTRE
# de déploiement, pas un secret (ADR-0044). À distinguer du `substitute_caddyfile`
# (patch de templates) : ici on réécrit une seule clé d'un fichier déjà validé.
# Préserve l'ownership du fichier (sed -i peut le casser).
# Pré-condition : ELECTRICORE_VERSION est présent (garanti par validate_config_env).
override_config_version() {
    local config_file="$1"
    local version="$2"
    local owner; owner=$(stat -c '%u:%g' "$config_file" 2>/dev/null || echo "")
    sed -i "s|^ELECTRICORE_VERSION=.*|ELECTRICORE_VERSION=${version}|" "$config_file"
    [[ -n "$owner" ]] && chown "$owner" "$config_file" 2>/dev/null || true
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
