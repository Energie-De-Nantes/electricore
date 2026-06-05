# shellcheck shell=bash
# Validateurs purs sourcés par install.sh. Renvoient 0 si valide, 1 sinon.
# Aucun side-effect, aucune sortie : utiliser via `if validate_X "$v"; then`.

validate_slug() {
    local v="${1:-}"
    [[ "$v" =~ ^[a-z0-9-]+$ ]] && (( ${#v} >= 2 )) && (( ${#v} <= 32 ))
}

validate_aes_key() {
    local v="${1:-}"
    [[ "$v" =~ ^[0-9a-fA-F]{32}$ ]] || [[ "$v" =~ ^[0-9a-fA-F]{64}$ ]]
}

validate_aes_iv() { validate_aes_key "$@"; }

validate_api_key() {
    local v="${1:-}"
    [[ -n "$v" ]] && (( ${#v} >= 32 ))
}

validate_url() {
    [[ "${1:-}" =~ ^(https?|sftp|file):// ]]
}

validate_email() {
    [[ "${1:-}" =~ ^[^@[:space:]]+@[^@[:space:]]+\.[^@[:space:]]+$ ]]
}

validate_domain() {
    [[ "${1:-}" =~ ^([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,}$ ]]
}
