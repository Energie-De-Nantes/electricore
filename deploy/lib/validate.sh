# shellcheck shell=bash
# Validation des ARGUMENTS CLI de install.sh (slug, domaine). Pures : 0 si valide,
# 1 sinon, aucun side-effect — utiliser via `if validate_X "$v"; then`.
#
# Le FORMAT des secrets (clés AES/API hex, URL SFTP) n'est plus validé ici : c'est
# désormais la SSOT du registre pydantic (electricore/config/runtime.py, field_validators),
# vérifié par le vrai conteneur aux étapes 11-12 d'install.sh (ADR-0049).

validate_slug() {
    local v="${1:-}"
    [[ "$v" =~ ^[a-z0-9-]+$ ]] && (( ${#v} >= 2 )) && (( ${#v} <= 32 ))
}

validate_domain() {
    [[ "${1:-}" =~ ^([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,}$ ]]
}
