# shellcheck shell=bash
# Détection OS pour le script d'install. Mockable via OS_RELEASE_PATH (tests).

detect_os() {
    local path="${OS_RELEASE_PATH:-/etc/os-release}"
    [[ -r "$path" ]] || { echo "unknown"; return 1; }
    local id version
    id=$(awk -F= '$1=="ID"{gsub(/"/,"",$2); print $2}' "$path")
    version=$(awk -F= '$1=="VERSION_ID"{gsub(/"/,"",$2); print $2}' "$path")
    echo "${id}-${version}"
}

is_supported_os() {
    local os
    os=$(detect_os) || return 1
    case "$os" in
        ubuntu-22.04|ubuntu-24.04|ubuntu-25.04|debian-12|debian-13) return 0 ;;
        *) return 1 ;;
    esac
}
