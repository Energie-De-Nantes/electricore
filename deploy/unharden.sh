#!/usr/bin/env bash
# Réversion autonome du durcissement VPS (ADR-0031) — « uninstall ».
#
# Inverse de deploy/harden.sh : retire le drop-in sshd (SSH root rétabli), la
# jail fail2ban et la conf unattended-upgrades posés par le durcissement. Le user
# admin `ops` est CONSERVÉ par défaut (--purge-ops pour le supprimer aussi).
# Idempotent, sans hypothèse de layout (OS/SSH-level, comme harden.sh).
#
# Ne fait que sourcer lib/harden.sh et appeler unharden_vps() : toute la logique
# vit dans la lib (aucune duplication).
#
# Usage :
#   # Sur une instance au layout courant (arbre deploy/ présent) :
#   sudo bash /srv/<slug>/deploy/unharden.sh [--purge-ops]
#
#   # Standalone (legacy /opt, ou box sans notre arbre deploy/) — bootstrap auto :
#   curl -fsSL https://raw.githubusercontent.com/Energie-De-Nantes/electricore/main/deploy/unharden.sh -o unharden.sh
#   sudo bash unharden.sh [--purge-ops]
#
# Cf. docs/deploiement.md § « Durcissement du VPS » → « Réverser le durcissement ».

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_BASE_URL_DEFAULT="${INSTALL_BASE_URL:-https://raw.githubusercontent.com/Energie-De-Nantes/electricore/main/deploy}"
# Sous-ensemble de lib/ dont unharden_vps() a besoin (log + die ; harden).
HARDEN_LIB_FILES=(log harden)

# _lib_complete <dir> : vrai si <dir> contient tous les helpers de HARDEN_LIB_FILES.
_lib_complete() {
    local f
    [[ -d "$1" ]] || return 1
    for f in "${HARDEN_LIB_FILES[@]}"; do [[ -f "$1/${f}.sh" ]] || return 1; done
}

# Résolution des helpers : co-localisés si complets (arbre deploy/ présent),
# sinon copie FRAÎCHE en /tmp (run standalone via curl, OU lib/ figé d'un run
# antérieur). Jamais de lib/ persistant laissé à pourrir à côté du script.
HELPERS_DIR="${SCRIPT_DIR}/lib"
HELPERS_DIR_TEMP=""
if ! _lib_complete "$HELPERS_DIR"; then
    command -v curl >/dev/null || { echo "curl introuvable, install curl puis relance." >&2; exit 1; }
    HELPERS_DIR_TEMP="$(mktemp -d "${TMPDIR:-/tmp}/electricore-lib.XXXXXX")"
    HELPERS_DIR="$HELPERS_DIR_TEMP"
    trap '[[ -n "${HELPERS_DIR_TEMP:-}" ]] && rm -rf "$HELPERS_DIR_TEMP"' EXIT
    echo "→ Bootstrap : téléchargement des helpers (copie fraîche) dans ${HELPERS_DIR}…"
    for f in "${HARDEN_LIB_FILES[@]}"; do
        if ! curl -fsSL "${INSTALL_BASE_URL_DEFAULT}/lib/${f}.sh" -o "${HELPERS_DIR}/${f}.sh"; then
            echo "✗ Échec téléchargement ${f}.sh depuis ${INSTALL_BASE_URL_DEFAULT}/lib/" >&2
            exit 1
        fi
    done
fi

for f in "${HARDEN_LIB_FILES[@]}"; do
    # shellcheck source=/dev/null
    source "${HELPERS_DIR}/${f}.sh"
done

usage_unharden() {
    cat <<EOF
Usage: sudo bash unharden.sh [options]

Réverse le durcissement VPS (ADR-0031) : retire le drop-in sshd (SSH root
rétabli), la jail fail2ban et la conf unattended-upgrades. Idempotent.
Le user admin ops est conservé par défaut.

Options :
  --purge-ops    Supprime AUSSI le user admin ops (sudoers + compte + home)
  --no-color     Désactive les couleurs ANSI
  -h, --help     Affiche cette aide
EOF
}

# parse_unharden_args "$@" — remplit OPT_PURGE_OPS (lu par unharden_vps).
parse_unharden_args() {
    OPT_PURGE_OPS=0
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --purge-ops) OPT_PURGE_OPS=1; shift ;;
            --no-color)  export NO_COLOR=1; shift ;;
            -h|--help)   usage_unharden; exit 0 ;;
            *) echo "Argument inconnu : $1" >&2; usage_unharden; exit 2 ;;
        esac
    done
}

main_unharden() {
    set -euo pipefail
    parse_unharden_args "$@"
    LOG_TOTAL_STEPS=1
    [[ $EUID -eq 0 ]] || die "à lancer en root (sudo bash $0 ...)"
    log_step "Réversion du durcissement VPS (ADR-0031)"
    unharden_vps
    log_ok "Réversion terminée. SSH root rétabli (défaut image) — pense à remettre 'User root' dans ~/.ssh/config si besoin."
}

# Guard : n'exécute `main_unharden` que si le script est exécuté, pas sourcé
# (les tests unitaires sourcent pour récupérer parse_unharden_args).
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main_unharden "$@"
fi
