#!/usr/bin/env bash
# Durcissement autonome d'un VPS déjà provisionné (ADR-0031).
#
# Rétro-durcit une instance existante — y compris l'ancien layout root-run
# /opt/electricore/ — sans relancer un reconfigure complet. Le durcissement est
# OS/SSH-level : aucune hypothèse de layout. La clé de `ops` est amorcée depuis
# ~root/.ssh/authorized_keys quel que soit l'emplacement de la stack
# (/srv/<slug>/ comme /opt/electricore/).
#
# Ce script ne fait que sourcer lib/harden.sh et appeler harden_vps() : toute la
# logique vit dans la lib, partagée avec install.sh (pas de duplication).
#
# Usage :
#   # Sur une instance au layout courant (arbre deploy/ présent) :
#   sudo bash /srv/<slug>/deploy/harden.sh [options]
#
#   # Standalone (legacy /opt, ou box sans notre arbre deploy/) — bootstrap auto :
#   curl -fsSL https://raw.githubusercontent.com/Energie-De-Nantes/electricore/main/deploy/harden.sh -o harden.sh
#   sudo bash harden.sh [options]
#
# Cf. docs/deploiement.md § « Durcissement du VPS » → « Rétro-durcir un VPS existant ».

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_BASE_URL_DEFAULT="${INSTALL_BASE_URL:-https://raw.githubusercontent.com/Energie-De-Nantes/electricore/main/deploy}"
# Sous-ensemble de lib/ dont harden_vps() a besoin (log + die ; ensure_packages).
HARDEN_LIB_FILES=(log system harden)

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

usage_harden() {
    cat <<EOF
Usage: sudo bash harden.sh [options]

Rétro-durcit un VPS déjà déployé (ADR-0031) : utilisateur admin ops + sudo,
sshd root-off clé-uniquement, fail2ban, unattended-upgrades. Idempotent.
Aucune hypothèse de layout : fonctionne sur /srv/<slug>/ comme /opt/electricore/.

Options :
  --admin-pubkey <key>      Clé SSH publique pour ops (défaut: copie ~root)
  --no-sshd                 Ne verrouille pas sshd (garde root SSH actif)
  --no-fail2ban             Saute fail2ban
  --no-unattended-upgrades  Saute les mises à jour automatiques
  --no-color                Désactive les couleurs ANSI
  -h, --help                Affiche cette aide
EOF
}

# parse_harden_args "$@" — remplit OPT_ADMIN_PUBKEY, OPT_NO_SSHD, OPT_NO_FAIL2BAN,
# OPT_NO_UNATTENDED (noms partagés avec cli.sh → harden_vps les lit indifféremment).
parse_harden_args() {
    OPT_ADMIN_PUBKEY=""
    OPT_NO_SSHD=0
    OPT_NO_FAIL2BAN=0
    OPT_NO_UNATTENDED=0
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --admin-pubkey)           OPT_ADMIN_PUBKEY="${2:-}"; shift 2 ;;
            --no-sshd)                OPT_NO_SSHD=1; shift ;;
            --no-fail2ban)            OPT_NO_FAIL2BAN=1; shift ;;
            --no-unattended-upgrades) OPT_NO_UNATTENDED=1; shift ;;
            --no-color)               export NO_COLOR=1; shift ;;
            -h|--help)                usage_harden; exit 0 ;;
            *) echo "Argument inconnu : $1" >&2; usage_harden; exit 2 ;;
        esac
    done
}

main_harden() {
    set -euo pipefail
    parse_harden_args "$@"
    LOG_TOTAL_STEPS=1
    [[ $EUID -eq 0 ]] || die "à lancer en root (sudo bash $0 ...)"
    log_step "Durcissement VPS (rétro, ADR-0031)"
    harden_vps
    log_ok "Durcissement terminé. Prochaine connexion admin : ssh ops@<vps> (root SSH coupé sauf --no-sshd)."
}

# Guard : n'exécute `main_harden` que si le script est exécuté, pas sourcé
# (les tests unitaires sourcent pour récupérer parse_harden_args).
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main_harden "$@"
fi
