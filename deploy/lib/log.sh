# shellcheck shell=bash
# Helpers de sortie pour install.sh. Couleurs ANSI désactivables via NO_COLOR.

if [[ -t 1 ]] && [[ -z "${NO_COLOR:-}" ]]; then
    _C_RESET=$'\033[0m'; _C_RED=$'\033[31m'; _C_GREEN=$'\033[32m'
    _C_YELLOW=$'\033[33m'; _C_BLUE=$'\033[34m'; _C_BOLD=$'\033[1m'
else
    _C_RESET=''; _C_RED=''; _C_GREEN=''; _C_YELLOW=''; _C_BLUE=''; _C_BOLD=''
fi

_STEP_NUM=0
_TS_START=$(date +%s)

_elapsed() { printf '%ds' "$(( $(date +%s) - _TS_START ))"; }

log_step() {
    _STEP_NUM=$((_STEP_NUM + 1))
    printf '\n%s━━ [%d/%d] %s%s  %s(%s)%s\n' \
        "$_C_BOLD" "$_STEP_NUM" "${LOG_TOTAL_STEPS:-9}" "$1" "$_C_RESET" \
        "$_C_BLUE" "$(_elapsed)" "$_C_RESET"
}
log_info() { printf '   %s\n' "$1"; }
log_ok()   { printf '   %s✓%s %s\n' "$_C_GREEN" "$_C_RESET" "$1"; }
log_warn() { printf '   %s⚠%s %s\n' "$_C_YELLOW" "$_C_RESET" "$1" >&2; }
log_err()  { printf '   %s✗%s %s\n' "$_C_RED" "$_C_RESET" "$1" >&2; }
log_skip() { printf '   %s↷%s %s\n' "$_C_BLUE" "$_C_RESET" "$1"; }

die() {
    log_err "$1"
    [[ $# -gt 1 ]] && log_info "$2"
    exit "${EXIT_CODE:-1}"
}
