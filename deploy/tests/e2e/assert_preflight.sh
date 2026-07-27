#!/usr/bin/env bash
# Assertions e2e du préflight sshd non-vierge (#656) — rejoue le cas Enargia : un
# compte SFTP au mot de passe (sans clé, sans bloc Match) doit bloquer le
# durcissement AVANT tout changement ; une fois remédié (clé migrée ou exception
# Match posée), le même durcissement doit passer.
#
# Le harnais multipass l'invoque via `multipass exec` (pas SSH) :
#   ./deploy/tests/e2e/multipass.sh verify-preflight refuse            <user>   # après un `harden` en échec
#   ./deploy/tests/e2e/multipass.sh verify-preflight pass              <user>   # après remédiation + `harden` réussi
#   ./deploy/tests/e2e/multipass.sh verify-preflight already-hardened  <user>   # reconfigure sans remédiation (finding 3, #656)
#
# Sur un vrai VPS :
#   sudo bash /srv/<slug>/deploy/tests/e2e/assert_preflight.sh <refuse|pass|already-hardened> <user>
#
# Exit 0 si toutes les assertions passent, 1 sinon ; exit 2 si usage invalide.
set -u

DROPIN="${SSHD_HARDEN_DROPIN:-/etc/ssh/sshd_config.d/50-electricore-harden.conf}"
PASS=0; FAIL=0
ok() { printf '  \033[32m✓\033[0m %s\n' "$1"; PASS=$((PASS+1)); }
ko() { printf '  \033[31m✗\033[0m %s\n' "$1"; FAIL=$((FAIL+1)); }
check() { if eval "$2" >/dev/null 2>&1; then ok "$1"; else ko "$1"; fi; }

[[ $EUID -eq 0 ]] || { echo "à lancer en root (sudo bash $0 <refuse|pass> <user>)" >&2; exit 2; }

MODE="${1:-}"
USER_AT_RISK="${2:-}"
[[ -n "$MODE" && -n "$USER_AT_RISK" ]] || {
    echo "Usage: $0 <refuse|pass> <user>" >&2
    exit 2
}

case "$MODE" in
    refuse)
        echo "→ Préflight #656 : durcissement REFUSÉ (${USER_AT_RISK} au mot de passe, sans clé, sans Match)"
        check "drop-in de durcissement ABSENT (rien n'a été posé)" \
              "! test -f ${DROPIN}"
        check "${USER_AT_RISK} toujours authentifiable par mot de passe (passwd -S = P)" \
              "passwd -S ${USER_AT_RISK} | awk '{print \$2}' | grep -qx P"
        check "${USER_AT_RISK} n'a toujours pas de clé SSH exploitable" \
              "! test -s \"\$(getent passwd ${USER_AT_RISK} | cut -d: -f6)/.ssh/authorized_keys\""
        ;;
    pass)
        echo "→ Préflight #656 : durcissement PASSE après remédiation (${USER_AT_RISK})"
        check "drop-in de durcissement présent"                    "test -f ${DROPIN}"
        check "sshd -t valide la config"                           "sshd -t"
        check "PasswordAuthentication no globalement (effectif)"   "sshd -T | grep -qix 'passwordauthentication no'"
        check "${USER_AT_RISK} a maintenant une clé SSH exploitable" \
              "test -s \"\$(getent passwd ${USER_AT_RISK} | cut -d: -f6)/.ssh/authorized_keys\""
        ;;
    already-hardened)
        echo "→ Préflight #656 (finding 3) : reconfigure sur box DÉJÀ durcie passe SANS remédiation (${USER_AT_RISK})"
        check "drop-in de durcissement présent (déjà posé avant ce reconfigure)" \
              "test -f ${DROPIN}"
        check "sshd -t valide la config"                           "sshd -t"
        check "PasswordAuthentication no globalement (effectif)"   "sshd -T | grep -qix 'passwordauthentication no'"
        check "${USER_AT_RISK} n'a TOUJOURS PAS de clé (jamais remédié — la box était déjà durcie, avant=no)" \
              "! test -s \"\$(getent passwd ${USER_AT_RISK} | cut -d: -f6)/.ssh/authorized_keys\""
        ;;
    *)
        echo "mode inconnu : '${MODE}' (attendu refuse|pass|already-hardened)" >&2
        exit 2
        ;;
esac

echo
if [[ "$FAIL" -eq 0 ]]; then
    printf "\033[32m%d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
    exit 0
else
    printf "\033[31m%d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
    exit 1
fi
