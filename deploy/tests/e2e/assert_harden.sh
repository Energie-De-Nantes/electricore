#!/usr/bin/env bash
# Assertions e2e du durcissement VPS (ADR-0031). Conçu pour tourner DANS la VM
# (ou sur un vrai VPS), en root, après un `install.sh` sans --no-harden.
#
# Le harnais multipass l'invoque via `multipass exec` (pas SSH), donc couper le
# SSH root ne casse pas ce script :
#   ./deploy/tests/e2e/multipass.sh verify
#
# Sur un vrai VPS :
#   sudo bash /srv/<slug>/deploy/tests/e2e/assert_harden.sh
#
# Exit 0 si toutes les assertions passent, 1 sinon.
set -u

ADMIN_USER="${HARDEN_ADMIN_USER:-ops}"
PASS=0; FAIL=0
ok() { printf '  \033[32m✓\033[0m %s\n' "$1"; PASS=$((PASS+1)); }
ko() { printf '  \033[31m✗\033[0m %s\n' "$1"; FAIL=$((FAIL+1)); }
check() { if eval "$2" >/dev/null 2>&1; then ok "$1"; else ko "$1"; fi; }

[[ $EUID -eq 0 ]] || { echo "à lancer en root (sudo bash $0)" >&2; exit 2; }

echo "→ Durcissement #258 : utilisateur admin ${ADMIN_USER}"
check "user ${ADMIN_USER} existe"                "id ${ADMIN_USER}"
check "${ADMIN_USER} a une clé authorized_keys"  "test -s \"\$(getent passwd ${ADMIN_USER} | cut -d: -f6)/.ssh/authorized_keys\""
check "/etc/sudoers.d/${ADMIN_USER} présent"     "test -f /etc/sudoers.d/${ADMIN_USER}"
check "règle sudoers valide (visudo -c)"         "visudo -cf /etc/sudoers.d/${ADMIN_USER}"
check "sudo NOPASSWD effectif pour ${ADMIN_USER}" "sudo -u ${ADMIN_USER} sudo -n true"
check "${ADMIN_USER} n'est PAS dans le groupe docker (rôle admin ≠ service)" \
      "! id -nG ${ADMIN_USER} | tr ' ' '\\n' | grep -qx docker"

echo
if [[ "$FAIL" -eq 0 ]]; then
    printf "\033[32m%d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
    exit 0
else
    printf "\033[31m%d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
    exit 1
fi
