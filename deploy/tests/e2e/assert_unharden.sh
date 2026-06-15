#!/usr/bin/env bash
# Assertions e2e de la RÉVERSION du durcissement (ADR-0031), à lancer DANS la VM
# (ou sur un vrai VPS) après `unharden.sh`. Vérifie qu'on est revenu au défaut :
# drop-in sshd retiré (root SSH rétabli), jail fail2ban et conf unattended retirées.
# Par défaut le user ops est conservé (lancer sans --purge-ops).
#
#   ./deploy/tests/e2e/multipass.sh verify-reverse
#
# Exit 0 si toutes les assertions passent, 1 sinon.
set -u

ADMIN_USER="${HARDEN_ADMIN_USER:-ops}"
PASS=0; FAIL=0
ok() { printf '  \033[32m✓\033[0m %s\n' "$1"; PASS=$((PASS+1)); }
ko() { printf '  \033[31m✗\033[0m %s\n' "$1"; FAIL=$((FAIL+1)); }
check() { if eval "$2" >/dev/null 2>&1; then ok "$1"; else ko "$1"; fi; }

[[ $EUID -eq 0 ]] || { echo "à lancer en root (sudo bash $0)" >&2; exit 2; }

echo "→ Réversion : sshd (accès root rétabli)"
check "drop-in de durcissement retiré"           "! test -f /etc/ssh/sshd_config.d/50-electricore-harden.conf"
check "sshd -t valide la config"                 "sshd -t"
check "PermitRootLogin n'est plus 'no' (root SSH rétabli)" "! sshd -T | grep -qix 'permitrootlogin no'"

echo
echo "→ Réversion : fail2ban + unattended-upgrades"
check "jail fail2ban ElectriCore retirée"        "! test -f /etc/fail2ban/jail.d/electricore.conf"
check "override auto-reboot retiré"              "! test -f /etc/apt/apt.conf.d/52electricore-unattended"
check "periodic unattended retiré"               "! test -f /etc/apt/apt.conf.d/20auto-upgrades"

echo
echo "→ Réversion : user admin (conservé par défaut, sans --purge-ops)"
check "${ADMIN_USER} toujours présent"           "id ${ADMIN_USER}"

echo
if [[ "$FAIL" -eq 0 ]]; then
    printf "\033[32m%d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
    exit 0
else
    printf "\033[31m%d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
    exit 1
fi
