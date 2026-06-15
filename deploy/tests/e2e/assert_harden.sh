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
echo "→ Durcissement #259 : verrouillage sshd (config effective via sshd -T)"
check "drop-in présent"                          "test -f /etc/ssh/sshd_config.d/50-electricore-harden.conf"
check "sshd -t valide la config"                 "sshd -t"
check "PermitRootLogin no (effectif)"            "sshd -T | grep -qix 'permitrootlogin no'"
check "PasswordAuthentication no (effectif)"     "sshd -T | grep -qix 'passwordauthentication no'"
check "KbdInteractiveAuthentication no (effectif)" "sshd -T | grep -qix 'kbdinteractiveauthentication no'"
check "PubkeyAuthentication yes (effectif)"       "sshd -T | grep -qix 'pubkeyauthentication yes'"
check "X11Forwarding no (effectif)"               "sshd -T | grep -qix 'x11forwarding no'"
check "MaxAuthTries 3 (effectif)"                 "sshd -T | grep -qix 'maxauthtries 3'"
check "service ssh actif (reload, pas restart)"   "systemctl is-active ssh || systemctl is-active sshd"

echo
echo "→ Durcissement #260 : fail2ban (jail sshd, backend systemd)"
check "fail2ban installé"                        "command -v fail2ban-client"
check "service fail2ban actif"                   "systemctl is-active fail2ban"
check "conf jail présente"                       "test -f /etc/fail2ban/jail.d/electricore.conf"
check "backend=systemd dans la conf"             "grep -q 'backend  = systemd' /etc/fail2ban/jail.d/electricore.conf"
check "jail sshd présent (fail2ban-client)"      "fail2ban-client status sshd"

echo
echo "→ Durcissement #261 : unattended-upgrades + auto-reboot 04:30"
check "paquet unattended-upgrades installé"      "dpkg -s unattended-upgrades"
check "20auto-upgrades active l'unattended"      "grep -q 'APT::Periodic::Unattended-Upgrade \"1\"' /etc/apt/apt.conf.d/20auto-upgrades"
check "origine sécurité activée (50unattended)"  "grep -iE '^[[:space:]]*[^/[:space:]].*security' /etc/apt/apt.conf.d/50unattended-upgrades"
check "Automatic-Reboot true"                    "grep -q 'Automatic-Reboot \"true\"' /etc/apt/apt.conf.d/52electricore-unattended"
check "Automatic-Reboot-Time 04:30"              "grep -q 'Automatic-Reboot-Time \"04:30\"' /etc/apt/apt.conf.d/52electricore-unattended"
check "timer apt-daily-upgrade activé"           "systemctl is-enabled apt-daily-upgrade.timer"

echo
if [[ "$FAIL" -eq 0 ]]; then
    printf "\033[32m%d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
    exit 0
else
    printf "\033[31m%d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
    exit 1
fi
