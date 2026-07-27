#!/usr/bin/env bash
# Assertions e2e du composant relais de l'installeur (`install.sh --relais`, #657).
# Vérifie l'ÉTAT POSÉ (timer actif, compose présent, refus sans clé SSH) — jamais un
# push réel vers un partenaire (aucune invocation de `docker compose run` ici : le
# premier run réel appartient au timer, pas à l'installeur ni à ce script).
#
# Le harnais multipass l'invoque via `multipass exec` (pas SSH) :
#   ./deploy/tests/e2e/multipass.sh verify-relais refuse  <slug>   # avant `run --relais` (clé SSH absente)
#   ./deploy/tests/e2e/multipass.sh verify-relais posed   <slug>   # après un `run --relais` réussi
#
# Sur un vrai VPS :
#   sudo bash /srv/<slug>/deploy/tests/e2e/assert_relais.sh <refuse|posed> <slug>
#
# Exit 0 si toutes les assertions passent, 1 sinon ; exit 2 si usage invalide.
set -u

PASS=0; FAIL=0
ok() { printf '  \033[32m✓\033[0m %s\n' "$1"; PASS=$((PASS+1)); }
ko() { printf '  \033[31m✗\033[0m %s\n' "$1"; FAIL=$((FAIL+1)); }
check() { if eval "$2" >/dev/null 2>&1; then ok "$1"; else ko "$1"; fi; }

[[ $EUID -eq 0 ]] || { echo "à lancer en root (sudo bash $0 <refuse|posed> <slug>)" >&2; exit 2; }

MODE="${1:-}"
SLUG="${2:-}"
[[ -n "$MODE" && -n "$SLUG" ]] || {
    echo "Usage: $0 <refuse|posed> <slug>" >&2
    exit 2
}

HOME_DIR="/srv/${SLUG}"
RELAIS_DIR="${HOME_DIR}/deploy/relais"

case "$MODE" in
    refuse)
        echo "→ Composant relais #657 : install.sh --relais REFUSÉ (clé SSH partenaire absente/permissive)"
        check "mini-compose relais ABSENT (rien n'a été posé)" \
              "! test -f ${RELAIS_DIR}/compose-relais.yml"
        check "unité systemd du service relais ABSENTE" \
              "! test -f /etc/systemd/system/electricore-relais.service"
        check "timer electricore-relais.timer NI actif NI enabled" \
              "! systemctl is-active electricore-relais.timer && ! systemctl is-enabled electricore-relais.timer"
        check "unité d'alerte OnFailure= ABSENTE (#668 : refus AVANT install_relais_alerte_units)" \
              "! test -f /etc/systemd/system/electricore-relais-alerte.service"
        check "script d'alerte ABSENT (#668)" \
              "! test -f /usr/local/bin/electricore-relais-alerte.sh"
        ;;
    posed)
        echo "→ Composant relais #657 : install.sh --relais POSÉ (socle + compose + timer)"
        check "mini-compose relais présent"                        "test -f ${RELAIS_DIR}/compose-relais.yml"
        check "compose relais tag-pinné par RELAIS_VERSION (pas ELECTRICORE_VERSION)" \
              "grep -q 'RELAIS_VERSION' ${RELAIS_DIR}/compose-relais.yml"
        check "unité service systemd présente"                     "test -f /etc/systemd/system/electricore-relais.service"
        check "unité service : ExecStart appelle docker compose run --rm" \
              "grep -q 'docker compose .* run --rm relais' /etc/systemd/system/electricore-relais.service"
        check "unité timer systemd présente"                       "test -f /etc/systemd/system/electricore-relais.timer"
        check "timer electricore-relais.timer actif"                "systemctl is-active electricore-relais.timer"
        check "timer electricore-relais.timer enabled (survit à un reboot)" \
              "systemctl is-enabled electricore-relais.timer"
        check "clé SSH partenaire présente en 600"                 "test -f ${HOME_DIR}/relais_ssh_key"
        check "clé SSH partenaire : droits 600 exactement"         "[ \"\$(stat -c '%a' ${HOME_DIR}/relais_ssh_key)\" = 600 ]"

        # ─── Alerte mail OnFailure= (#659, câblée sur ce layout par #668) ────────────
        echo "→ Composant relais #668 : alerte mail OnFailure= câblée (script + unité posés, PAS activée)"
        check "unité service relais : OnFailure=electricore-relais-alerte.service" \
              "grep -qx 'OnFailure=electricore-relais-alerte.service' /etc/systemd/system/electricore-relais.service"
        check "script d'alerte posé et exécutable (/usr/local/bin)" \
              "test -x /usr/local/bin/electricore-relais-alerte.sh"
        check "script d'alerte : aucun résidu /etc/electricore-relais/relais.env" \
              "! grep -q '/etc/electricore-relais/relais.env' /usr/local/bin/electricore-relais-alerte.sh"
        check "unité d'alerte systemd présente"                    "test -f /etc/systemd/system/electricore-relais-alerte.service"
        check "unité d'alerte : EnvironmentFile=${HOME_DIR}/config.env (layout #657, pas relais.env)" \
              "grep -qx 'EnvironmentFile=${HOME_DIR}/config.env' /etc/systemd/system/electricore-relais-alerte.service"
        check "unité d'alerte NON activée (jamais démarrée par l'installeur, déclenchée par OnFailure= seul)" \
              "! systemctl is-active electricore-relais-alerte.service"
        check "unité d'alerte SANS [Install] (pas de enable --now — is-enabled répondrait 'static', pas un signal utile)" \
              "! grep -qx '\[Install\]' /etc/systemd/system/electricore-relais-alerte.service"
        if command -v systemd-analyze >/dev/null 2>&1; then
            check "systemd-analyze verify : electricore-relais.service (OnFailure= compris)" \
                  "systemd-analyze verify /etc/systemd/system/electricore-relais.service"
            check "systemd-analyze verify : electricore-relais-alerte.service" \
                  "systemd-analyze verify /etc/systemd/system/electricore-relais-alerte.service"
        else
            echo "  (systemd-analyze indisponible sur cette VM — rendu déjà couvert par le runner bash unitaire, #668)"
        fi
        # Pas de domaine ni de Caddy pour le composant relais seul (#657 AC2) : sur une
        # box où seul --relais a tourné (jamais la stack), le chemin relais n'a jamais
        # téléchargé/substitué de Caddyfile.
        check "pas de Caddyfile posé par le chemin relais (composant relais seul)" \
              "! test -f ${HOME_DIR}/deploy/docker/Caddyfile"
        # Socle commun durci (partagé avec la stack, #656/#657) : mêmes garanties.
        check "durcissement VPS actif (drop-in sshd posé, socle commun)" \
              "test -f /etc/ssh/sshd_config.d/50-electricore-harden.conf"
        ;;
    *)
        echo "mode inconnu : '${MODE}' (attendu refuse|posed)" >&2
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
