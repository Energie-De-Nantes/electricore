#!/usr/bin/env bash
# Assertions e2e du composant relais de l'installeur (`install.sh --relais`, #657).
# Vérifie l'ÉTAT POSÉ (unités présentes, timer PAS armé sur état vierge #673, refus
# sans clé SSH) — jamais un push réel vers un partenaire (aucune invocation de
# `docker compose run` ici : le premier run réel appartient au timer, pas à
# l'installeur ni à ce script).
#
# Le harnais multipass l'invoque via `multipass exec` (pas SSH) :
#   ./deploy/tests/e2e/multipass.sh verify-relais refuse    <slug>   # avant `run --relais` (clé SSH absente)
#   ./deploy/tests/e2e/multipass.sh verify-relais posed     <slug>   # après un `run --relais` réussi
#   ./deploy/tests/e2e/multipass.sh verify-relais onfailure <slug>   # échec forcé → l'alerte tire (stub msmtp, #668)
#
# Sur un vrai VPS :
#   sudo bash /srv/<slug>/deploy/tests/e2e/assert_relais.sh <refuse|posed|onfailure> <slug>
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
    echo "Usage: $0 <refuse|posed|onfailure> <slug>" >&2
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
        # Garde #673 : la VM e2e n'a jamais été amorcée (seed = geste d'opérateur,
        # jamais joué par l'installeur ni par ce harnais) → état vierge → le timer
        # doit être posé mais JAMAIS armé (ni actif ni enabled : un reboot ne doit
        # pas le démarrer).
        check "timer PAS armé sur état vierge (#673 : ni actif ni enabled)" \
              "! systemctl is-active electricore-relais.timer && ! systemctl is-enabled electricore-relais.timer"
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

        # ─── msmtprc RENDU (#674) : plus aucune étape manuelle ───────────────────────
        echo "→ Composant relais #674 : msmtprc rendu par l'installeur (token jamais en clair)"
        check "msmtprc posé par l'installeur (RENDU, plus une édition manuelle)" \
              "test -f /etc/electricore-relais/msmtprc"
        check "msmtprc : droits 600 exactement" \
              "[ \"\$(stat -c '%a' /etc/electricore-relais/msmtprc)\" = 600 ]"
        check "msmtprc : owned root (AC1 #674 — 600 ET root, l'unité d'alerte tourne root)" \
              "[ \"\$(stat -c '%U' /etc/electricore-relais/msmtprc)\" = root ]"
        check "msmtprc : passwordeval extrait le token de secrets.env (sops hôte, jamais en clair)" \
              "grep -q '^passwordeval' /etc/electricore-relais/msmtprc && grep -q 'sops decrypt' /etc/electricore-relais/msmtprc"
        check "msmtprc : aucune directive password= en clair (fuite du token)" \
              "! grep -qE '^[[:space:]]*password[[:space:]]' /etc/electricore-relais/msmtprc"
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
    onfailure)
        # Échec forcé → OnFailure= déclenche l'alerte (AC3 de #668, finding 2 revue #669).
        # Prouve la MÉCANIQUE (unité déclenchée, script exécuté jusqu'à msmtp) avec un
        # stub — le mail réel reste le drill de la générale #661. Réservé à une VM/box
        # de test : écarte temporairement le compose pour un échec déterministe
        # (indépendant de l'état de l'image et du réseau), puis restaure tout.
        echo "→ Composant relais #668 : échec forcé du service → l'alerte OnFailure= tire (stub msmtp)"
        CONFIG_ENV="${HOME_DIR}/config.env"
        COMPOSE="${RELAIS_DIR}/compose-relais.yml"
        [[ -f "$COMPOSE" && -f "$CONFIG_ENV" ]] || {
            echo "état posé requis (lancer verify-relais posed d'abord)" >&2
            exit 2
        }
        MARKER="$(mktemp /tmp/relais-alerte-drill.XXXXXX)"

        # Stub msmtp : /usr/local/bin précède /usr/bin dans le PATH systemd → capture
        # les destinataires au lieu d'envoyer (le vrai msmtp de /usr/bin reste intact).
        cat > /usr/local/bin/msmtp <<STUB
#!/usr/bin/env bash
printf '%s\n' "\$@" >> "${MARKER}"
cat > /dev/null
STUB
        chmod +x /usr/local/bin/msmtp
        # Destinataire de test si le provider n'en déclare pas (retiré à la fin).
        mails_injectes=0
        grep -q '^RELAIS_ALERTE_MAILS=' "$CONFIG_ENV" || {
            echo 'RELAIS_ALERTE_MAILS=drill@test.local' >> "$CONFIG_ENV"
            mails_injectes=1
        }

        mv "$COMPOSE" "${COMPOSE}.drill"
        if systemctl start electricore-relais.service >/dev/null 2>&1; then
            ko "le start aurait dû échouer (compose écarté) — échec forcé impossible"
        else
            ok "electricore-relais.service en échec forcé (compose écarté)"
        fi
        # OnFailure= démarre l'unité d'alerte en asynchrone : on lui laisse jusqu'à 15 s.
        for _ in $(seq 1 30); do [[ -s "$MARKER" ]] && break; sleep 0.5; done
        [[ -s "$MARKER" ]] && ok "OnFailure= a déclenché l'alerte jusqu'à msmtp (stub appelé)" \
                           || ko "l'alerte n'est jamais arrivée à msmtp (marqueur vide après 15 s)"
        grep -q '@' "$MARKER" 2>/dev/null && ok "msmtp a reçu au moins un destinataire" \
                                          || ko "aucun destinataire transmis à msmtp"
        check "l'unité d'alerte a terminé avec succès (ExecMainStatus=0)" \
              "[ \"\$(systemctl show electricore-relais-alerte.service -p ExecMainStatus --value)\" = 0 ]"

        # Remise en état : rien ne doit rester du drill.
        mv "${COMPOSE}.drill" "$COMPOSE"
        rm -f /usr/local/bin/msmtp "$MARKER"
        [[ "$mails_injectes" -eq 1 ]] && sed -i '/^RELAIS_ALERTE_MAILS=drill@test\.local$/d' "$CONFIG_ENV"
        systemctl reset-failed electricore-relais.service electricore-relais-alerte.service 2>/dev/null || true
        ;;
    *)
        echo "mode inconnu : '${MODE}' (attendu refuse|posed|onfailure)" >&2
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
