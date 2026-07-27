#!/usr/bin/env bash
# Test unitaire autonome du hook d'alerte OnFailure= du relais (#659).
# Bash only, stubs msmtp/journalctl sur le PATH — même style que unit.sh (PASS/FAIL,
# ok/ko), mais fichier séparé : ce script exerce un exécutable standalone en
# subprocess, pas des fonctions de deploy/lib/ à sourcer.
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ALERTE_SH="${SCRIPT_DIR}/../relais/electricore-relais-alerte.sh"

PASS=0; FAIL=0
ok() { printf '  \033[32m✓\033[0m %s\n' "$1"; PASS=$((PASS+1)); }
ko() { printf '  \033[31m✗\033[0m %s\n' "$1"; FAIL=$((FAIL+1)); }

echo "→ electricore-relais-alerte.sh"

bash -n "$ALERTE_SH" && ok "bash -n : syntaxe valide" || ko "bash -n a échoué"
[[ -x "$ALERTE_SH" ]] && ok "exécutable (chmod +x commité)" || ko "pas exécutable"

stub_dir=$(mktemp -d)
args_file=$(mktemp)
stdin_file=$(mktemp)

cat > "${stub_dir}/msmtp" <<STUB
#!/usr/bin/env bash
printf '%s\n' "\$@" > "${args_file}"
cat > "${stdin_file}"
STUB
chmod +x "${stub_dir}/msmtp"

cat > "${stub_dir}/journalctl" <<'STUB'
#!/usr/bin/env bash
echo "STUB_JOURNAL_LINE_1 relais: échec push vers partenaire"
echo "STUB_JOURNAL_LINE_2 relais: run aveugle (#643)"
STUB
chmod +x "${stub_dir}/journalctl"

# Cas 1 : destinataires renseignés → msmtp appelé avec les 2 destinataires en
# arguments séparés, corps = Subject + lignes du journal stub.
( PATH="${stub_dir}:$PATH" RELAIS_ALERTE_MAILS="a@x.fr,b@y.fr" bash "$ALERTE_SH" )
rc=$?
[[ "$rc" -eq 0 ]] && ok "exit 0 (mail envoyé)" || ko "exit non-zéro (got $rc)"
grep -qx 'a@x.fr' "$args_file" && grep -qx 'b@y.fr' "$args_file" \
    && ok "msmtp reçoit les 2 destinataires en arguments séparés" \
    || ko "destinataires absents ou non séparés dans les args msmtp"
grep -q '^Subject:' "$stdin_file" && ok "le mail contient un Subject:" || ko "Subject: absent du mail"
grep -q 'STUB_JOURNAL_LINE_1' "$stdin_file" && grep -q 'STUB_JOURNAL_LINE_2' "$stdin_file" \
    && ok "le corps contient les lignes du journal stub" \
    || ko "lignes du journal absentes du corps du mail"

# Cas 2 : RELAIS_ALERTE_MAILS vide → exit 0 sans appeler msmtp (le chemin
# d'échec ne doit jamais échouer lui-même pour cause de config absente).
rm -f "$args_file"
( PATH="${stub_dir}:$PATH" RELAIS_ALERTE_MAILS="" bash "$ALERTE_SH" ) 2>/dev/null
rc=$?
[[ "$rc" -eq 0 ]] && ok "RELAIS_ALERTE_MAILS vide → exit 0" || ko "RELAIS_ALERTE_MAILS vide → exit non-zéro (got $rc)"
[[ ! -f "$args_file" ]] && ok "RELAIS_ALERTE_MAILS vide → msmtp jamais appelé" || ko "msmtp a été appelé malgré RELAIS_ALERTE_MAILS vide"

rm -rf "$stub_dir" "$args_file" "$stdin_file" 2>/dev/null

echo
if [[ "$FAIL" -eq 0 ]]; then
    printf "\033[32m%d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
    exit 0
else
    printf "\033[31m%d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
    exit 1
fi
