#!/usr/bin/env bash
# Test unitaire autonome du hook d'alerte OnFailure= du relais (#659, #668).
# Bash only, stubs msmtp/journalctl sur le PATH — même style que unit.sh (PASS/FAIL,
# ok/ko), mais fichier séparé : ce script exerce un exécutable en subprocess, pas des
# fonctions de deploy/lib/ à sourcer directement.
#
# Depuis #668, le hook n'est plus un fichier statique dans deploy/relais/ : il est
# RENDU par render_relais_alerte_script (deploy/lib/relais.sh) — install.sh ne fetch
# que deploy/lib/*.sh (fetch_lib_files), ce script doit donc voyager en heredoc, comme
# render_relais_compose. Ce runner matérialise le rendu dans un fichier temporaire
# avant de l'exercer, exactement comme install_relais_alerte_units le ferait en prod
# (à ceci près qu'il écrit dans /usr/local/bin/, pas dans un tmpdir de test).
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIB_DIR="${SCRIPT_DIR}/../lib"
# shellcheck source=../lib/relais.sh
source "${LIB_DIR}/relais.sh"

PASS=0; FAIL=0
ok() { printf '  \033[32m✓\033[0m %s\n' "$1"; PASS=$((PASS+1)); }
ko() { printf '  \033[31m✗\033[0m %s\n' "$1"; FAIL=$((FAIL+1)); }

echo "→ render_relais_alerte_script (hook OnFailure=, #659/#668)"

work_dir=$(mktemp -d)
ALERTE_SH="${work_dir}/electricore-relais-alerte.sh"
render_relais_alerte_script > "$ALERTE_SH"
chmod +x "$ALERTE_SH"

bash -n "$ALERTE_SH" && ok "bash -n : syntaxe valide" || ko "bash -n a échoué"
[[ -x "$ALERTE_SH" ]] && ok "rendu exécutable (chmod +x, comme install_relais_alerte_units)" || ko "pas exécutable"

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

# Cas 3 : CSV édité à la main, avec espaces autour des virgules → adresses propres
# (un " b@y.fr" avec espace de tête est un destinataire invalide côté msmtp).
( PATH="${stub_dir}:$PATH" RELAIS_ALERTE_MAILS="a@x.fr , b@y.fr" bash "$ALERTE_SH" )
grep -qx 'a@x.fr' "$args_file" && grep -qx 'b@y.fr' "$args_file" \
    && ok "CSV avec espaces → destinataires sans espace parasite" \
    || ko "CSV avec espaces → adresses polluées dans les args msmtp"

# Cas 4 : hostname en échec → le mail part quand même (sous set -e, fabriquer le
# sujet ne doit jamais pouvoir tuer le script avant l'envoi).
printf '#!/usr/bin/env bash\nexit 1\n' > "${stub_dir}/hostname"; chmod +x "${stub_dir}/hostname"
rm -f "$args_file"
( PATH="${stub_dir}:$PATH" RELAIS_ALERTE_MAILS="a@x.fr" bash "$ALERTE_SH" )
rc=$?
[[ "$rc" -eq 0 && -f "$args_file" ]] && ok "hostname en échec → mail envoyé quand même" || ko "hostname en échec → pas de mail (rc=$rc)"

# Cas 5 : commentaire de fin de ligne dans la valeur — systemd (EnvironmentFile=) ne
# coupe PAS les commentaires inline, contrairement à dotenv/compose : le hook doit
# l'amputer lui-même, sinon « # » et les mots du commentaire partent comme
# destinataires → échec msmtp, AUCUN mail, silencieusement (revue #669).
rm -f "$args_file"
( PATH="${stub_dir}:$PATH" RELAIS_ALERTE_MAILS='a@x.fr,b@y.fr   # ops + astreinte' bash "$ALERTE_SH" )
rc=$?
[[ "$rc" -eq 0 ]] && ok "commentaire inline → exit 0" || ko "commentaire inline → exit non-zéro (got $rc)"
grep -qx 'a@x.fr' "$args_file" 2>/dev/null && grep -qx 'b@y.fr' "$args_file" \
    && ok "commentaire inline → les 2 destinataires, propres" \
    || ko "commentaire inline → destinataires pollués ou absents"
grep -q '#' "$args_file" 2>/dev/null && ko "un fragment de commentaire a fui dans les args msmtp" \
    || ok "aucun fragment de commentaire dans les args msmtp"

# Cas 6 : valeur réduite à un commentaire (« RELAIS_ALERTE_MAILS=  # à remplir ») →
# même chemin que vide : exit 0 sans appeler msmtp.
rm -f "$args_file"
( PATH="${stub_dir}:$PATH" RELAIS_ALERTE_MAILS='   # à remplir' bash "$ALERTE_SH" ) 2>/dev/null
rc=$?
[[ "$rc" -eq 0 ]] && ok "valeur commentaire-seul → exit 0" || ko "valeur commentaire-seul → exit non-zéro (got $rc)"
[[ ! -f "$args_file" ]] && ok "valeur commentaire-seul → msmtp jamais appelé" || ko "msmtp appelé sur une valeur commentaire-seul"

# Cas 7 : le hook reste sur le chemin msmtprc host-level (jamais dans git, jamais
# per-slug — un seul token SMTP par box), pas de résidu /etc/electricore-relais/relais.env.
grep -q -- '--file=/etc/electricore-relais/msmtprc' "$ALERTE_SH" \
    && ok "msmtp --file= pointe /etc/electricore-relais/msmtprc (documenté, jamais dans git)" \
    || ko "chemin msmtprc absent/incorrect"
grep -q '/etc/electricore-relais/relais.env' "$ALERTE_SH" \
    && ko "résidu bare-metal /etc/electricore-relais/relais.env dans le script" \
    || ok "aucun résidu /etc/electricore-relais/relais.env dans le script"

# Cas 8 : pull d'image docker bruyant (#678) — le journal réel mêle des dizaines de
# lignes de progression (Extracting/Pull complete/Waiting/Verifying Checksum/Already
# exists) à l'erreur réelle (constaté sur le premier mail réel, VPS Enargia 28/07 :
# ~30 lignes de bruit pour 15 lignes utiles). Le corps du mail doit montrer l'erreur,
# pas la progression — un journalctl dédié (prioritaire dans le PATH) simule ce mélange.
noisy_stub_dir=$(mktemp -d)
cat > "${noisy_stub_dir}/journalctl" <<'STUB'
#!/usr/bin/env bash
for i in $(seq 1 30); do
    echo "Extracting [==================================================>]  13B/13B"
done
echo "Pull complete"
echo "Waiting"
echo "Verifying Checksum"
echo "Already exists"
echo "electricore-relais-1  | Traceback (most recent call last):"
echo "electricore-relais-1  | sops.exceptions.SopsDecryptionError: échec du déchiffrement (clé absente ?)"
echo "electricore-relais.service: Main process exited, code=exited, status=1/FAILURE"
STUB
chmod +x "${noisy_stub_dir}/journalctl"

rm -f "$args_file" "$stdin_file"
( PATH="${noisy_stub_dir}:${stub_dir}:$PATH" RELAIS_ALERTE_MAILS="a@x.fr" bash "$ALERTE_SH" )
rc=$?
[[ "$rc" -eq 0 ]] && ok "pull bruyant → exit 0" || ko "pull bruyant → exit non-zéro (got $rc)"
grep -q 'SopsDecryptionError' "$stdin_file" \
    && ok "pull bruyant → l'erreur réelle est dans le corps du mail" \
    || ko "pull bruyant → l'erreur réelle est ABSENTE du corps du mail"
grep -q 'Extracting' "$stdin_file" \
    && ko "pull bruyant → la progression docker a fui dans le corps du mail" \
    || ok "pull bruyant → aucune ligne de progression docker dans le corps du mail"

rm -rf "$noisy_stub_dir"

rm -rf "$stub_dir" "$work_dir" "$args_file" "$stdin_file" 2>/dev/null

echo
if [[ "$FAIL" -eq 0 ]]; then
    printf "\033[32m%d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
    exit 0
else
    printf "\033[31m%d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
    exit 1
fi
