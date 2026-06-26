#!/usr/bin/env bash
# Runner unitaire pour les helpers de deploy/lib/. Bash only, zéro dépendance.
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIB_DIR="${SCRIPT_DIR}/../lib"
FIXTURES_DIR="${SCRIPT_DIR}/fixtures"

# shellcheck source=../lib/validate.sh
source "${LIB_DIR}/validate.sh"
# shellcheck source=../lib/os.sh
source "${LIB_DIR}/os.sh"
# shellcheck source=../lib/cli.sh
source "${LIB_DIR}/cli.sh"
# shellcheck source=../lib/config.sh
source "${LIB_DIR}/config.sh"
# shellcheck source=../lib/env_validate.sh
source "${LIB_DIR}/env_validate.sh"
# shellcheck source=../lib/harden.sh
source "${LIB_DIR}/harden.sh"

# `install.sh` est protégé par un guard `main` ; le sourcer expose
# `fetch_lib_files` sans déclencher l'exécution du script.
# shellcheck source=../install.sh
source "${SCRIPT_DIR}/../install.sh"

# `harden.sh` (wrapper autonome) est protégé par un guard `main_harden` ; le
# sourcer expose `parse_harden_args` sans rien exécuter. NB : sourcer install.sh
# ci-dessus a écrasé SCRIPT_DIR (→ deploy/) ; on passe par LIB_DIR, stable.
# shellcheck source=../harden.sh
source "${LIB_DIR}/../harden.sh"

# `unharden.sh` (wrapper de réversion) — guard `main_unharden` ; expose
# parse_unharden_args + les fonctions de réversion (déjà dans lib/harden.sh).
# shellcheck source=../unharden.sh
source "${LIB_DIR}/../unharden.sh"

PASS=0; FAIL=0
ok() { printf '  \033[32m✓\033[0m %s\n' "$1"; PASS=$((PASS+1)); }
ko() { printf '  \033[31m✗\033[0m %s\n' "$1"; FAIL=$((FAIL+1)); }

assert_ok()   { local desc="$1"; shift; if "$@" >/dev/null 2>&1; then ok "$desc"; else ko "$desc (exit non-zero)"; fi; }
assert_fail() { local desc="$1"; shift; if "$@" >/dev/null 2>&1; then ko "$desc (devait échouer)"; else ok "$desc"; fi; }
assert_eq()   { if [[ "$1" == "$2" ]]; then ok "$3"; else ko "$3 — got '$1', want '$2'"; fi; }

echo "→ validate.sh"
assert_ok   "slug 'edn'"                       validate_slug edn
assert_ok   "slug 'enargia-test'"              validate_slug enargia-test
assert_fail "slug 'EDN' (majuscules)"          validate_slug EDN
assert_fail "slug 'edn_test' (underscore)"     validate_slug edn_test
assert_fail "slug 'e' (trop court)"            validate_slug e
assert_fail "slug vide"                        validate_slug ""

assert_ok   "aes_key 32 hex"                   validate_aes_key "$(printf 'a%.0s' {1..32})"
assert_ok   "aes_key 64 hex MAJ+min"           validate_aes_key "abCDef0123456789abCDef0123456789abCDef0123456789abCDef0123456789"
assert_fail "aes_key 31 hex (trop court)"      validate_aes_key "$(printf 'a%.0s' {1..31})"
assert_fail "aes_key 64 non-hex"               validate_aes_key "$(printf 'z%.0s' {1..64})"
assert_fail "aes_key vide"                     validate_aes_key ""

assert_ok   "api_key 32 chars"                 validate_api_key "$(printf 'a%.0s' {1..32})"
assert_fail "api_key 31 chars"                 validate_api_key "$(printf 'a%.0s' {1..31})"
assert_fail "api_key vide"                     validate_api_key ""

assert_ok   "url sftp avec creds"              validate_url "sftp://u:p@h.fr:22/p"
assert_ok   "url file"                         validate_url "file:///var/enedis/"
assert_ok   "url https"                        validate_url "https://example.com"
assert_fail "url plain"                        validate_url "not-a-url"
assert_fail "url ftp (non supporté)"           validate_url "ftp://h.fr/"

assert_ok   "email basique"                    validate_email "user@example.com"
assert_fail "email sans @"                     validate_email "user.example.com"

assert_ok   "domain electricore.fr"            validate_domain "electricore.fr"
assert_ok   "domain edn.electricore.fr"        validate_domain "edn.electricore.fr"
assert_fail "domain underscore"                validate_domain "edn_electricore.fr"

echo
echo "→ os.sh"
assert_eq  "$(OS_RELEASE_PATH=${FIXTURES_DIR}/os-release-ubuntu-24.04 detect_os)" \
           "ubuntu-24.04" "detect_os Ubuntu 24.04"
assert_eq  "$(OS_RELEASE_PATH=${FIXTURES_DIR}/os-release-debian-12 detect_os)" \
           "debian-12"    "detect_os Debian 12"
assert_eq  "$(OS_RELEASE_PATH=${FIXTURES_DIR}/os-release-alpine detect_os)" \
           "alpine-3.19.1" "detect_os Alpine (présentation)"
assert_ok   "is_supported_os Ubuntu 24.04"   bash -c "export OS_RELEASE_PATH='${FIXTURES_DIR}/os-release-ubuntu-24.04'; source '${LIB_DIR}/os.sh'; is_supported_os"
assert_ok   "is_supported_os Debian 12"      bash -c "export OS_RELEASE_PATH='${FIXTURES_DIR}/os-release-debian-12';   source '${LIB_DIR}/os.sh'; is_supported_os"
assert_fail "is_supported_os Alpine"         bash -c "export OS_RELEASE_PATH='${FIXTURES_DIR}/os-release-alpine';      source '${LIB_DIR}/os.sh'; is_supported_os"
assert_fail "is_supported_os Ubuntu 20.04"   bash -c "export OS_RELEASE_PATH='${FIXTURES_DIR}/os-release-ubuntu-20.04'; source '${LIB_DIR}/os.sh'; is_supported_os"

echo
echo "→ harden.sh / authorized_keys_present (garde-fou anti-verrouillage)"
tmp_ak=$(mktemp)
printf 'ssh-ed25519 AAAAC3Nz... ops@host\n' > "$tmp_ak"
assert_ok   "clé présente → 0"                 authorized_keys_present "$tmp_ak"
: > "$tmp_ak"
assert_fail "fichier vide → 1"                 authorized_keys_present "$tmp_ak"
printf '# que des commentaires\n\n   \n' > "$tmp_ak"
assert_fail "commentaires/blancs seuls → 1"    authorized_keys_present "$tmp_ak"
rm -f "$tmp_ak"
assert_fail "fichier absent → 1"               authorized_keys_present "/nonexistent-ak-$$"

echo
echo "→ harden.sh / render_sshd_hardening (drop-in sshd)"
sshd_conf="$(render_sshd_hardening)"
grep -qx "PermitRootLogin no"              <<<"$sshd_conf" && ok "drop-in: PermitRootLogin no" || ko "drop-in PermitRootLogin"
grep -qx "PasswordAuthentication no"       <<<"$sshd_conf" && ok "drop-in: PasswordAuthentication no" || ko "drop-in PasswordAuthentication"
grep -qx "KbdInteractiveAuthentication no" <<<"$sshd_conf" && ok "drop-in: KbdInteractiveAuthentication no" || ko "drop-in KbdInteractive"
grep -qx "PubkeyAuthentication yes"        <<<"$sshd_conf" && ok "drop-in: PubkeyAuthentication yes (clé conservée)" || ko "drop-in Pubkey"
grep -qx "X11Forwarding no"                <<<"$sshd_conf" && ok "drop-in: X11Forwarding no" || ko "drop-in X11Forwarding"
grep -qx "MaxAuthTries 3"                  <<<"$sshd_conf" && ok "drop-in: MaxAuthTries 3" || ko "drop-in MaxAuthTries"
! grep -qi "AllowUsers" <<<"$sshd_conf"    && ok "drop-in: pas d'AllowUsers (piège évité, ADR-0031)" || ko "drop-in AllowUsers présent"

echo
echo "→ harden.sh / render_fail2ban_jail (jail sshd)"
jail_conf="$(render_fail2ban_jail)"
grep -qx "\[sshd\]"            <<<"$jail_conf" && ok "jail: section [sshd]" || ko "jail [sshd]"
grep -qx "enabled  = true"    <<<"$jail_conf" && ok "jail: enabled = true" || ko "jail enabled"
grep -qx "backend  = systemd" <<<"$jail_conf" && ok "jail: backend = systemd (piège Debian/Ubuntu moderne)" || ko "jail backend systemd"
grep -qx "maxretry = 3"       <<<"$jail_conf" && ok "jail: maxretry = 3" || ko "jail maxretry"
grep -qE "^findtime = "       <<<"$jail_conf" && ok "jail: findtime défini" || ko "jail findtime"
grep -qE "^bantime  = "       <<<"$jail_conf" && ok "jail: bantime défini" || ko "jail bantime"

echo
echo "→ harden.sh / unattended-upgrades (maj auto + reboot 04:30)"
periodic_conf="$(render_unattended_periodic)"
grep -qx 'APT::Periodic::Update-Package-Lists "1";' <<<"$periodic_conf" && ok "periodic: maj des listes activée" || ko "periodic update-lists"
grep -qx 'APT::Periodic::Unattended-Upgrade "1";'   <<<"$periodic_conf" && ok "periodic: unattended-upgrade activé" || ko "periodic unattended"
override_conf="$(render_unattended_override)"
grep -qx 'Unattended-Upgrade::Automatic-Reboot "true";'          <<<"$override_conf" && ok "override: Automatic-Reboot true" || ko "override reboot true"
grep -qx 'Unattended-Upgrade::Automatic-Reboot-Time "04:30";'    <<<"$override_conf" && ok "override: reboot 04:30 (après backup 03:30)" || ko "override reboot time"
# Heure paramétrable
override_05="$(UNATTENDED_REBOOT_TIME=05:15 render_unattended_override)"
grep -q '"05:15"' <<<"$override_05" && ok "override: heure de reboot paramétrable" || ko "override heure non paramétrable"

echo
echo "→ cli.sh / parse_args"
( parse_args --slug edn --domain edn.fr >/dev/null 2>&1
  [[ "$OPT_SLUG" == "edn" && "$OPT_DOMAIN" == "edn.fr" && "$OPT_VERSION" == "latest" ]]
) && ok "parse_args minimal (--slug + --domain)" || ko "parse_args minimal"

( parse_args --slug edn --domain edn.fr --version 1.7.0 --ssh-pubkey "ssh-ed25519 AAAA" --skip-dns >/dev/null 2>&1
  [[ "$OPT_VERSION" == "1.7.0" && "$OPT_SSH_PUBKEY" == "ssh-ed25519 AAAA" && "$OPT_SKIP_DNS" == "1" ]]
) && ok "parse_args avec --version --ssh-pubkey --skip-dns" || ko "parse_args options complètes"

assert_fail "parse_args sans --slug"      parse_args --domain edn.fr
assert_fail "parse_args sans --domain"    parse_args --slug edn
assert_fail "parse_args flag inconnu"     parse_args --slug edn --domain edn.fr --foo

# Durcissement (ADR-0031) : --no-harden et --admin-pubkey
( parse_args --slug edn --domain edn.fr >/dev/null 2>&1
  [[ "$OPT_NO_HARDEN" == "0" && -z "$OPT_ADMIN_PUBKEY" ]]
) && ok "parse_args: durcissement actif par défaut (OPT_NO_HARDEN=0)" || ko "parse_args durcissement par défaut"

( parse_args --slug edn --domain edn.fr --no-harden >/dev/null 2>&1
  [[ "$OPT_NO_HARDEN" == "1" ]]
) && ok "parse_args: --no-harden → OPT_NO_HARDEN=1" || ko "parse_args --no-harden"

( parse_args --slug edn --domain edn.fr --admin-pubkey "ssh-ed25519 AAAA ops" >/dev/null 2>&1
  [[ "$OPT_ADMIN_PUBKEY" == "ssh-ed25519 AAAA ops" ]]
) && ok "parse_args: --admin-pubkey capturé" || ko "parse_args --admin-pubkey"

# Toggles granulaires (cohérents avec harden.sh, #262)
( parse_args --slug edn --domain edn.fr --no-fail2ban --no-unattended-upgrades --no-sshd >/dev/null 2>&1
  [[ "$OPT_NO_SSHD" == "1" && "$OPT_NO_FAIL2BAN" == "1" && "$OPT_NO_UNATTENDED" == "1" ]]
) && ok "parse_args: toggles granulaires --no-sshd/--no-fail2ban/--no-unattended-upgrades" || ko "parse_args toggles granulaires"

echo
echo "→ harden.sh (wrapper autonome) / parse_harden_args"
( parse_harden_args >/dev/null 2>&1
  [[ "$OPT_NO_SSHD" == "0" && "$OPT_NO_FAIL2BAN" == "0" && "$OPT_NO_UNATTENDED" == "0" && -z "$OPT_ADMIN_PUBKEY" ]]
) && ok "parse_harden_args: défauts (tout durci, pas d'override)" || ko "parse_harden_args défauts"

( parse_harden_args --admin-pubkey "ssh-ed25519 BBBB ops" --no-fail2ban >/dev/null 2>&1
  [[ "$OPT_ADMIN_PUBKEY" == "ssh-ed25519 BBBB ops" && "$OPT_NO_FAIL2BAN" == "1" && "$OPT_NO_SSHD" == "0" ]]
) && ok "parse_harden_args: --admin-pubkey + --no-fail2ban" || ko "parse_harden_args options"

( parse_harden_args --no-sshd --no-unattended-upgrades >/dev/null 2>&1
  [[ "$OPT_NO_SSHD" == "1" && "$OPT_NO_UNATTENDED" == "1" ]]
) && ok "parse_harden_args: --no-sshd + --no-unattended-upgrades" || ko "parse_harden_args no-sshd/no-unattended"

echo
echo "→ unharden.sh (réversion) / parse_unharden_args + no-op"
( parse_unharden_args >/dev/null 2>&1; [[ "$OPT_PURGE_OPS" == "0" ]] ) \
    && ok "parse_unharden_args: ops conservé par défaut (OPT_PURGE_OPS=0)" || ko "parse_unharden_args défaut"
( parse_unharden_args --purge-ops >/dev/null 2>&1; [[ "$OPT_PURGE_OPS" == "1" ]] ) \
    && ok "parse_unharden_args: --purge-ops → 1" || ko "parse_unharden_args --purge-ops"
( parse_unharden_args --bogus >/dev/null 2>&1 ); [[ "$?" -eq 2 ]] \
    && ok "parse_unharden_args: argument inconnu → exit 2" || ko "parse_unharden_args arg inconnu"
# Réversions no-op (rien à retirer) — branches sûres, sans toucher sshd/systemd
( SSHD_HARDEN_DROPIN="/nonexistent-$$" unharden_sshd >/dev/null 2>&1 ) \
    && ok "unharden_sshd: drop-in absent → no-op (pas de reload sshd)" || ko "unharden_sshd no-op"
( FAIL2BAN_JAIL="/nonexistent-$$" remove_fail2ban_jail >/dev/null 2>&1 ) \
    && ok "remove_fail2ban_jail: jail absente → no-op" || ko "remove_fail2ban_jail no-op"
( UNATTENDED_OVERRIDE="/nope1-$$" UNATTENDED_PERIODIC="/nope2-$$" remove_unattended_config >/dev/null 2>&1 ) \
    && ok "remove_unattended_config: conf absente → no-op" || ko "remove_unattended_config no-op"
# remove_unattended_config retire bien les fichiers présents (file-only, sûr)
uov=$(mktemp); uop=$(mktemp)
( UNATTENDED_OVERRIDE="$uov" UNATTENDED_PERIODIC="$uop" remove_unattended_config >/dev/null 2>&1 )
[[ ! -f "$uov" && ! -f "$uop" ]] && ok "remove_unattended_config: retire les fichiers présents" || ko "remove_unattended_config retrait"
rm -f "$uov" "$uop"

echo
echo "→ install.sh / lib_dir_complete (anti trap stale-lib)"
assert_ok   "lib/ du repo est complet"          lib_dir_complete "${LIB_DIR}"
assert_fail "répertoire absent → incomplet"     lib_dir_complete "/nonexistent-libdir-$$"
incdir=$(mktemp -d); : > "${incdir}/log.sh"   # un seul helper sur douze
assert_fail "lib/ partiel (helper manquant) → incomplet" lib_dir_complete "$incdir"
rm -rf "$incdir"

echo
echo "→ install.sh / fetch_lib_files"
tmp_target=$(mktemp -d)
fetch_lib_files "file://${FIXTURES_DIR}/fake_lib" "$tmp_target"
[[ -f "${tmp_target}/log.sh" && -f "${tmp_target}/cli.sh" && -f "${tmp_target}/config.sh" && -f "${tmp_target}/harden.sh" ]] \
    && ok "fetch_lib_files: les 12 helpers sont téléchargés au 1er appel" \
    || ko "fetch_lib_files: helpers manquants après 1er appel"
# 2e appel idempotent (les fichiers existent déjà, doit re-télécharger sans erreur)
fetch_lib_files "file://${FIXTURES_DIR}/fake_lib" "$tmp_target"
[[ -f "${tmp_target}/log.sh" ]] && ok "fetch_lib_files: idempotent (2e appel ne casse rien)" \
    || ko "fetch_lib_files: 2e appel a effacé les fichiers"
# URL invalide → exit non-zero pour signaler l'échec
fetch_lib_files "file:///tmp/nonexistent-dir-$$" "$tmp_target" 2>/dev/null && ko "fetch_lib_files: URL invalide devrait échouer" \
    || ok "fetch_lib_files: URL invalide → exit non-zero"
rm -rf "$tmp_target"

echo
echo "→ config.sh / map_version_to_git_ref"
assert_eq "$(map_version_to_git_ref latest)"      "main"       "latest → main (alias Docker)"
assert_eq "$(map_version_to_git_ref 1.7.0rc2)"    "v1.7.0rc2"  "rc → v-prefixed tag"
assert_eq "$(map_version_to_git_ref 1.6.1)"       "v1.6.1"     "stable → v-prefixed tag"
assert_eq "$(map_version_to_git_ref 2.0.0)"       "v2.0.0"     "major bump → v-prefixé"
assert_eq "$(map_version_to_git_ref 1.8.0a1)"     "v1.8.0a1"   "alpha PEP 440"
assert_eq "$(map_version_to_git_ref main)"        "main"       "branche main inchangée"
assert_eq "$(map_version_to_git_ref dev)"         "dev"        "branche dev inchangée"
assert_eq "$(map_version_to_git_ref abc1234)"     "abc1234"    "SHA inchangé"

echo
echo "→ config.sh / substitute_*"
tmp_env=$(mktemp)
cp "${FIXTURES_DIR}/env-template" "$tmp_env"
substitute_env "$tmp_env" "edn"
grep -q "^INSTANCE_SLUG=edn$" "$tmp_env" && ok "substitute_env: INSTANCE_SLUG=edn" || ko "substitute_env INSTANCE_SLUG"
grep -q "^BACKUPS_PATH=/srv/edn/backups$" "$tmp_env" && ok "substitute_env: BACKUPS_PATH=/srv/edn/backups" || ko "substitute_env BACKUPS_PATH"
grep -q "^ELECTRICORE_VERSION=latest$" "$tmp_env" && ok "substitute_env: ELECTRICORE_VERSION inchangée si non passée" || ko "substitute_env touche ELECTRICORE_VERSION sans argument"
rm -f "$tmp_env"

# substitute_env avec version explicite
tmp_env=$(mktemp)
cp "${FIXTURES_DIR}/env-template" "$tmp_env"
substitute_env "$tmp_env" "edn" "1.7.0rc3"
grep -q "^ELECTRICORE_VERSION=1.7.0rc3$" "$tmp_env" && ok "substitute_env: ELECTRICORE_VERSION=1.7.0rc3 (3e arg)" || ko "substitute_env n'écrit pas ELECTRICORE_VERSION"
grep -q "^INSTANCE_SLUG=edn$" "$tmp_env" && ok "substitute_env: INSTANCE_SLUG préservé avec version" || ko "substitute_env INSTANCE_SLUG cassé avec version"
rm -f "$tmp_env"

tmp_caddy=$(mktemp)
cp "${FIXTURES_DIR}/caddyfile-template" "$tmp_caddy"
substitute_caddyfile "$tmp_caddy" "edn.electricore.fr" "ops@edn.fr"
grep -q "edn.electricore.fr" "$tmp_caddy" && ok "substitute_caddyfile: domaine" || ko "substitute_caddyfile domaine"
grep -q "ops@edn.fr" "$tmp_caddy" && ok "substitute_caddyfile: email" || ko "substitute_caddyfile email"
! grep -q "electricore.exemple.fr" "$tmp_caddy" && ok "substitute_caddyfile: pas de placeholder résiduel" \
    || ko "substitute_caddyfile placeholder résiduel"
rm -f "$tmp_caddy"

echo
echo "→ env_validate.sh"
assert_eq "$(read_env_var ${FIXTURES_DIR}/env-valid API_KEY)" "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" "read_env_var API_KEY"
assert_eq "$(read_env_var ${FIXTURES_DIR}/env-valid QUOTED_VALUE)" "hello world" "read_env_var avec guillemets"
assert_eq "$(read_env_var ${FIXTURES_DIR}/env-valid WITH_COMMENT)" "foo" "read_env_var ignore # comment"
assert_eq "$(read_env_var ${FIXTURES_DIR}/env-valid ABSENT)" "" "read_env_var clé absente → vide"

assert_ok   "validate_env_file (fixture valide)"   validate_env_file "${FIXTURES_DIR}/env-valid" "edn"
assert_fail "validate_env_file (fixture invalide)" validate_env_file "${FIXTURES_DIR}/env-invalid" "edn"
assert_fail "validate_env_file (slug mismatch)"    validate_env_file "${FIXTURES_DIR}/env-valid" "wrong-slug"
assert_fail "validate_env_file (fichier absent)"   validate_env_file "/nonexistent" "edn"

# IV optionnel (ADR-0040) : une clé AES-256 SANS __IV (schéma IV-préfixé) est déjà acceptée
# par la fixture env-valid ci-dessus. Inversement, un __IV présent mais non-hex doit échouer.
tmp_env=$(mktemp); cp "${FIXTURES_DIR}/env-valid" "$tmp_env"
echo "AES__TROUSSEAU__aes256_2026__IV=zzzz" >> "$tmp_env"
assert_fail "validate_env_file (__IV présent mais non-hex)" validate_env_file "$tmp_env" "edn"
rm -f "$tmp_env"

# prepend_errors_to_env doit insérer un bloc en tête sans dupliquer si rappelée
tmp_env=$(mktemp); cp "${FIXTURES_DIR}/env-template" "$tmp_env"
prepend_errors_to_env "$tmp_env" $'erreur A\nerreur B'
grep -q "VALIDATION ÉCHOUÉE" "$tmp_env" && ok "prepend_errors_to_env: insère le header" || ko "header absent"
grep -q "x erreur A" "$tmp_env" && ok "prepend_errors_to_env: liste les erreurs" || ko "erreurs absentes"
# Rejouer doit remplacer le bloc précédent, pas l'empiler
prepend_errors_to_env "$tmp_env" $'erreur C'
header_count=$(grep -c "VALIDATION ÉCHOUÉE" "$tmp_env" || true)
assert_eq "$header_count" "1" "prepend_errors_to_env: idempotent (1 seul bloc après 2 appels)"
rm -f "$tmp_env"

# Régression : prepend_errors_to_env ne doit JAMAIS effacer les vraies données,
# même si le .env contient des séparateurs `# ===` (comme dans .env.example).
tmp_env=$(mktemp); cp "${FIXTURES_DIR}/env-with-section-headers" "$tmp_env"
prepend_errors_to_env "$tmp_env" "erreur quelconque"
grep -q "^INSTANCE_SLUG=edn$" "$tmp_env" && ok "prepend: INSTANCE_SLUG survit aux séparateurs # ===" || ko "INSTANCE_SLUG effacé"
grep -q "^API_KEY=should-survive" "$tmp_env" && ok "prepend: API_KEY survit" || ko "API_KEY effacé"
grep -c "^# ===" "$tmp_env" >/dev/null
sep_count=$(grep -c "^# ===" "$tmp_env" || true)
[[ "$sep_count" -ge 4 ]] && ok "prepend: séparateurs de section préservés ($sep_count présents)" \
    || ko "séparateurs # === effacés ($sep_count restants)"
# Rejouer doit toujours préserver les données
prepend_errors_to_env "$tmp_env" "erreur 2"
grep -q "^INSTANCE_SLUG=edn$" "$tmp_env" && ok "prepend: INSTANCE_SLUG survit au 2e appel" || ko "INSTANCE_SLUG effacé après 2e prepend"
rm -f "$tmp_env"

echo
echo "→ env_validate.sh / strip_validation_error_block"
# Cas 1 : bloc présent + validate_env_file OK → bloc supprimé
tmp_env=$(mktemp); cp "${FIXTURES_DIR}/env-valid" "$tmp_env"
prepend_errors_to_env "$tmp_env" "erreur temporaire"
grep -q "VALIDATION-ERROR-BLOCK-BEGIN" "$tmp_env" || { ko "setup: bloc absent avant strip"; rm -f "$tmp_env"; }
strip_validation_error_block "$tmp_env"
grep -q "VALIDATION-ERROR-BLOCK-BEGIN" "$tmp_env" && ko "strip: bloc toujours présent après strip" || ok "strip: bloc absent après strip"
# Cas 2 : idempotent — aucun bloc → no-op, fichier inchangé
cp "${FIXTURES_DIR}/env-valid" "$tmp_env"
checksum_before=$(md5sum "$tmp_env" | awk '{print $1}')
strip_validation_error_block "$tmp_env"
checksum_after=$(md5sum "$tmp_env" | awk '{print $1}')
assert_eq "$checksum_after" "$checksum_before" "strip: no-op si aucun bloc présent"
# Cas 3 : les vraies données survivent au strip
cp "${FIXTURES_DIR}/env-valid" "$tmp_env"
prepend_errors_to_env "$tmp_env" "erreur x"
strip_validation_error_block "$tmp_env"
grep -q "^INSTANCE_SLUG=edn$" "$tmp_env" && ok "strip: INSTANCE_SLUG survit" || ko "strip: INSTANCE_SLUG effacé"
grep -q "^API_KEY=" "$tmp_env" && ok "strip: API_KEY survit" || ko "strip: API_KEY effacé"
rm -f "$tmp_env"

echo
echo "→ user.sh / ensure_backups_dir (contrat uid 1000, #459)"
# Le conteneur tourne en uid 1000 → /srv/<slug>/backups doit lui appartenir, sinon
# backup_duckdb.sh plante au mkdir (« Permission denied »). On vérifie sur un home
# jetable, en chownant vers SOI-MÊME (CONTAINER_UID=$(id -u)) pour tourner sans root.
bk_root=$(mktemp -d)
( CONTAINER_UID="$(id -u)" CONTAINER_GID="$(id -g)" SRV_BASE="$bk_root" ensure_backups_dir edn >/dev/null 2>&1 )
[[ -d "${bk_root}/edn/backups" ]] && ok "ensure_backups_dir: crée /srv/<slug>/backups" || ko "backups non créé"
assert_eq "$(stat -c '%u' "${bk_root}/edn/backups" 2>/dev/null)" "$(id -u)" \
    "ensure_backups_dir: backups owned par CONTAINER_UID (pas <slug>)"
assert_eq "$(stat -c '%a' "${bk_root}/edn/backups" 2>/dev/null)" "2750" \
    "ensure_backups_dir: setgid 2750 (snapshots héritent du groupe → lecture <slug>)"
# Idempotent + ré-assertion après un chown -R clobber (cas reconfigure : chown_instance_home
# redonne backups à <slug>, ensure_backups_dir doit le reprendre).
chmod 0700 "${bk_root}/edn/backups"
( CONTAINER_UID="$(id -u)" CONTAINER_GID="$(id -g)" SRV_BASE="$bk_root" ensure_backups_dir edn >/dev/null 2>&1 ) \
    && ok "ensure_backups_dir: idempotent (2e appel ré-asserte sans erreur)" || ko "ensure_backups_dir 2e appel échoue"
assert_eq "$(stat -c '%a' "${bk_root}/edn/backups" 2>/dev/null)" "2750" \
    "ensure_backups_dir: ré-asserte le mode après clobber (reconfigure)"
rm -rf "$bk_root"

# ensure_slug_in_container_group : no-op si <slug> est déjà membre du groupe cible.
# On joue le user courant + son gid primaire → branche skip atteignable sans root.
me=$(id -un)
( CONTAINER_GID="$(id -g)" ensure_slug_in_container_group "$me" >/dev/null 2>&1 ) \
    && ok "ensure_slug_in_container_group: no-op si déjà membre (pas de usermod)" \
    || ko "ensure_slug_in_container_group a échoué sur un membre existant"

echo
if [[ "$FAIL" -eq 0 ]]; then
    printf "\033[32m%d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
    exit 0
else
    printf "\033[31m%d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
    exit 1
fi
