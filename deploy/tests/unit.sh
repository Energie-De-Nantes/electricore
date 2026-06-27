#!/usr/bin/env bash
# Runner unitaire pour les helpers de deploy/lib/. Bash only, zéro dépendance.
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIB_DIR="${SCRIPT_DIR}/../lib"
FIXTURES_DIR="${SCRIPT_DIR}/fixtures"

# shellcheck source=../lib/log.sh
source "${LIB_DIR}/log.sh"
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
# shellcheck source=../lib/ingestion.sh
source "${LIB_DIR}/ingestion.sh"
# shellcheck source=../lib/secrets.sh
source "${LIB_DIR}/secrets.sh"
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

# `add-provider.sh` (outil admin secrets-as-code, ADR-0044) — guard
# `main_add_provider` ; expose parse_add_provider_args + add_recipient_to_sops + add_provider.
# shellcheck source=../add-provider.sh
source "${LIB_DIR}/../add-provider.sh"

# `smoke.sh` (fume l'image Docker buildée, issue #435) — guard `main_smoke` ;
# expose smoke_image + smoke_importabilite + smoke_dechiffrement.
# shellcheck source=../docker/smoke.sh
source "${LIB_DIR}/../docker/smoke.sh"

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
( parse_args --slug edn --domain edn.fr --deploy-repo "git@example.test:org/deploy.git" >/dev/null 2>&1
  [[ "$OPT_SLUG" == "edn" && "$OPT_DOMAIN" == "edn.fr" && "$OPT_VERSION" == "latest" ]]
) && ok "parse_args minimal (--slug + --domain + --deploy-repo)" || ko "parse_args minimal"

( parse_args --slug edn --domain edn.fr --deploy-repo "git@example.test:org/deploy.git" --version 1.7.0 --ssh-pubkey "ssh-ed25519 AAAA" --skip-dns >/dev/null 2>&1
  [[ "$OPT_VERSION" == "1.7.0" && "$OPT_SSH_PUBKEY" == "ssh-ed25519 AAAA" && "$OPT_SKIP_DNS" == "1" ]]
) && ok "parse_args avec --version --ssh-pubkey --skip-dns" || ko "parse_args options complètes"

# Override local du tag (#460) : OPT_VERSION_EXPLICIT distingue --version passé du défaut.
( parse_args --slug edn --domain edn.fr --deploy-repo "git@example.test:org/deploy.git" >/dev/null 2>&1
  [[ "$OPT_VERSION_EXPLICIT" == "0" ]]
) && ok "parse_args: OPT_VERSION_EXPLICIT=0 sans --version (baseline GitOps)" || ko "parse_args OPT_VERSION_EXPLICIT défaut"
( parse_args --slug edn --domain edn.fr --deploy-repo "git@example.test:org/deploy.git" --version 3.4.0rc6 >/dev/null 2>&1
  [[ "$OPT_VERSION_EXPLICIT" == "1" && "$OPT_VERSION" == "3.4.0rc6" ]]
) && ok "parse_args: OPT_VERSION_EXPLICIT=1 avec --version (override local)" || ko "parse_args OPT_VERSION_EXPLICIT avec --version"

# --deploy-repo est OBLIGATOIRE depuis le cutover secrets-as-code (ADR-0044 §8)
assert_fail "parse_args sans --deploy-repo" parse_args --slug edn --domain edn.fr
( parse_args --slug edn --domain edn.fr --deploy-repo "git@example.test:org/deploy.git" >/dev/null 2>&1
  [[ "$OPT_DEPLOY_REPO" == "git@example.test:org/deploy.git" ]]
) && ok "parse_args: --deploy-repo capturé" || ko "parse_args --deploy-repo"

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
[[ -f "${tmp_target}/log.sh" && -f "${tmp_target}/cli.sh" && -f "${tmp_target}/config.sh" && -f "${tmp_target}/secrets.sh" && -f "${tmp_target}/harden.sh" ]] \
    && ok "fetch_lib_files: les 13 helpers sont téléchargés au 1er appel" \
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
echo "→ config.sh / substitute_caddyfile"
tmp_caddy=$(mktemp)
cp "${FIXTURES_DIR}/caddyfile-template" "$tmp_caddy"
substitute_caddyfile "$tmp_caddy" "edn.electricore.fr" "ops@edn.fr"
grep -q "edn.electricore.fr" "$tmp_caddy" && ok "substitute_caddyfile: domaine" || ko "substitute_caddyfile domaine"
grep -q "ops@edn.fr" "$tmp_caddy" && ok "substitute_caddyfile: email" || ko "substitute_caddyfile email"
! grep -q "electricore.exemple.fr" "$tmp_caddy" && ok "substitute_caddyfile: pas de placeholder résiduel" \
    || ko "substitute_caddyfile placeholder résiduel"
rm -f "$tmp_caddy"

echo
echo "→ config.sh / override_config_version (override local du tag, #460)"
# Simule un config.env pinné par le dépôt (rc5) + une clé non-version à préserver.
tmp_cfg=$(mktemp)
printf 'INSTANCE_SLUG=edn\nELECTRICORE_VERSION=3.4.0rc5\nBACKUPS_PATH=/srv/edn/backups\nODOO_ENV=prod\n' > "$tmp_cfg"
override_config_version "$tmp_cfg" "3.4.0rc6"
grep -q "^ELECTRICORE_VERSION=3.4.0rc6$" "$tmp_cfg" && ok "override_config_version: ELECTRICORE_VERSION overridé (rc5 → rc6)" || ko "override_config_version n'écrit pas la version"
grep -q "^INSTANCE_SLUG=edn$" "$tmp_cfg" && ok "override_config_version: INSTANCE_SLUG préservé (baseline GitOps intacte)" || ko "override_config_version a touché INSTANCE_SLUG"
grep -q "^BACKUPS_PATH=/srv/edn/backups$" "$tmp_cfg" && ok "override_config_version: BACKUPS_PATH préservé" || ko "override_config_version a touché BACKUPS_PATH"
grep -q "^ODOO_ENV=prod$" "$tmp_cfg" && ok "override_config_version: autres clés préservées (ODOO_ENV)" || ko "override_config_version a touché ODOO_ENV"
# Une seule ligne ELECTRICORE_VERSION (pas de duplication)
vcount=$(grep -c "^ELECTRICORE_VERSION=" "$tmp_cfg" || true)
assert_eq "$vcount" "1" "override_config_version: une seule ligne ELECTRICORE_VERSION après override"
rm -f "$tmp_cfg"

echo
echo "→ install.sh / câblage de l'override --version (garde anti-régression #299)"
# override_config_version a survécu à #299, mais son APPEL dans install.sh avait été
# supprimé en même temps que le fix ETL → --version devenait inerte (l'image pinée du
# dépôt déployée quand même). La fonction + son test unitaire passaient vert pendant que
# le câblage réel disparaissait. On verrouille le câblage par une garde de présence.
# SCRIPT_DIR a pu être muté par un helper sourcé plus haut → on recalcule depuis BASH_SOURCE.
install_sh="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/install.sh"
grep -q "override_config_version" "$install_sh" \
    && ok "install.sh appelle override_config_version (--version câblé)" \
    || ko "install.sh n'appelle PAS override_config_version → --version inerte (régression #299)"
grep -q "OPT_VERSION_EXPLICIT" "$install_sh" \
    && ok "install.sh garde l'override derrière OPT_VERSION_EXPLICIT" \
    || ko "install.sh ne garde pas l'override derrière OPT_VERSION_EXPLICIT"

echo
echo "→ env_validate.sh / read_env_var"
tmp_rev=$(mktemp)
printf 'API_KEY=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\nQUOTED_VALUE="hello world"\nWITH_COMMENT=foo   # trailing comment ignored\n' > "$tmp_rev"
assert_eq "$(read_env_var "$tmp_rev" API_KEY)" "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" "read_env_var API_KEY"
assert_eq "$(read_env_var "$tmp_rev" QUOTED_VALUE)" "hello world" "read_env_var avec guillemets"
assert_eq "$(read_env_var "$tmp_rev" WITH_COMMENT)" "foo" "read_env_var ignore # comment"
assert_eq "$(read_env_var "$tmp_rev" ABSENT)" "" "read_env_var clé absente → vide"
rm -f "$tmp_rev"

echo
echo "→ env_validate.sh / split config/secret (ADR-0044)"
# config.env valide (slug + version + backups, AUCUN secret)
tmp_cfg=$(mktemp)
printf 'INSTANCE_SLUG=edn\nELECTRICORE_VERSION=1.7.0\nBACKUPS_PATH=/srv/edn/backups\nBOT__NOTIFY_CHAT_ID=-100123\n' > "$tmp_cfg"
assert_ok   "validate_config_env (config claire valide)"   validate_config_env "$tmp_cfg" "edn"
assert_fail "validate_config_env (slug mismatch)"          validate_config_env "$tmp_cfg" "autre"
rm -f "$tmp_cfg"
# Garde-fou anti-fuite : un secret dans config.env doit faire échouer
tmp_cfg=$(mktemp)
printf 'INSTANCE_SLUG=edn\nELECTRICORE_VERSION=1.7.0\nBACKUPS_PATH=/srv/edn/backups\nAPI_KEY=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n' > "$tmp_cfg"
assert_fail "validate_config_env refuse un secret en clair (API_KEY)" validate_config_env "$tmp_cfg" "edn"
rm -f "$tmp_cfg"
# Idem pour un credential Odoo (ODOO__PASSWORD, le bloc unique lu par runtime.odoo, #439)
tmp_cfg=$(mktemp)
printf 'INSTANCE_SLUG=edn\nELECTRICORE_VERSION=1.7.0\nBACKUPS_PATH=/srv/edn/backups\nODOO__PASSWORD=secret_factice\n' > "$tmp_cfg"
assert_fail "validate_config_env refuse un secret en clair (ODOO__PASSWORD)" validate_config_env "$tmp_cfg" "edn"
rm -f "$tmp_cfg"
# Token bot : BOT__TOKEN confère une capacité ⇒ secret (ADR-0046 §7)
tmp_cfg=$(mktemp)
printf 'INSTANCE_SLUG=edn\nELECTRICORE_VERSION=1.7.0\nBACKUPS_PATH=/srv/edn/backups\nBOT__TOKEN=000000:factice\n' > "$tmp_cfg"
assert_fail "validate_config_env refuse un secret en clair (BOT__TOKEN)" validate_config_env "$tmp_cfg" "edn"
rm -f "$tmp_cfg"
# Mais BOT__NOTIFY_CHAT_ID (routage, pas une capacité) est AUTORISÉ en config.env (ADR-0046 §7)
tmp_cfg=$(mktemp)
printf 'INSTANCE_SLUG=edn\nELECTRICORE_VERSION=1.7.0\nBACKUPS_PATH=/srv/edn/backups\nBOT__NOTIFY_CHAT_ID=-100\n' > "$tmp_cfg"
assert_ok "validate_config_env autorise BOT__NOTIFY_CHAT_ID (routage)" validate_config_env "$tmp_cfg" "edn"
rm -f "$tmp_cfg"
# Trousseau API : une clé API__TROUSSEAU__ en clair dans config.env est une fuite (ADR-0046 §4)
tmp_cfg=$(mktemp)
printf 'INSTANCE_SLUG=edn\nELECTRICORE_VERSION=1.7.0\nBACKUPS_PATH=/srv/edn/backups\nAPI__TROUSSEAU__librewatt__KEY=secret_factice\n' > "$tmp_cfg"
assert_fail "validate_config_env refuse un secret en clair (API__TROUSSEAU__)" validate_config_env "$tmp_cfg" "edn"
rm -f "$tmp_cfg"
# config.env manquant version → échec
tmp_cfg=$(mktemp); printf 'INSTANCE_SLUG=edn\nBACKUPS_PATH=/srv/edn/backups\n' > "$tmp_cfg"
assert_fail "validate_config_env exige ELECTRICORE_VERSION" validate_config_env "$tmp_cfg" "edn"
rm -f "$tmp_cfg"

# Secrets (clair) : trousseau API + SFTP + trousseau AES (ADR-0046 §4)
tmp_sec=$(mktemp)
printf 'API__TROUSSEAU__librewatt__KEY=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\nSFTP__URL=sftp://u:p@h.fr:22/x\nAES__TROUSSEAU__demo__KEY=00112233445566778899aabbccddeeff\n' > "$tmp_sec"
assert_ok   "validate_secrets_plaintext (secrets clairs valides)" validate_secrets_plaintext "$tmp_sec"
rm -f "$tmp_sec"
# Trousseau AES vide → échec
tmp_sec=$(mktemp); printf 'API__TROUSSEAU__librewatt__KEY=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\nSFTP__URL=sftp://u:p@h.fr:22/x\n' > "$tmp_sec"
assert_fail "validate_secrets_plaintext refuse trousseau AES vide" validate_secrets_plaintext "$tmp_sec"
rm -f "$tmp_sec"
# Trousseau API vide → échec (ADR-0046 §4 : ≥ 1 consommateur requis)
tmp_sec=$(mktemp); printf 'SFTP__URL=sftp://u:p@h.fr:22/x\nAES__TROUSSEAU__demo__KEY=00112233445566778899aabbccddeeff\n' > "$tmp_sec"
assert_fail "validate_secrets_plaintext refuse trousseau API vide" validate_secrets_plaintext "$tmp_sec"
rm -f "$tmp_sec"

echo
echo "→ secrets.sh (fake-binaries age-keygen/ssh-keygen/sops/git)"
FAKE_BIN="${FIXTURES_DIR}/fake_bin"
# Sandbox d'instance jetable : SRV_BASE pointe sur un tmp, fakes age/ssh-keygen/sops/git sur PATH.
secrets_root=$(mktemp -d)
export SRV_BASE="$secrets_root"
install -d "${secrets_root}/edn"

# generate_box_identities : génère age.key + ssh_deploy_key (600) + imprime les 2 pubs.
out=$(PATH="${FAKE_BIN}:$PATH" generate_box_identities edn 2>/dev/null)
grep -q "^AGE_PUBLIC_KEY=age1" <<<"$out" && ok "generate_box_identities: imprime la clé age publique" || ko "pas de AGE_PUBLIC_KEY"
grep -q "^SSH_DEPLOY_PUBKEY=ssh-ed25519" <<<"$out" && ok "generate_box_identities: imprime la deploy key SSH publique" || ko "pas de SSH_DEPLOY_PUBKEY"
[[ -f "${secrets_root}/edn/age.key" ]] && ok "generate_box_identities: clé age privée écrite" || ko "age.key absent"
[[ -f "${secrets_root}/edn/ssh_deploy_key" ]] && ok "generate_box_identities: deploy key privée écrite" || ko "ssh_deploy_key absent"
perm=$(stat -c '%a' "${secrets_root}/edn/age.key" 2>/dev/null)
assert_eq "$perm" "600" "generate_box_identities: age.key en 600 (clé privée, ne sort jamais)"
# Idempotent : 2e appel ne régénère pas (clé conservée)
key_before=$(cat "${secrets_root}/edn/age.key")
PATH="${FAKE_BIN}:$PATH" generate_box_identities edn >/dev/null 2>&1
key_after=$(cat "${secrets_root}/edn/age.key")
assert_eq "$key_after" "$key_before" "generate_box_identities: idempotent (clé privée conservée au 2e run)"

# pull_deploy_repo : clone le dépôt privé simulé, relie providers/, tire config.env.
git_src=$(mktemp -d)
install -d "${git_src}/providers/edn"
printf 'INSTANCE_SLUG=edn\nELECTRICORE_VERSION=1.7.0\nBACKUPS_PATH=/srv/edn/backups\n' > "${git_src}/providers/edn/config.env"
printf '#ENC[fake-ciphertext]\n' > "${git_src}/providers/edn/secrets.env"
PATH="${FAKE_BIN}:$PATH" FAKE_GIT_SRC="$git_src" GIT_BIN=git \
    pull_deploy_repo "git@example.test:org/deploy.git" edn >/dev/null 2>&1
[[ -L "${secrets_root}/edn/providers" ]] && ok "pull_deploy_repo: providers/ relié (symlink)" || ko "providers/ non relié"
[[ -f "${secrets_root}/edn/providers/edn/secrets.env" ]] && ok "pull_deploy_repo: secrets.env accessible via providers/<slug>/" || ko "secrets.env inaccessible"
[[ -f "${secrets_root}/edn/config.env" ]] && ok "pull_deploy_repo: config.env clair tiré à la racine du home" || ko "config.env non tiré"
rm -rf "$git_src"

# box_can_decrypt : vrai si clé + secrets présents ET sops réussit ; faux si sops échoue.
PATH="${FAKE_BIN}:$PATH" FAKE_SOPS_FAIL=0 box_can_decrypt edn \
    && ok "box_can_decrypt: vrai quand sops déchiffre (clé destinataire)" || ko "box_can_decrypt devait réussir"
PATH="${FAKE_BIN}:$PATH" FAKE_SOPS_FAIL=1 box_can_decrypt edn \
    && ko "box_can_decrypt: devait échouer si sops échoue (clé non destinataire)" \
    || ok "box_can_decrypt: faux quand sops échoue (deux temps — pas encore destinataire)"

# validate_secrets_env : déchiffre via (fake) sops puis valide le clair.
PATH="${FAKE_BIN}:$PATH" validate_secrets_env "${secrets_root}/edn/providers/edn/secrets.env" "${secrets_root}/edn/age.key" >/dev/null 2>&1 \
    && ok "validate_secrets_env: déchiffre + valide le clair (fake sops)" || ko "validate_secrets_env a échoué"
PATH="${FAKE_BIN}:$PATH" FAKE_SOPS_FAIL=1 validate_secrets_env "${secrets_root}/edn/providers/edn/secrets.env" "${secrets_root}/edn/age.key" >/dev/null 2>&1 \
    && ko "validate_secrets_env: devait échouer si déchiffrement KO" \
    || ok "validate_secrets_env: échec propre si le déchiffrement KO"

unset SRV_BASE
rm -rf "$secrets_root"

echo
echo "→ add-provider.sh (parse + add_recipient + add_provider, fake sops)"
# parse_add_provider_args : exige --provider-dir + --age-pubkey (format age1…)
( parse_add_provider_args --provider-dir providers/edn --age-pubkey age1abcdef >/dev/null 2>&1
  [[ "$OPT_PROVIDER_DIR" == "providers/edn" && "$OPT_AGE_PUBKEY" == "age1abcdef" && "$OPT_NO_UPDATEKEYS" == "0" ]]
) && ok "parse_add_provider_args: --provider-dir + --age-pubkey" || ko "parse_add_provider_args minimal"
assert_fail "parse_add_provider_args: --age-pubkey manquant" parse_add_provider_args --provider-dir providers/edn
assert_fail "parse_add_provider_args: clé non-age rejetée"   parse_add_provider_args --provider-dir p --age-pubkey "ssh-ed25519 AAAA"
( parse_add_provider_args --provider-dir p --age-pubkey age1zzz --no-updatekeys >/dev/null 2>&1
  [[ "$OPT_NO_UPDATEKEYS" == "1" ]]
) && ok "parse_add_provider_args: --no-updatekeys" || ko "parse_add_provider_args --no-updatekeys"

# add_recipient_to_sops : insère un destinataire, idempotent au 2e appel.
prov=$(mktemp -d)
cp "${LIB_DIR}/../providers/example/.sops.yaml.example" "${prov}/.sops.yaml"
add_recipient_to_sops "${prov}/.sops.yaml" "age1nouvelleboxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" >/dev/null 2>&1
grep -q "age1nouvelleboxxx" "${prov}/.sops.yaml" && ok "add_recipient_to_sops: destinataire inséré" || ko "destinataire non inséré"
add_recipient_to_sops "${prov}/.sops.yaml" "age1nouvelleboxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" >/dev/null 2>&1
n=$(grep -c "age1nouvelleboxxx" "${prov}/.sops.yaml")
assert_eq "$n" "1" "add_recipient_to_sops: idempotent (1 seule occurrence après 2 appels)"
# Les DEUX destinataires admin (opérationnel + escrow hors-ligne, ADR-0046 §8) survivent à
# l'ajout d'une box : l'escrow est destinataire permanent de chaque règle (secours re-keying).
grep -q "age1adminops" "${prov}/.sops.yaml" && ok "add_recipient_to_sops: admin opérationnel préservé" || ko "admin opérationnel effacé"
grep -q "age1adminescrow" "${prov}/.sops.yaml" && ok "add_recipient_to_sops: admin escrow préservé (destinataire permanent)" || ko "admin escrow effacé"

# add_provider : ajoute + updatekeys (fake sops) ; --no-updatekeys saute le re-chiffrement.
printf '#ENC[fake]\n' > "${prov}/secrets.env"
PATH="${FAKE_BIN}:$PATH" SOPS_BIN=sops add_provider "$prov" "age1autreboxyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy" 1 >/dev/null 2>&1 \
    && ok "add_provider: ajoute destinataire + updatekeys (fake sops réussit)" || ko "add_provider a échoué"
grep -q "age1autreboxyyy" "${prov}/.sops.yaml" && ok "add_provider: 2e destinataire bien ajouté" || ko "2e destinataire absent"
PATH="${FAKE_BIN}:$PATH" SOPS_BIN=sops add_provider "$prov" "age1encoreunboxzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz" 0 >/dev/null 2>&1 \
    && ok "add_provider: --no-updatekeys ajoute sans re-chiffrer" || ko "add_provider --no-updatekeys"
rm -rf "$prov"

echo
echo "→ .sops.yaml.example : modèle clé escrow admin (ADR-0046 §8, #437)"
SOPS_EX="${LIB_DIR}/../providers/example/.sops.yaml.example"
# Trois destinataires : admin OPÉRATIONNEL + admin ESCROW (hors-ligne) DISTINCTS + box.
n_age=$(grep -cE '^[[:space:]]*- age1' "$SOPS_EX")
assert_eq "$n_age" "3" ".sops.yaml.example: 3 destinataires (admin ops + escrow + box)"
grep -qi "escrow" "$SOPS_EX" && ok ".sops.yaml.example: clé escrow modélisée + commentée" || ko "escrow absent du modèle"

echo
echo "→ smoke.sh (fume l'image Docker — fake docker)"
# Fixtures SOPS factices : le fake docker ne les lit pas (il simule l'entrypoint),
# elles n'ont qu'à exister pour les montages `-v`.
SMOKE_FIX=$(mktemp -d); : > "${SMOKE_FIX}/secrets.env"; : > "${SMOKE_FIX}/test_age.key"
PATH="${FAKE_BIN}:$PATH" DOCKER_BIN=docker SMOKE_FIXTURES_DIR="$SMOKE_FIX" \
    assert_ok "smoke_image: image saine (import + déchiffrement OK) → succès" \
    smoke_image "electricore:test"

# Importabilité cassée → échec qui NOMME la vérification fautive.
out=$(PATH="${FAKE_BIN}:$PATH" DOCKER_BIN=docker SMOKE_FIXTURES_DIR="$SMOKE_FIX" \
    FAKE_SMOKE_IMPORT_FAIL=1 smoke_image "electricore:test" 2>&1); rc=$?
[[ "$rc" -ne 0 ]] && ok "smoke_image: import cassé → exit non-zero" || ko "smoke_image: import cassé devait échouer"
grep -qi "importabilité" <<<"$out" && ok "smoke_image: import cassé → message nomme l'importabilité" || ko "smoke_image: message d'échec import manquant"

# Déchiffrement cassé (entrypoint fail-fast) → échec qui NOMME la vérification fautive.
out=$(PATH="${FAKE_BIN}:$PATH" DOCKER_BIN=docker SMOKE_FIXTURES_DIR="$SMOKE_FIX" \
    FAKE_SMOKE_DECRYPT_FAIL=1 smoke_image "electricore:test" 2>&1); rc=$?
[[ "$rc" -ne 0 ]] && ok "smoke_image: déchiffrement cassé → exit non-zero" || ko "smoke_image: déchiffrement cassé devait échouer"
grep -qi "déchiffrement" <<<"$out" && ok "smoke_image: déchiffrement cassé → message nomme le déchiffrement" || ko "smoke_image: message d'échec déchiffrement manquant"

# Sans tag → erreur d'usage explicite (exit 2, distinct d'un échec de fumée).
( smoke_image >/dev/null 2>&1 ); rc=$?
[[ "$rc" -eq 2 ]] && ok "smoke_image: sans tag → exit 2 (usage)" || ko "smoke_image: sans tag devait être exit 2 (got $rc)"

# Détails PORTEURS (régression silencieuse = faux échec en CI) : on trace les appels docker.
SMOKE_LOG=$(mktemp)
PATH="${FAKE_BIN}:$PATH" DOCKER_BIN=docker SMOKE_FIXTURES_DIR="$SMOKE_FIX" FAKE_DOCKER_LOG="$SMOKE_LOG" \
    smoke_image "electricore:test" >/dev/null 2>&1
# L'import DOIT contourner l'entrypoint SOPS, sinon il fail-fast → faux échec (#434, ADR-0044 §3).
grep -q -- '-e ELECTRICORE_DECRYPT=off' "$SMOKE_LOG" \
    && ok "smoke_image: l'import contourne l'entrypoint (ELECTRICORE_DECRYPT=off, #434)" \
    || ko "smoke_image: l'import ne pose pas ELECTRICORE_DECRYPT=off"
# Le déchiffrement DOIT monter les deux secrets aux chemins attendus par l'entrypoint,
# sinon il fail-fast (chemins par défaut /run/secrets/{secrets.env,age.key}).
grep -Eq -- '-v [^ ]*/secrets\.env:/run/secrets/secrets\.env:ro' "$SMOKE_LOG" \
    && ok "smoke_image: déchiffrement monte secrets.env → /run/secrets/secrets.env" \
    || ko "smoke_image: secrets.env non monté au bon chemin"
grep -Eq -- '-v [^ ]*/test_age\.key:/run/secrets/age\.key:ro' "$SMOKE_LOG" \
    && ok "smoke_image: déchiffrement monte la clé age → /run/secrets/age.key" \
    || ko "smoke_image: clé age non montée au bon chemin"
rm -f "$SMOKE_LOG"

rm -rf "$SMOKE_FIX"

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

# La branche `usermod -aG` EST le correctif (#459) : sans root, on la rend atteignable
# via des stubs getent/id/usermod sur le PATH. Groupe gid 1000 « existant » (getent
# stubé) + <slug> pas encore membre (id stubé) → on vérifie que usermod est invoqué
# avec le bon groupe (résolu depuis le gid) et le bon slug.
stub_dir=$(mktemp -d); usermod_log="${stub_dir}/usermod.args"
cat > "${stub_dir}/getent" <<'STUB'
#!/usr/bin/env bash
[[ "$1" == group ]] && { printf 'edn-data:x:%s:\n' "$2"; exit 0; }
exit 2
STUB
cat > "${stub_dir}/id" <<'STUB'
#!/usr/bin/env bash
[[ "$1" == -nG ]] && { echo users; exit 0; }
exit 0
STUB
cat > "${stub_dir}/usermod" <<'STUB'
#!/usr/bin/env bash
printf '%s\n' "$*" > "$USERMOD_ARGS_FILE"
STUB
chmod +x "${stub_dir}/getent" "${stub_dir}/id" "${stub_dir}/usermod"
( PATH="${stub_dir}:$PATH" USERMOD_ARGS_FILE="$usermod_log" \
    ensure_slug_in_container_group edn >/dev/null 2>&1 )
assert_eq "$(cat "$usermod_log" 2>/dev/null)" "-aG edn-data edn" \
    "ensure_slug_in_container_group: usermod -aG <grp> <slug> quand <slug> non-membre"
rm -rf "$stub_dir"

echo
echo "→ ingestion.sh / _ingestion_parse_job_id (clé réelle de l'API = id, pas job_id)"
# Forme RÉELLE de la réponse POST /ingestion/run (202, IngestionJobResponse, output=null).
assert_eq "$(_ingestion_parse_job_id '{"id":"abc-123","mode":"test","status":"running","started_at":"2026-06-26T16:56:16","finished_at":null,"error":null,"output":null}')" \
    "abc-123" "_ingestion_parse_job_id: extrait id sur la réponse POST réelle"
assert_eq "$(_ingestion_parse_job_id '{"id": "spaced-id","status":"running"}')" \
    "spaced-id" "_ingestion_parse_job_id: tolère l'espace après :"
assert_eq "$(_ingestion_parse_job_id '{"job_id":"old-shape"}')" \
    "" "_ingestion_parse_job_id: ignore l'ancienne clé fictive job_id (régression)"
assert_eq "$(_ingestion_parse_job_id '{}')" \
    "" "_ingestion_parse_job_id: champ absent → vide"

echo
echo "→ ingestion.sh / _ingestion_parse_status"
assert_eq "$(_ingestion_parse_status '{"status":"completed"}')"  "completed" "_ingestion_parse_status: completed"
assert_eq "$(_ingestion_parse_status '{"status":"failed"}')"     "failed"    "_ingestion_parse_status: failed"
assert_eq "$(_ingestion_parse_status '{"status":"running"}')"    "running"   "_ingestion_parse_status: running"
assert_eq "$(_ingestion_parse_status '{"status": "completed"}')" "completed" "_ingestion_parse_status: tolère l'espace"
assert_eq "$(_ingestion_parse_status '{}')"                      ""          "_ingestion_parse_status: champ absent → vide"

echo
echo "→ ingestion.sh / _ingestion_read_scheduler_key (fake sops déchiffre secrets.env)"
# Sandbox jetable : SRV_BASE pointe un tmp avec age.key + providers/<slug>/secrets.env.
# Le fake sops émet un dotenv clair contenant la clé du label "scheduler" → on l'extrait.
ik_root=$(mktemp -d)
install -d "${ik_root}/edn/providers/edn"
: > "${ik_root}/edn/age.key"
printf '#ENC[fake-ciphertext]\n' > "${ik_root}/edn/providers/edn/secrets.env"
assert_eq "$(PATH="${FAKE_BIN}:$PATH" SRV_BASE="$ik_root" _ingestion_read_scheduler_key edn)" \
    "ssssssssssssssssssssssssssssssss" \
    "_ingestion_read_scheduler_key: extrait API__TROUSSEAU__scheduler__KEY (pas un API_KEY inexistant)"
assert_eq "$(PATH="${FAKE_BIN}:$PATH" SRV_BASE="$ik_root" FAKE_SOPS_FAIL=1 _ingestion_read_scheduler_key edn)" \
    "" \
    "_ingestion_read_scheduler_key: vide si sops échoue (clé age non destinataire)"
rm -rf "$ik_root"

echo
echo "→ ingestion.sh / poll_ingestion_job"
# Cas 1 : completed immédiatement → 0
_ingestion_call_get_job() { echo '{"status":"completed"}'; }
assert_ok   "poll_ingestion_job: completed → 0" \
    poll_ingestion_job testslug testkey abc-123 3 0

# Cas 2 : failed immédiatement → 1
_ingestion_call_get_job() { echo '{"status":"failed"}'; }
assert_fail "poll_ingestion_job: failed → 1" \
    poll_ingestion_job testslug testkey abc-123 3 0

# Cas 3 : timeout (toujours running après max_retries) → 1
_ingestion_call_get_job() { echo '{"status":"running"}'; }
assert_fail "poll_ingestion_job: timeout → 1" \
    poll_ingestion_job testslug testkey abc-123 3 0

# Cas 4 : running × 2 puis completed → 0 (état via fichier, survit aux subshells)
_poll_seq=$(mktemp)
printf 'running\nrunning\ncompleted\n' > "$_poll_seq"
_ingestion_call_get_job() {
    local s; s=$(head -1 "$_poll_seq")
    { tail -n +2 "$_poll_seq" > "${_poll_seq}.tmp" && mv "${_poll_seq}.tmp" "$_poll_seq"; } 2>/dev/null || true
    printf '{"status":"%s"}\n' "${s:-running}"
}
assert_ok   "poll_ingestion_job: running×2 puis completed → 0" \
    poll_ingestion_job testslug testkey abc-123 5 0
rm -f "$_poll_seq" "${_poll_seq}.tmp"

echo
if [[ "$FAIL" -eq 0 ]]; then
    printf "\033[32m%d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
    exit 0
else
    printf "\033[31m%d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
    exit 1
fi
