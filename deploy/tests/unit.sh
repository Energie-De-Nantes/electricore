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
# shellcheck source=../lib/user.sh
source "${LIB_DIR}/user.sh"
# shellcheck source=../lib/relais.sh
source "${LIB_DIR}/relais.sh"

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

echo "→ validate.sh (args CLI seulement — le format des secrets vit en pydantic, ADR-0049)"
assert_ok   "slug 'edn'"                       validate_slug edn
assert_ok   "slug 'enargia-test'"              validate_slug enargia-test
assert_fail "slug 'EDN' (majuscules)"          validate_slug EDN
assert_fail "slug 'edn_test' (underscore)"     validate_slug edn_test
assert_fail "slug 'e' (trop court)"            validate_slug e
assert_fail "slug vide"                        validate_slug ""

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
echo "→ harden.sh / sshd_preflight_parse_passwordauth (AC5 — couture awk, vrai dump sshd -T)"
SSHD_PREFLIGHT_FIX="${FIXTURES_DIR}/sshd_preflight"
assert_eq "$(sshd_preflight_parse_passwordauth < "${SSHD_PREFLIGHT_FIX}/sshd-T-dump.txt")" \
    "no" "extrait passwordauthentication depuis un vrai dump sshd -T"

echo
echo "→ harden.sh / sshd_preflight_effective_passwordauth (portabilité Match Address, #656 revue)"
if command -v sshd >/dev/null 2>&1; then
    _match_dir="$(mktemp -d)"
    ssh-keygen -t ed25519 -N '' -f "${_match_dir}/hostkey" -q </dev/null >/dev/null 2>&1
    _match_conf="${_match_dir}/sshd_config"
    { printf 'HostKey %s\n' "${_match_dir}/hostkey"; cat "${SSHD_PREFLIGHT_FIX}/sshd_config_match_address"; } > "$_match_conf"
    assert_eq "$(sshd_preflight_effective_passwordauth "$_match_conf" testuser)" \
        "no" "config avec bloc Match Address + -C complet → sshd -T conclut (pas de fatal(), #656 revue)"
    rm -rf "$_match_dir"
else
    echo "  (binaire sshd absent — test Match Address sauté)"
fi

echo
echo "→ harden.sh / sshd_preflight_at_risk_accounts (préflight non-vierge, #656)"
assert_eq "$(sshd_preflight_at_risk_accounts < "${SSHD_PREFLIGHT_FIX}/password-account.records")" \
    "enedis_deposit" "compte au mot de passe, sans clé, bascule yes→no → à risque"
assert_eq "$(sshd_preflight_at_risk_accounts < "${SSHD_PREFLIGHT_FIX}/keyed-account.records")" \
    "" "compte migré en clé → pas à risque"
assert_eq "$(sshd_preflight_at_risk_accounts < "${SSHD_PREFLIGHT_FIX}/matched-account.records")" \
    "" "bloc Match protecteur (yes avant ET après, pas de bascule) → pas à risque"
assert_eq "$(sshd_preflight_at_risk_accounts < "${SSHD_PREFLIGHT_FIX}/virgin.records")" \
    "" "machine vierge (comptes verrouillés + admin en clé) → aucun compte à risque"
# root : hors audit, juridiction du garde-fou anti-verrouillage (finding 1, #656 revue)
assert_eq "$(sshd_preflight_at_risk_accounts < "${SSHD_PREFLIGHT_FIX}/root-account.records")" \
    "" "root au mot de passe qui basculerait yes→no → PAS signalé (juridiction du garde-fou)"
# Box déjà durcie (reconfigure) : avant=no déjà (le compte est déjà coupé sans le
# nouveau drop-in) → no→no, pas de bascule → silencieux (finding 3, #656 revue)
assert_eq "$(sshd_preflight_at_risk_accounts < "${SSHD_PREFLIGHT_FIX}/already-hardened.records")" \
    "" "box déjà durcie (avant=no → après=no) → pas signalé, reconfigure silencieux"
# Comptes système sans login (shell nologin/false) : la fonction ne regarde QUE
# passwd -S/clé/effective, jamais le shell — mais un compte verrouillé (L) ou sans
# mot de passe (NP) ne doit jamais remonter, quel que soit le reste (#656 AC4).
assert_eq "$(printf 'www-data:L:0:yes:no\n' | sshd_preflight_at_risk_accounts)" \
    "" "compte système verrouillé (L) → pas de faux positif"
assert_eq "$(printf 'guest:NP:0:yes:no\n' | sshd_preflight_at_risk_accounts)" \
    "" "compte sans mot de passe (NP) → pas de faux positif"
# Plusieurs comptes à risque à la fois → tous nommés (un par ligne)
assert_eq "$(printf 'a:P:0:yes:no\nb:L:0:yes:no\nc:P:0:yes:no\n' | sshd_preflight_at_risk_accounts)" \
    "$(printf 'a\nc')" "plusieurs comptes à risque → tous remontés"

echo
echo "→ harden.sh / sshd_preflight_oracle_failed_accounts (fail-closed, #656 revue)"
assert_eq "$(sshd_preflight_oracle_failed_accounts < "${SSHD_PREFLIGHT_FIX}/oracle-failure.records")" \
    "$(printf 'broken_avant\nbroken_apres')" "effective vide (avant OU après) sur un candidat → oracle en échec, les deux remontés"
assert_eq "$(sshd_preflight_oracle_failed_accounts < "${SSHD_PREFLIGHT_FIX}/password-account.records")" \
    "" "oracle qui répond (yes/no bien formés) → pas d'échec"
assert_eq "$(printf 'root:P:0::no\n' | sshd_preflight_oracle_failed_accounts)" \
    "" "root exclu même en échec d'oracle (hors audit)"
assert_eq "$(printf 'svc:L:0::no\n' | sshd_preflight_oracle_failed_accounts)" \
    "" "compte non-candidat (verrouillé) → pas concerné par le fail-closed"

echo
echo "→ harden.sh / sshd_preflight_refuse_if_at_risk (bloque AVANT tout changement, #656)"
# sshd_preflight_collect est surchargée (même précédent que poll_ingestion_job /
# _ingestion_call_get_job) : on teste la composition collecte→décision→die, pas le
# vrai collecteur (impur, nécessite root+sshd — couvert par l'e2e multipass).
out=$(sshd_preflight_collect() { printf 'enedis_deposit:P:0:yes:no\n'; }; sshd_preflight_refuse_if_at_risk 2>&1)
rc=$?
[[ "$rc" -ne 0 ]] && ok "compte à risque → refuse (die, exit non-zero)" || ko "compte à risque aurait dû refuser"
grep -q "enedis_deposit" <<<"$out" && ok "message de refus nomme le compte à risque" || ko "message de refus ne nomme pas le compte"

out=$(sshd_preflight_collect() { printf 'ops:P:1:yes:no\nroot:L:0:no:no\n'; }; sshd_preflight_refuse_if_at_risk 2>&1)
rc=$?
[[ "$rc" -eq 0 ]] && ok "machine vierge (aucun compte à risque) → passe (exit 0)" || ko "machine vierge n'aurait pas dû refuser"

out=$(sshd_preflight_collect() { printf 'root:P:0:yes:no\n'; }; sshd_preflight_refuse_if_at_risk 2>&1)
rc=$?
[[ "$rc" -eq 0 ]] && ok "root au mot de passe qui basculerait → n'est PAS ce qui refuse (finding 1)" || ko "root n'aurait pas dû faire refuser le préflight"

out=$(sshd_preflight_collect() { printf 'legacy_svc:P:0:no:no\n'; }; sshd_preflight_refuse_if_at_risk 2>&1)
rc=$?
[[ "$rc" -eq 0 ]] && ok "box déjà durcie (no→no) → reconfigure passe silencieusement (finding 3)" || ko "box déjà durcie aurait dû passer"

# Fail-closed : l'oracle ne répond pas pour un compte candidat → refuse, message DÉDIÉ
# (pas celui des comptes à risque — ses remédiations Match/clé ne s'appliquent pas ici).
out=$(sshd_preflight_collect() { printf 'sonde_muette:P:0::no\n'; }; sshd_preflight_refuse_if_at_risk 2>&1)
rc=$?
[[ "$rc" -ne 0 ]] && ok "oracle en échec sur un candidat → refuse (fail-closed)" || ko "oracle en échec aurait dû refuser"
grep -q "impossible de conclure" <<<"$out" && ok "message de refus dédié (pas le message comptes à risque)" || ko "message de refus fail-closed absent/incorrect"
grep -q "sonde_muette" <<<"$out" && ok "message fail-closed nomme le compte" || ko "message fail-closed ne nomme pas le compte"
! grep -q "Remédier : migrer" <<<"$out" && ok "message fail-closed n'affiche PAS les remédiations comptes-à-risque" || ko "message fail-closed a affiché les remédiations comptes-à-risque à tort"

# Dates de dernier login mdp (demande de Virgile, #656 revue) : seam surchargeable,
# même précédent que sshd_preflight_collect — la logique refuse/passe n'en dépend pas
# (aide à la décision uniquement), mais le message de refus doit les afficher.
out=$(
    sshd_preflight_collect() { printf 'enedis_deposit:P:0:yes:no\n'; }
    sshd_preflight_last_password_login() { printf '2026-03-14'; }
    sshd_preflight_refuse_if_at_risk 2>&1
)
grep -q "2026-03-14" <<<"$out" && ok "message comptes-à-risque affiche la date de dernier login mdp" || ko "date de dernier login absente du message comptes-à-risque"

out=$(
    sshd_preflight_collect() { printf 'sonde_muette:P:0::no\n'; }
    sshd_preflight_last_password_login() { printf 'aucun login mdp sur la fenêtre du journal (depuis 2025-01-01)'; }
    sshd_preflight_refuse_if_at_risk 2>&1
)
grep -q "2025-01-01" <<<"$out" && ok "message fail-closed affiche aussi la date de dernier login mdp" || ko "date de dernier login absente du message fail-closed"

echo
echo "→ harden.sh / sshd_preflight_last_password_login (borne à la fenêtre observable, #656 revue)"
assert_eq "$(sshd_preflight_last_password_login inconnu_$$ 2>/dev/null | grep -c "jamais utilisé")" \
    "0" "ne dit JAMAIS 'jamais utilisé' — borné à la fenêtre observable du journal"

echo
echo "→ relais.sh / relais_ssh_key_path + relais_ssh_key_mode_ok (#657)"
assert_eq "$(SRV_BASE=/srv relais_ssh_key_path edn)" "/srv/edn/relais_ssh_key" \
    "relais_ssh_key_path: chemin de convention (racine du home d'instance)"

rk_root=$(mktemp -d)
rk_path="${rk_root}/relais_ssh_key"
: > "$rk_path"; chmod 600 "$rk_path"
assert_ok "relais_ssh_key_mode_ok: 600 → ok" relais_ssh_key_mode_ok "$rk_path"
chmod 400 "$rk_path"
assert_ok "relais_ssh_key_mode_ok: 400 (lecture seule) → ok aussi" relais_ssh_key_mode_ok "$rk_path"
chmod 640 "$rk_path"
assert_fail "relais_ssh_key_mode_ok: 640 (lisible par le groupe) → refuse" relais_ssh_key_mode_ok "$rk_path"
chmod 644 "$rk_path"
assert_fail "relais_ssh_key_mode_ok: 644 (lisible par tous) → refuse" relais_ssh_key_mode_ok "$rk_path"
assert_fail "relais_ssh_key_mode_ok: fichier absent → refuse" relais_ssh_key_mode_ok "${rk_root}/nonexistent"
rm -rf "$rk_root"

echo
echo "→ relais.sh / check_relais_ssh_key (refus AVANT tout, jamais de génération/copie, #657 AC3)"
ck_root=$(mktemp -d)
install -d "${ck_root}/edn"
out=$(SRV_BASE="$ck_root" check_relais_ssh_key edn 2>&1); rc=$?
[[ "$rc" -ne 0 ]] && ok "check_relais_ssh_key: clé absente → refuse (die, exit non-zero)" || ko "check_relais_ssh_key aurait dû refuser (clé absente)"
grep -q "relais_ssh_key" <<<"$out" && ok "check_relais_ssh_key: message nomme le chemin de remédiation" || ko "check_relais_ssh_key: chemin absent du message de refus"

: > "${ck_root}/edn/relais_ssh_key"; chmod 644 "${ck_root}/edn/relais_ssh_key"
out=$(SRV_BASE="$ck_root" check_relais_ssh_key edn 2>&1); rc=$?
[[ "$rc" -ne 0 ]] && ok "check_relais_ssh_key: droits trop ouverts (644) → refuse" || ko "check_relais_ssh_key aurait dû refuser (644)"

chmod 600 "${ck_root}/edn/relais_ssh_key"
( CONTAINER_UID="$(id -u)" CONTAINER_GID="$(id -g)" SRV_BASE="$ck_root" check_relais_ssh_key edn >/dev/null 2>&1 ) \
    && ok "check_relais_ssh_key: présente + 600 → passe" || ko "check_relais_ssh_key aurait dû passer (600)"
assert_eq "$(stat -c '%u' "${ck_root}/edn/relais_ssh_key")" "$(id -u)" \
    "check_relais_ssh_key: aligne l'ownership sur CONTAINER_UID (lecture conteneur, uid 1000/home /app)"
rm -rf "$ck_root"

echo
echo "→ relais.sh / render_relais_compose (mini-compose, pur, #657)"
compose_out="$(render_relais_compose)"
grep -q 'RELAIS_VERSION' <<<"$compose_out" && ok "render_relais_compose: tag pinné par RELAIS_VERSION" || ko "render_relais_compose RELAIS_VERSION absent"
grep -q 'relais_ssh_key:/app/.ssh/id_ed25519:ro' <<<"$compose_out" \
    && ok "render_relais_compose: clé SSH montée au nom par défaut paramiko (id_ed25519, home /app)" \
    || ko "render_relais_compose: montage clé SSH absent/incorrect"
grep -q 'relais_data:/data' <<<"$compose_out" \
    && ok "render_relais_compose: journal DuckDB sur volume nommé, monté sur /data (chowné electricore par le Dockerfile)" \
    || ko "render_relais_compose: volume relais_data absent"
grep -q 'run --rm relais' <<<"$compose_out" && ok "render_relais_compose: documente l'invocation run --rm (pas up -d)" || ko "render_relais_compose: invocation run --rm non documentée"
grep -q 'age.key:/run/secrets/age.key:ro' <<<"$compose_out" \
    && ok "render_relais_compose: trousseau AES mutualisé — même age.key que la stack" \
    || ko "render_relais_compose: montage age.key absent"
grep -qx 'name: electricore-relais' <<<"$compose_out" \
    && ok "render_relais_compose: name: explicite (namespace stable, indépendant du dossier)" \
    || ko "render_relais_compose: name: top-level absent"
grep -qF -- '- ${FLUX_DEPOSIT_DIR:-/srv/electricore/flux-deposit}:${FLUX_DEPOSIT_DIR:-/srv/electricore/flux-deposit}:ro' <<<"$compose_out" \
    && ok "render_relais_compose: dépôt local des flux monté ro (même chemin dedans/dehors, FLUX_DEPOSIT_DIR)" \
    || ko "render_relais_compose: montage flux-deposit absent/incorrect"

echo
echo "→ relais.sh / relais_etat_vierge (garde #673 : timer jamais armé sans amorce)"
# Fake docker : `volume inspect` répond le mountpoint contrôlé par le test, ou échoue
# (volume absent) si le drapeau `absent` existe — la garde ne dépend de docker que
# pour résoudre ce chemin.
rv_root=$(mktemp -d)
rv_bin="${rv_root}/bin"; rv_mp="${rv_root}/volume"
mkdir -p "$rv_bin" "$rv_mp"
cat > "${rv_bin}/docker" <<EOF
#!/usr/bin/env bash
[[ -f "${rv_root}/absent" ]] && { echo "Error: no such volume" >&2; exit 1; }
echo "${rv_mp}"
EOF
chmod +x "${rv_bin}/docker"

: > "${rv_root}/absent"
PATH="${rv_bin}:$PATH" relais_etat_vierge \
    && ok "relais_etat_vierge: volume docker absent → vierge" \
    || ko "relais_etat_vierge: volume absent aurait dû être vierge"
rm "${rv_root}/absent"
PATH="${rv_bin}:$PATH" relais_etat_vierge \
    && ok "relais_etat_vierge: volume vide (aucun journal) → vierge" \
    || ko "relais_etat_vierge: volume vide aurait dû être vierge"
touch "${rv_mp}/relais.duckdb"
PATH="${rv_bin}:$PATH" relais_etat_vierge \
    && ko "relais_etat_vierge: journal présent aurait dû casser la virginité" \
    || ok "relais_etat_vierge: journal présent → pas vierge (timer armable)"

echo
echo "→ relais.sh / ensure_relais_flux_deposit (dépôt file://, jamais modifié si existant, #657)"
fd_root=$(mktemp -d); mkdir -p "$fd_root/edn"
( CONTAINER_UID="$(id -u)" CONTAINER_GID="$(id -g)" ensure_relais_flux_deposit "$fd_root/edn/flux-deposit" >/dev/null 2>&1 )
[[ -d "$fd_root/edn/flux-deposit" ]] && ok "ensure_relais_flux_deposit: crée le dépôt absent" || ko "ensure_relais_flux_deposit: dépôt non créé"
[[ "$(stat -c '%a' "$fd_root/edn/flux-deposit")" == "750" ]] && ok "ensure_relais_flux_deposit: créé en 750 (lecture conteneur)" || ko "ensure_relais_flux_deposit: mode inattendu ($(stat -c '%a' "$fd_root/edn/flux-deposit"))"
chmod 700 "$fd_root/edn/flux-deposit"
( ensure_relais_flux_deposit "$fd_root/edn/flux-deposit" >/dev/null 2>&1 )
[[ "$(stat -c '%a' "$fd_root/edn/flux-deposit")" == "700" ]] && ok "ensure_relais_flux_deposit: dépôt existant laissé tel quel (prod SFTP Enargia)" || ko "ensure_relais_flux_deposit: a modifié un dépôt existant"
rm -rf "$fd_root"

echo
echo "→ relais.sh / render_relais_service (unité systemd ADAPTÉE — compose run, #657)"
svc_out="$(render_relais_service edn)"
grep -qx "User=edn" <<<"$svc_out" && ok "render_relais_service: User=<slug>" || ko "render_relais_service User manquant/incorrect"
grep -q "WorkingDirectory=/srv/edn/deploy/relais" <<<"$svc_out" && ok "render_relais_service: WorkingDirectory=/srv/<slug>/deploy/relais" || ko "render_relais_service WorkingDirectory incorrect"
grep -q 'docker compose --env-file ../../config.env -f compose-relais.yml run --rm relais' <<<"$svc_out" \
    && ok "render_relais_service: ExecStart appelle docker compose run --rm (escalade #657 AC6 : exit non-zero traverse jusqu'à failed)" \
    || ko "render_relais_service ExecStart incorrect"
grep -qx "Type=oneshot" <<<"$svc_out" && ok "render_relais_service: Type=oneshot" || ko "render_relais_service Type incorrect"
grep -qx "OnFailure=electricore-relais-alerte.service" <<<"$svc_out" \
    && ok "render_relais_service: OnFailure=electricore-relais-alerte.service (#668)" \
    || ko "render_relais_service: OnFailure= absent/incorrect"
svc_out2="$(render_relais_service enargia)"
grep -qx "User=enargia" <<<"$svc_out2" && ok "render_relais_service: paramétré par slug (2e instance)" || ko "render_relais_service pas paramétré par slug"

echo
echo "→ relais.sh / render_relais_alerte_service (EnvironmentFile sur le layout conteneurisé #657, #668)"
alerte_svc_out="$(render_relais_alerte_service edn)"
grep -qx "EnvironmentFile=/srv/edn/config.env" <<<"$alerte_svc_out" \
    && ok "render_relais_alerte_service: EnvironmentFile=/srv/<slug>/config.env" \
    || ko "render_relais_alerte_service: EnvironmentFile incorrect (résidu /etc/electricore-relais/relais.env ?)"
grep -q '/etc/electricore-relais/relais.env' <<<"$alerte_svc_out" \
    && ko "render_relais_alerte_service: chemin bare-metal /etc/electricore-relais/relais.env résiduel" \
    || ok "render_relais_alerte_service: aucun résidu /etc/electricore-relais/relais.env"
grep -qx "ExecStart=/usr/local/bin/electricore-relais-alerte.sh" <<<"$alerte_svc_out" \
    && ok "render_relais_alerte_service: ExecStart=/usr/local/bin/electricore-relais-alerte.sh" \
    || ko "render_relais_alerte_service: ExecStart incorrect"
grep -qx "Type=oneshot" <<<"$alerte_svc_out" && ok "render_relais_alerte_service: Type=oneshot" || ko "render_relais_alerte_service: Type incorrect"
alerte_svc_out2="$(render_relais_alerte_service enargia)"
grep -qx "EnvironmentFile=/srv/enargia/config.env" <<<"$alerte_svc_out2" \
    && ok "render_relais_alerte_service: paramétré par slug (2e instance)" \
    || ko "render_relais_alerte_service: pas paramétré par slug"

echo
echo "→ relais.sh / render_relais_alerte_script (hook shell+msmtp, statique, #659/#668)"
alerte_sh_out="$(render_relais_alerte_script)"
grep -qx '#!/usr/bin/env bash' <<<"$alerte_sh_out" && ok "render_relais_alerte_script: shebang bash" || ko "render_relais_alerte_script: shebang absent/incorrect"
grep -q 'RELAIS_ALERTE_MAILS' <<<"$alerte_sh_out" && ok "render_relais_alerte_script: lit RELAIS_ALERTE_MAILS" || ko "render_relais_alerte_script: RELAIS_ALERTE_MAILS absent"
grep -q 'msmtp' <<<"$alerte_sh_out" && ok "render_relais_alerte_script: invoque msmtp" || ko "render_relais_alerte_script: msmtp absent"
# Le commentaire du script EXPLIQUE justement l'absence de Python (mots "Python"/"pipeline.py"
# légitimes en commentaire) — seules les lignes de CODE (non-commentaires) comptent ici.
grep -v '^#' <<<"$alerte_sh_out" | grep -qi 'python' \
    && ko "render_relais_alerte_script: invoque python (doit rester shell pur)" \
    || ok "render_relais_alerte_script: aucune invocation Python"
bash -n <(printf '%s\n' "$alerte_sh_out") && ok "render_relais_alerte_script: syntaxe bash valide" || ko "render_relais_alerte_script: bash -n a échoué"

echo
echo "→ relais.sh / render_relais_alerte_msmtprc (msmtprc RENDU, passwordeval sops, #674)"
msmtprc_out="$(SRV_BASE=/srv render_relais_alerte_msmtprc edn smtp.example.fr 587 alertes@example.fr alertes@example.fr)"
grep -qx 'host smtp.example.fr' <<<"$msmtprc_out" && ok "render_relais_alerte_msmtprc: host substitué" || ko "render_relais_alerte_msmtprc: host absent/incorrect"
grep -qx 'port 587' <<<"$msmtprc_out" && ok "render_relais_alerte_msmtprc: port substitué" || ko "render_relais_alerte_msmtprc: port absent/incorrect"
grep -qx 'from alertes@example.fr' <<<"$msmtprc_out" && ok "render_relais_alerte_msmtprc: from substitué" || ko "render_relais_alerte_msmtprc: from absent/incorrect"
grep -qx 'user alertes@example.fr' <<<"$msmtprc_out" && ok "render_relais_alerte_msmtprc: user substitué" || ko "render_relais_alerte_msmtprc: user absent/incorrect"
grep -q '^passwordeval' <<<"$msmtprc_out" && ok "render_relais_alerte_msmtprc: passwordeval présent (token jamais écrit en clair)" || ko "render_relais_alerte_msmtprc: passwordeval absent"
grep -q 'sops decrypt' <<<"$msmtprc_out" && ok "render_relais_alerte_msmtprc: extraction via un sops decrypt hôte" || ko "render_relais_alerte_msmtprc: sops decrypt absent"
grep -q 'SOPS_AGE_KEY_FILE=/srv/edn/age.key' <<<"$msmtprc_out" && ok "render_relais_alerte_msmtprc: SOPS_AGE_KEY_FILE pointe la clé age de la box" || ko "render_relais_alerte_msmtprc: chemin age.key absent/incorrect"
grep -q '/srv/edn/providers/edn/secrets.env' <<<"$msmtprc_out" && ok "render_relais_alerte_msmtprc: cible providers/<slug>/secrets.env" || ko "render_relais_alerte_msmtprc: chemin secrets.env absent/incorrect"
grep -q 'ALERTE__SMTP__PASSWORD' <<<"$msmtprc_out" && ok "render_relais_alerte_msmtprc: extrait le champ ALERTE__SMTP__PASSWORD" || ko "render_relais_alerte_msmtprc: nom de champ absent"
grep -q 'pipefail' <<<"$msmtprc_out" && ok "render_relais_alerte_msmtprc: pipefail — un échec sops se propage (pas de mot de passe vide avalé en silence)" || ko "render_relais_alerte_msmtprc: pipefail absent"
grep -qE '^[[:space:]]*password[[:space:]]' <<<"$msmtprc_out" && ko "render_relais_alerte_msmtprc: directive password= en clair (fuite du token)" || ok "render_relais_alerte_msmtprc: aucune directive password= en clair"
msmtprc_out2="$(render_relais_alerte_msmtprc enargia smtp2.example.fr 25 a@b.fr a@b.fr)"
grep -q '/srv/enargia/age.key' <<<"$msmtprc_out2" && ok "render_relais_alerte_msmtprc: paramétré par slug (2e instance)" || ko "render_relais_alerte_msmtprc: pas paramétré par slug"
msmtprc_out3="$(render_relais_alerte_msmtprc edn smtp.example.fr 587 alertes@example.fr alertes@example.fr)"
[[ "$msmtprc_out" == "$msmtprc_out3" ]] && ok "render_relais_alerte_msmtprc: rendu idempotent (mêmes entrées → même sortie, re-run sûr)" || ko "render_relais_alerte_msmtprc: rendu non déterministe"

echo
echo "→ relais.sh / msmtprc passwordeval — comportement stubé (#674 : token jamais en clair, extraction sops)"
pwdeval_cmd=$(sed -n 's/^passwordeval //p' <<<"$msmtprc_out")
[[ -n "$pwdeval_cmd" ]] && ok "passwordeval : commande extraite du msmtprc rendu" || ko "passwordeval : commande introuvable dans le msmtprc rendu"

pw_stub_dir=$(mktemp -d)
cat > "${pw_stub_dir}/sops" <<'STUB'
#!/usr/bin/env bash
# Stub sops : ignore ses arguments, émet le secrets.env déchiffré factice (mêmes noms
# de champs que providers/example/secrets.env.example, #674).
printf 'ALERTE__SMTP__PASSWORD=stubbed_token_xyz\n'
STUB
chmod +x "${pw_stub_dir}/sops"
pw_out=$(PATH="${pw_stub_dir}:$PATH" bash -c "$pwdeval_cmd" 2>/dev/null)
[[ "$pw_out" == "stubbed_token_xyz" ]] \
    && ok "passwordeval (sops stubbé) : extrait exactement la valeur ALERTE__SMTP__PASSWORD" \
    || ko "passwordeval (sops stubbé) : sortie inattendue (got '${pw_out}')"

# Échec du sops hôte (#674 : log explicite, pas de crash silencieux) : la commande doit
# échouer (exit non-zéro) plutôt que produire un mot de passe vide accepté en silence —
# c'est ce code de sortie que msmtp lit pour détecter l'échec de passwordeval.
cat > "${pw_stub_dir}/sops" <<'STUB'
#!/usr/bin/env bash
echo "sops: erreur de déchiffrement (clé age invalide, stub)" >&2
exit 1
STUB
chmod +x "${pw_stub_dir}/sops"
PATH="${pw_stub_dir}:$PATH" bash -c "$pwdeval_cmd" >/dev/null 2>"${pw_stub_dir}/err"
pw_rc=$?
[[ "$pw_rc" -ne 0 ]] && ok "passwordeval : sops hôte en échec → code de sortie non-zéro (pas de crash silencieux)" \
                      || ko "passwordeval : échec sops non propagé (exit ${pw_rc})"
[[ -s "${pw_stub_dir}/err" ]] && ok "passwordeval : le sops stubbé en échec loggue sur stderr (capté par journalctl)" \
                               || ko "passwordeval : aucun message d'erreur sur stderr"

# Champ absent de secrets.env (sops OK, ALERTE__SMTP__PASSWORD manquant) : sans garde,
# sed n'émet rien et rc=0 → msmtp partirait authentifier avec un mot de passe VIDE
# (erreur d'auth confuse, loin de secrets.env). Le `| grep .` final transforme
# l'extraction vide en échec explicite de passwordeval (arbitrage revue #675).
cat > "${pw_stub_dir}/sops" <<'STUB'
#!/usr/bin/env bash
printf 'AUTRE_CHAMP=valeur\n'
STUB
chmod +x "${pw_stub_dir}/sops"
PATH="${pw_stub_dir}:$PATH" bash -c "$pwdeval_cmd" >/dev/null 2>&1
pw_rc=$?
[[ "$pw_rc" -ne 0 ]] && ok "passwordeval : champ absent de secrets.env → échec explicite (extraction vide ≠ mot de passe vide)" \
                      || ko "passwordeval : extraction vide acceptée en silence (exit ${pw_rc})"
rm -rf "$pw_stub_dir"

echo
echo "→ relais.sh / systemd-analyze verify (rendu réel, #668 AC1)"
if command -v systemd-analyze >/dev/null 2>&1; then
    sdv_dir=$(mktemp -d)
    # ExecStart de l'unité d'alerte est un chemin fixe hors-repo (/usr/local/bin/...),
    # posé en prod par install_relais_alerte_units (jamais par ces tests) — substitué
    # ici par un stub exécutable local, pour ne vérifier que la SYNTAXE de l'unité.
    printf '#!/usr/bin/env bash\nexit 0\n' > "${sdv_dir}/electricore-relais-alerte.sh"
    chmod +x "${sdv_dir}/electricore-relais-alerte.sh"
    render_relais_alerte_service edn \
        | sed "s#/usr/local/bin/electricore-relais-alerte.sh#${sdv_dir}/electricore-relais-alerte.sh#" \
        > "${sdv_dir}/electricore-relais-alerte.service"
    systemd-analyze verify "${sdv_dir}/electricore-relais-alerte.service" 2>"${sdv_dir}/err" \
        && ok "systemd-analyze verify: electricore-relais-alerte.service" \
        || ko "systemd-analyze verify: electricore-relais-alerte.service ($(cat "${sdv_dir}/err"))"
    if [[ -x /usr/bin/docker ]]; then
        render_relais_service edn > "${sdv_dir}/electricore-relais.service"
        systemd-analyze verify "${sdv_dir}/electricore-relais.service" 2>"${sdv_dir}/err2" \
            && ok "systemd-analyze verify: electricore-relais.service (OnFailure= compris)" \
            || ko "systemd-analyze verify: electricore-relais.service ($(cat "${sdv_dir}/err2"))"
    else
        log_skip "systemd-analyze verify (electricore-relais.service) sauté : /usr/bin/docker absent de cet environnement"
    fi
    rm -rf "$sdv_dir"
else
    log_skip "systemd-analyze indisponible dans cet environnement — rendu couvert par les tests grep ci-dessus (#668)"
fi

echo
echo "→ relais.sh / render_relais_timer (unité systemd, pure, #657)"
timer_out="$(render_relais_timer)"
grep -qx "Unit=electricore-relais.service" <<<"$timer_out" && ok "render_relais_timer: cible electricore-relais.service" || ko "render_relais_timer Unit= incorrect"
grep -qx "Persistent=true" <<<"$timer_out" && ok "render_relais_timer: Persistent=true (rattrape un boot manqué)" || ko "render_relais_timer Persistent manquant"
grep -qx "OnActiveSec=1min" <<<"$timer_out" \
    && ok "render_relais_timer: OnActiveSec=1min (un timer démarré à froid planifie, #682)" \
    || ko "render_relais_timer OnActiveSec manquant — start à froid = interblocage jusqu'au reboot"
grep -qx "WantedBy=timers.target" <<<"$timer_out" && ok "render_relais_timer: WantedBy=timers.target" || ko "render_relais_timer WantedBy manquant"

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

# Composant relais (#657) : --domain devient optionnel avec --relais (socle commun +
# composant relais seul, pas de domaine ni de Caddy) ; sans --relais, comportement
# stack intégralement préservé (--domain reste obligatoire).
( parse_args --slug edn --domain edn.fr --deploy-repo "git@example.test:org/deploy.git" >/dev/null 2>&1
  [[ "$OPT_RELAIS" == "0" ]]
) && ok "parse_args: OPT_RELAIS=0 par défaut (stack, compat intégrale)" || ko "parse_args OPT_RELAIS défaut"

( parse_args --slug edn --relais --deploy-repo "git@example.test:org/deploy.git" >/dev/null 2>&1
  [[ "$OPT_RELAIS" == "1" && -z "$OPT_DOMAIN" ]]
) && ok "parse_args: --relais sans --domain → accepté (OPT_RELAIS=1)" || ko "parse_args --relais sans --domain aurait dû être accepté"

( parse_args --slug edn --relais --domain edn.fr --deploy-repo "git@example.test:org/deploy.git" >/dev/null 2>&1
  [[ "$OPT_RELAIS" == "1" && "$OPT_DOMAIN" == "edn.fr" ]]
) && ok "parse_args: --relais avec --domain fourni quand même → accepté, capturé" || ko "parse_args --relais avec --domain"

assert_fail "parse_args sans --domain NI --relais → refuse toujours (stack inchangée)" \
    parse_args --slug edn --deploy-repo "git@example.test:org/deploy.git"

assert_fail "parse_args --relais sans --deploy-repo → refuse (identité/secrets mutualisés requis)" \
    parse_args --slug edn --relais --domain edn.fr

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
[[ -f "${tmp_target}/log.sh" && -f "${tmp_target}/cli.sh" && -f "${tmp_target}/config.sh" && -f "${tmp_target}/secrets.sh" && -f "${tmp_target}/harden.sh" && -f "${tmp_target}/relais.sh" ]] \
    && ok "fetch_lib_files: les 14 helpers sont téléchargés au 1er appel" \
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
# Token SMTP de l'alerte relais : ALERTE__SMTP__PASSWORD confère une capacité ⇒ secret (#674, ADR-0046 §7)
tmp_cfg=$(mktemp)
printf 'INSTANCE_SLUG=edn\nELECTRICORE_VERSION=1.7.0\nBACKUPS_PATH=/srv/edn/backups\nALERTE__SMTP__PASSWORD=token_factice\n' > "$tmp_cfg"
assert_fail "validate_config_env refuse un secret en clair (ALERTE__SMTP__PASSWORD, #674)" validate_config_env "$tmp_cfg" "edn"
rm -f "$tmp_cfg"
# Mais ALERTE__SMTP__{HOST,PORT,FROM,USER} (routage SMTP, pas une capacité) sont AUTORISÉS
# en config.env (#674, ADR-0046 §7 — même split que BOT__NOTIFY_CHAT_ID ci-dessus)
tmp_cfg=$(mktemp)
printf 'INSTANCE_SLUG=edn\nELECTRICORE_VERSION=1.7.0\nBACKUPS_PATH=/srv/edn/backups\nALERTE__SMTP__HOST=smtp.example.fr\nALERTE__SMTP__PORT=587\nALERTE__SMTP__FROM=alertes@example.fr\nALERTE__SMTP__USER=alertes@example.fr\n' > "$tmp_cfg"
assert_ok "validate_config_env autorise ALERTE__SMTP__{HOST,PORT,FROM,USER} (routage, #674)" validate_config_env "$tmp_cfg" "edn"
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

# component="relais" (#657) : exige RELAIS_VERSION, PAS ELECTRICORE_VERSION/BACKUPS_PATH
# (une box relais-seul n'a pas de stack) — le config.env est PARTAGÉ par les deux
# composants, chacun ne réclame que ses propres clés.
tmp_cfg=$(mktemp)
printf 'INSTANCE_SLUG=edn\nRELAIS_VERSION=1.2.0\nRELAIS__SOURCE_URL=file:///srv/edn/flux\nFLUX_DEPOSIT_DIR=/srv/edn/flux\nRELAIS__PARTNER_URL=sftp://relais@partenaire.example/in\n' > "$tmp_cfg"
assert_ok "validate_config_env (component=relais) : RELAIS_VERSION présent, pas de stack requis" \
    validate_config_env "$tmp_cfg" "edn" "relais"
rm -f "$tmp_cfg"

# Cohérence dépôt file:// (#657, terrain Enargia /flux/enedis) : le conteneur ne voit
# que ce qu'on lui monte — file:// sans FLUX_DEPOSIT_DIR identique = relais aveugle.
tmp_cfg=$(mktemp)
printf 'INSTANCE_SLUG=edn\nRELAIS_VERSION=1.2.0\nRELAIS__SOURCE_URL=file:///flux/enedis\n' > "$tmp_cfg"
assert_fail "validate_config_env (relais) : file:// sans FLUX_DEPOSIT_DIR → refuse" \
    validate_config_env "$tmp_cfg" "edn" "relais"
printf 'FLUX_DEPOSIT_DIR=/autre/chemin\n' >> "$tmp_cfg"
assert_fail "validate_config_env (relais) : FLUX_DEPOSIT_DIR incohérent avec l'URL → refuse" \
    validate_config_env "$tmp_cfg" "edn" "relais"
rm -f "$tmp_cfg"
tmp_cfg=$(mktemp)
printf 'INSTANCE_SLUG=edn\nRELAIS_VERSION=1.2.0\nRELAIS__SOURCE_URL=sftp://relais@source.example/flux\n' > "$tmp_cfg"
assert_ok "validate_config_env (relais) : source sftp:// sans FLUX_DEPOSIT_DIR → OK" \
    validate_config_env "$tmp_cfg" "edn" "relais"
rm -f "$tmp_cfg"

tmp_cfg=$(mktemp); printf 'INSTANCE_SLUG=edn\n' > "$tmp_cfg"
assert_fail "validate_config_env (component=relais) exige RELAIS_VERSION" \
    validate_config_env "$tmp_cfg" "edn" "relais"
rm -f "$tmp_cfg"

# Alerte mail (#674, arbitrage revue #675) : RELAIS_ALERTE_MAILS posé = alerte voulue
# ⇒ ALERTE__SMTP__{HOST,FROM,USER} requis (PORT a un défaut, 587). msmtp refuse un
# fichier à directive vide : sans ce fail-fast AVANT la pose, le premier reconfigure
# écraserait un msmtprc qui marche par un fichier invalide — alerte morte en silence.
tmp_cfg=$(mktemp)
printf 'INSTANCE_SLUG=edn\nRELAIS_VERSION=1.2.0\nRELAIS__SOURCE_URL=sftp://relais@source.example/flux\nRELAIS_ALERTE_MAILS=ops@example.fr\n' > "$tmp_cfg"
assert_fail "validate_config_env (relais) : RELAIS_ALERTE_MAILS posé sans ALERTE__SMTP__* → refuse (msmtprc serait invalide)" \
    validate_config_env "$tmp_cfg" "edn" "relais"
printf 'ALERTE__SMTP__HOST=smtp.example.fr\n' >> "$tmp_cfg"
assert_fail "validate_config_env (relais) : HOST seul ne suffit pas (FROM/USER requis aussi)" \
    validate_config_env "$tmp_cfg" "edn" "relais"
printf 'ALERTE__SMTP__FROM=alertes@example.fr\nALERTE__SMTP__USER=alertes@example.fr\n' >> "$tmp_cfg"
assert_ok "validate_config_env (relais) : alerte complète (HOST/FROM/USER, PORT en défaut) → OK" \
    validate_config_env "$tmp_cfg" "edn" "relais"
rm -f "$tmp_cfg"

# Sans RELAIS_ALERTE_MAILS l'alerte est désarmée (le hook sort en 0 avant msmtp) :
# aucune exigence SMTP — une box relais sans alerte mail reste une config valide.
tmp_cfg=$(mktemp)
printf 'INSTANCE_SLUG=edn\nRELAIS_VERSION=1.2.0\nRELAIS__SOURCE_URL=sftp://relais@source.example/flux\n' > "$tmp_cfg"
assert_ok "validate_config_env (relais) : sans RELAIS_ALERTE_MAILS, aucune exigence ALERTE__SMTP__*" \
    validate_config_env "$tmp_cfg" "edn" "relais"
rm -f "$tmp_cfg"

# component="relais" : un config.env qui n'a QUE ELECTRICORE_VERSION/BACKUPS_PATH (pas de
# RELAIS_VERSION) échoue toujours côté relais — bump d'ELECTRICORE_VERSION seul ne
# suffit pas à satisfaire le composant relais (indépendance des deux tags, #657).
tmp_cfg=$(mktemp)
printf 'INSTANCE_SLUG=edn\nELECTRICORE_VERSION=1.7.0\nBACKUPS_PATH=/srv/edn/backups\n' > "$tmp_cfg"
assert_fail "validate_config_env (component=relais) : ELECTRICORE_VERSION seul ne suffit pas" \
    validate_config_env "$tmp_cfg" "edn" "relais"
# Le même fichier reste valide côté stack (défaut, compat intégrale)
assert_ok "validate_config_env (défaut stack) : le même config.env reste valide côté stack" \
    validate_config_env "$tmp_cfg" "edn"
rm -f "$tmp_cfg"

# Garde-fou anti-fuite (#693) : un credential EMBARQUÉ DANS L'URL de RELAIS__SOURCE_URL /
# RELAIS__PARTNER_URL (motif sftp://user:pass@host) est une fuite — la variable elle-même
# APPARTIENT à config.env (le mini-compose du relais la lit par --env-file), seul le mot de
# passe embarqué est refusé. Une URL sans credential (file://…, sftp://user@host/…) reste
# légitime — authentification par clé SSH, chemin nominal du relais.
tmp_cfg=$(mktemp)
printf 'INSTANCE_SLUG=edn\nRELAIS_VERSION=1.2.0\nRELAIS__SOURCE_URL=sftp://user:hunter2@source.example/flux\n' > "$tmp_cfg"
assert_fail "validate_config_env (relais) : mot de passe embarqué dans RELAIS__SOURCE_URL → refuse (#693)" \
    validate_config_env "$tmp_cfg" "edn" "relais"
rm -f "$tmp_cfg"

tmp_cfg=$(mktemp)
printf 'INSTANCE_SLUG=edn\nRELAIS_VERSION=1.2.0\nRELAIS__SOURCE_URL=sftp://relais@source.example/flux\nRELAIS__PARTNER_URL=sftp://user:hunter2@partenaire.example/in\n' > "$tmp_cfg"
assert_fail "validate_config_env (relais) : mot de passe embarqué dans RELAIS__PARTNER_URL → refuse (#693)" \
    validate_config_env "$tmp_cfg" "edn" "relais"
rm -f "$tmp_cfg"

tmp_cfg=$(mktemp)
printf 'INSTANCE_SLUG=edn\nRELAIS_VERSION=1.2.0\nRELAIS__SOURCE_URL=file:///srv/edn/flux\nFLUX_DEPOSIT_DIR=/srv/edn/flux\nRELAIS__PARTNER_URL=sftp://relais@partenaire.example/in\n' > "$tmp_cfg"
assert_ok "validate_config_env (relais) : URL sans credential (file://…, sftp://user@host/…) → OK (#693)" \
    validate_config_env "$tmp_cfg" "edn" "relais"
rm -f "$tmp_cfg"

# Le CONTENU de secrets.env (format clés AES/API, URL SFTP) n'est plus validé en bash :
# SSOT pydantic (tests/unit/test_runtime.py), vérifié par le conteneur étapes 11-12 (ADR-0049).

echo
echo "→ user.sh / chown_instance_home (l'exception age.key survit au chown -R, #672 bis)"
# Fake chown : journalise les appels — l'assertion porte sur l'ORDRE (le ré-assert
# age.key doit venir APRÈS le balayage -R, sinon il est écrasé — reconfigure Enargia
# 28/07). Testable sans root : aucun vrai chown n'est exécuté.
ch_root=$(mktemp -d)
ch_bin="${ch_root}/bin"; mkdir -p "$ch_bin" "${ch_root}/edn"
: > "${ch_root}/edn/age.key"
cat > "${ch_bin}/chown" <<EOF
#!/usr/bin/env bash
printf '%s\n' "\$*" >> "${ch_root}/chown.log"
EOF
chmod +x "${ch_bin}/chown"
PATH="${ch_bin}:$PATH" SRV_BASE="$ch_root" CONTAINER_UID=1000 CONTAINER_GID=1000 chown_instance_home edn
grep -q -- "-R edn:edn ${ch_root}/edn" "${ch_root}/chown.log" \
    && ok "chown_instance_home: balayage -R slug:slug du home" || ko "chown_instance_home: balayage -R absent"
tail -1 "${ch_root}/chown.log" | grep -q "1000:1000 ${ch_root}/edn/age.key" \
    && ok "chown_instance_home: age.key ré-asserté CONTAINER_UID APRÈS le balayage" \
    || ko "chown_instance_home: age.key pas ré-asserté après le -R (l'entrypoint SOPS re-cassera au reconfigure)"
rm -f "${ch_root}/chown.log" "${ch_root}/edn/age.key"
PATH="${ch_bin}:$PATH" SRV_BASE="$ch_root" chown_instance_home edn
[[ "$(wc -l < "${ch_root}/chown.log")" == "1" ]] \
    && ok "chown_instance_home: sans age.key → seul le balayage (pas de chown fantôme)" \
    || ko "chown_instance_home: appel chown inattendu en l'absence d'age.key"

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
# Résidu d'un run interrompu : un age.key VIDE (l'ancienne redirection le créait avant
# l'échec d'age-keygen — VPS Enargia) doit être remplacé, pas « conservé ».
install -d "${secrets_root}/residu"
: > "${secrets_root}/residu/age.key"
out=$(PATH="${FAKE_BIN}:$PATH" generate_box_identities residu 2>/dev/null)
[[ -s "${secrets_root}/residu/age.key" ]] && grep -q "^AGE_PUBLIC_KEY=age1" <<<"$out" \
    && ok "generate_age_identity: age.key vide (résidu) → régénéré, pub imprimée" \
    || ko "generate_age_identity: age.key vide conservé — runs empoisonnés à vie"

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

# Reconfigure (2e appel = pull, .git présent) : l'invocation DOIT porter
# -c safe.directory=<repo> — le home d'instance appartient au slug, le pull tourne
# root → « dubious ownership » au premier reconfigure sinon (box Enargia, 28/07).
git_log=$(mktemp)
PATH="${FAKE_BIN}:$PATH" FAKE_GIT_SRC="$git_src" FAKE_GIT_LOG="$git_log" GIT_BIN=git \
    pull_deploy_repo "git@example.test:org/deploy.git" edn >/dev/null 2>&1
grep -q -- "-c safe.directory=${secrets_root}/edn/deploy-repo -C" "$git_log" \
    && ok "pull_deploy_repo: reconfigure = pull avec -c safe.directory (root vs home du slug)" \
    || ko "pull_deploy_repo: -c safe.directory absent de l'invocation pull"
rm -f "$git_log"
rm -rf "$git_src"

# box_can_decrypt : vrai si clé + secrets présents ET sops réussit ; faux si sops échoue.
PATH="${FAKE_BIN}:$PATH" FAKE_SOPS_FAIL=0 box_can_decrypt edn \
    && ok "box_can_decrypt: vrai quand sops déchiffre (clé destinataire)" || ko "box_can_decrypt devait réussir"
PATH="${FAKE_BIN}:$PATH" FAKE_SOPS_FAIL=1 box_can_decrypt edn \
    && ko "box_can_decrypt: devait échouer si sops échoue (clé non destinataire)" \
    || ok "box_can_decrypt: faux quand sops échoue (deux temps — pas encore destinataire)"

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

# ── Fake docker modèle le fail-fast de l'entrypoint (#453) ───────────────────
# Un `docker run <image> <cmd>` sans bypass (ELECTRICORE_DECRYPT=off) NI secrets
# montés doit fail-fast. Cela verrouille la règle : la crypto ne passe JAMAIS par
# l'image (son entrypoint SOPS bloquerait sans secrets → bug #1 de la bascule EDN).
echo
echo "→ fake docker / entrypoint fail-fast (#453)"
out=$(PATH="${FAKE_BIN}:$PATH" docker run --rm electricore:test age-keygen 2>&1); rc=$?
[[ "$rc" -ne 0 ]] \
    && ok "fake docker: age-keygen sans bypass → fail-fast (crypto ne passe jamais par l'image)" \
    || ko "fake docker: age-keygen sans bypass devait fail-fast"
grep -qi "fail-fast\|entrypoint" <<<"$out" \
    && ok "fake docker: fail-fast mentionne l'entrypoint" \
    || ko "fake docker: fail-fast manque le motif entrypoint"

# Avec bypass ELECTRICORE_DECRYPT=off → l'appel doit RÉUSSIR (bypass explicite documenté).
# Ici age-keygen n'est plus une commande simulée mais la règle est :
# le fail-fast est levé par le bypass, puis la commande non-simulée retourne exit 1 —
# ce qui est distinct du fail-fast d'entrypoint (l'entrypoint n'a pas bloqué).
# On teste le cas le plus simple : python (toujours simulé) avec bypass → succès.
out=$(PATH="${FAKE_BIN}:$PATH" docker run --rm -e ELECTRICORE_DECRYPT=off electricore:test python -c "pass" 2>&1); rc=$?
[[ "$rc" -eq 0 ]] \
    && ok "fake docker: python avec ELECTRICORE_DECRYPT=off → bypass (entrypoint non bloquant)" \
    || ko "fake docker: python avec bypass devait réussir (got rc=$rc)"

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
