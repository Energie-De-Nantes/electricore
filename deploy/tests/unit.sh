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

PASS=0; FAIL=0
ok() { printf '  \033[32m✓\033[0m %s\n' "$1"; PASS=$((PASS+1)); }
ko() { printf '  \033[31m✗\033[0m %s\n' "$1"; FAIL=$((FAIL+1)); }

assert_ok()   { local desc="$1"; shift; if "$@" 2>/dev/null; then ok "$desc"; else ko "$desc (exit non-zero)"; fi; }
assert_fail() { local desc="$1"; shift; if "$@" 2>/dev/null; then ko "$desc (devait échouer)"; else ok "$desc"; fi; }
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
if [[ "$FAIL" -eq 0 ]]; then
    printf "\033[32m%d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
    exit 0
else
    printf "\033[31m%d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
    exit 1
fi
