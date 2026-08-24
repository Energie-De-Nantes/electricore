#!/usr/bin/env bash
# Auto-test de packages/electricore-kiosque/smoke.sh (fake docker/curl, #706).
# Même motif que deploy/tests/unit.sh (fake binaires, pas de vraie image) — sibling
# minimal plutôt que le harnais du moteur (fake docker moteur modélise l'entrypoint
# SOPS, hors-sujet ici : le Kiosque ne déchiffre rien).
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FAKE_BIN="${SCRIPT_DIR}/fixtures/fake_bin"

# shellcheck source=../smoke.sh
source "${SCRIPT_DIR}/../smoke.sh"

PASS=0
FAIL=0
ok() {
    printf '  \033[32m✓\033[0m %s\n' "$1"
    PASS=$((PASS + 1))
}
ko() {
    printf '  \033[31m✗\033[0m %s\n' "$1"
    FAIL=$((FAIL + 1))
}

echo "→ smoke.sh (kiosque, fake docker + curl)"

PATH="${FAKE_BIN}:$PATH" DOCKER_BIN=docker CURL_BIN=curl \
    smoke_kiosque_image "electricore-kiosque:test" >/dev/null 2>&1 \
    && ok "smoke_kiosque_image: image saine (sert + fail-fast) → succès" \
    || ko "smoke_kiosque_image: image saine aurait dû réussir"

out=$(PATH="${FAKE_BIN}:$PATH" DOCKER_BIN=docker CURL_BIN=curl FAKE_CURL_FAIL=1 \
    smoke_kiosque_image "electricore-kiosque:test" 2>&1)
rc=$?
[[ "$rc" -ne 0 ]] && ok "smoke_kiosque_image: service muet (HTTP KO) → exit non-zero" \
    || ko "smoke_kiosque_image: service muet aurait dû échouer"
grep -qi "ne sert pas" <<<"$out" && ok "smoke_kiosque_image: message nomme la fumée fautive (sert)" \
    || ko "smoke_kiosque_image: message de la fumée 'sert' absent"

out=$(PATH="${FAKE_BIN}:$PATH" DOCKER_BIN=docker CURL_BIN=curl FAKE_DOCKER_NO_FAILFAST=1 \
    smoke_kiosque_image "electricore-kiosque:test" 2>&1)
rc=$?
[[ "$rc" -ne 0 ]] && ok "smoke_kiosque_image: fail-fast absent de l'image → détecté, exit non-zero" \
    || ko "smoke_kiosque_image: absence de fail-fast aurait dû être détectée"
grep -qi "fail-fast" <<<"$out" && ok "smoke_kiosque_image: message nomme la fumée fautive (fail-fast)" \
    || ko "smoke_kiosque_image: message de la fumée 'fail-fast' absent"

(smoke_kiosque_image >/dev/null 2>&1)
rc=$?
[[ "$rc" -eq 2 ]] && ok "smoke_kiosque_image: sans tag → exit 2 (usage)" \
    || ko "smoke_kiosque_image: sans tag devait être exit 2 (got $rc)"

echo
echo "${PASS} ok, ${FAIL} ko"
[[ "$FAIL" -eq 0 ]]
