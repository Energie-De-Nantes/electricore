#!/usr/bin/env bash
# shellcheck shell=bash
# smoke.sh — fume une image electricore-kiosque DÉJÀ buildée (#706).
#
# Deux vérifications, contre l'IMAGE (pas seulement le code Python — déjà couvert par
# packages/electricore-kiosque/tests/test_main.py) :
#   1. Le service sert : démarré avec KIOSQUE__APPS/KIOSQUE__TITRE/KIOSQUE__API_URL,
#      GET / répond 200 (l'accueil). Aucun secret requis — l'API n'est jamais appelée
#      au démarrage (KIOSQUE__API_URL est lue paresseusement, à l'usage d'un notebook).
#   2. Fail-fast : KIOSQUE__APPS référence une app hors catalogue → le conteneur
#      s'arrête en erreur (exit non-zero) avec le message explicite (app.py NomAppInconnu).
#
# Usage :   packages/electricore-kiosque/smoke.sh <image-tag>
#
# Surcharges de test (motif deploy/docker/smoke.sh, deploy/tests/unit.sh) :
#   DOCKER_BIN  binaire docker (défaut: docker ; les tests le stubbent)
#   CURL_BIN    binaire curl (défaut: curl ; les tests le stubbent)

DOCKER_BIN="${DOCKER_BIN:-docker}"
CURL_BIN="${CURL_BIN:-curl}"

# smoke_kiosque_sert <tag> : le conteneur démarre et sert l'accueil (HTTP 200 sur /).
smoke_kiosque_sert() {
    local tag="$1"
    local port="${SMOKE_KIOSQUE_PORT:-18765}"
    local cid
    cid=$("$DOCKER_BIN" run -d --rm \
        -e KIOSQUE__APPS=exports \
        -e KIOSQUE__TITRE=Smoke \
        -e KIOSQUE__API_URL=http://smoke.invalid \
        -p "${port}:8765" \
        "$tag") || return 1

    local rc=1 tries=0
    while [[ $tries -lt 20 ]]; do
        if "$CURL_BIN" -sf -o /dev/null "http://127.0.0.1:${port}/"; then
            rc=0
            break
        fi
        sleep 0.5
        tries=$((tries + 1))
    done

    "$DOCKER_BIN" stop "$cid" >/dev/null 2>&1
    return "$rc"
}

# smoke_kiosque_fail_fast <tag> : nom hors catalogue → conteneur en erreur, message explicite.
smoke_kiosque_fail_fast() {
    local tag="$1"
    local out
    out=$("$DOCKER_BIN" run --rm \
        -e KIOSQUE__APPS=typo_inexistante \
        -e KIOSQUE__TITRE=Smoke \
        -e KIOSQUE__API_URL=http://smoke.invalid \
        "$tag" 2>&1)
    local rc=$?
    [[ "$rc" -ne 0 && "$out" == *"typo_inexistante"* ]]
}

# smoke_kiosque_image <tag> : lance les deux fumées, rapporte chaque verdict, échoue si l'une casse.
smoke_kiosque_image() {
    local tag="${1:-}"
    if [[ -z "$tag" ]]; then
        echo "usage: smoke.sh <image-tag>" >&2
        return 2
    fi
    local rc=0
    if smoke_kiosque_sert "$tag"; then
        echo "smoke: le service sert (HTTP 200 sur /) OK"
    else
        echo "smoke: ÉCHEC — le service ne sert pas (voir docker logs)" >&2
        rc=1
    fi
    if smoke_kiosque_fail_fast "$tag"; then
        echo "smoke: fail-fast OK (nom hors catalogue → conteneur en erreur, message explicite)"
    else
        echo "smoke: ÉCHEC — le fail-fast n'a pas produit l'erreur/le message attendus" >&2
        rc=1
    fi
    return "$rc"
}

main_smoke() {
    set -euo pipefail
    smoke_kiosque_image "$@"
}

# Guard : exécute main_smoke seulement si lancé (`bash smoke.sh …`), pas sourcé (tests).
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main_smoke "$@"
fi
