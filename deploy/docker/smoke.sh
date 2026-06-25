#!/usr/bin/env bash
# shellcheck shell=bash
# smoke.sh — fume une image electricore DÉJÀ buildée (issue #435).
#
# Deux vérifications, contre l'IMAGE (pas le seul script entrypoint) :
#   1. Importabilité : l'image démarre et les modules de tête s'importent. On contourne
#      l'entrypoint SOPS (ELECTRICORE_DECRYPT=off) — sinon il fail-fast faute de secrets
#      montés (#434, ADR-0044 §3) et le smoke échouerait pour une mauvaise raison.
#   2. Déchiffrement : monté avec les fixtures SOPS de test, l'entrypoint déchiffre et
#      injecte un label AES DYNAMIQUE (AES__TROUSSEAU__demo__KEY, ADR-0037) — le scénario
#      de tests/integration/test_entrypoint_dechiffrement.py, mais contre l'image.
#
# Usage :   deploy/docker/smoke.sh <image-tag>
#
# Surcharges de test (motif maison, cf. deploy/lib/secrets.sh) :
#   DOCKER_BIN           binaire docker (défaut: docker ; les tests le stubbent)
#   SMOKE_FIXTURES_DIR   dossier des fixtures SOPS (défaut: tests/fixtures/sops du repo)

# Commandes externes + emplacements, surchargeables pour les tests (pas de set -e ici :
# ce fichier est SOURCÉ par deploy/tests/unit.sh ; errexit est posé dans le guard `main`).
DOCKER_BIN="${DOCKER_BIN:-docker}"
_SMOKE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Racine du repo résolue en absolu (sans `..` : `docker -v` monte mal les chemins relatifs).
_REPO_ROOT="$(cd "${_SMOKE_DIR}/../.." && pwd)"
SMOKE_FIXTURES_DIR="${SMOKE_FIXTURES_DIR:-${_REPO_ROOT}/tests/fixtures/sops}"

# Modules de tête dont l'échec d'import = image cassée (même liste que release.yml).
_SMOKE_IMPORTS='import electricore; import electricore.ingestion.runner; import electricore.api.main; import dbt.cli.main'

# smoke_importabilite <tag> : l'image s'importe (entrypoint SOPS contourné).
smoke_importabilite() {
    local tag="$1"
    "$DOCKER_BIN" run --rm -e ELECTRICORE_DECRYPT=off "$tag" \
        python -c "$_SMOKE_IMPORTS"
}

# smoke_dechiffrement <tag> : monté avec les fixtures SOPS, l'entrypoint injecte le
# label AES dynamique. On n'inspecte QUE le code de sortie : la commande dans le
# conteneur (`test -n`) échoue si le label n'a pas été injecté → l'entrypoint n'a pas
# déchiffré. C'est le scénario de test_entrypoint_dechiffrement.py, contre l'image.
smoke_dechiffrement() {
    local tag="$1"
    "$DOCKER_BIN" run --rm \
        -v "${SMOKE_FIXTURES_DIR}/secrets.env:/run/secrets/secrets.env:ro" \
        -v "${SMOKE_FIXTURES_DIR}/test_age.key:/run/secrets/age.key:ro" \
        "$tag" \
        sh -c 'test -n "${AES__TROUSSEAU__demo__KEY:-}"'
}

# smoke_image <tag> : lance les deux fumées, rapporte chaque verdict, échoue si l'une casse.
smoke_image() {
    local tag="${1:-}"
    if [[ -z "$tag" ]]; then
        echo "usage: smoke.sh <image-tag>" >&2
        return 2
    fi
    local rc=0
    if smoke_importabilite "$tag"; then
        echo "smoke: importabilité OK"
    else
        echo "smoke: ÉCHEC importabilité (l'image ne démarre/importe pas)" >&2
        rc=1
    fi
    if smoke_dechiffrement "$tag"; then
        echo "smoke: déchiffrement OK (label AES dynamique injecté)"
    else
        echo "smoke: ÉCHEC déchiffrement (entrypoint SOPS n'a pas injecté le label)" >&2
        rc=1
    fi
    return "$rc"
}

main_smoke() {
    set -euo pipefail
    smoke_image "$@"
}

# Guard : exécute main_smoke seulement si lancé (`bash smoke.sh …`), pas sourcé (tests).
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main_smoke "$@"
fi
